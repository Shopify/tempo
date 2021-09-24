package encoding

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/kit/log/level"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/uber-go/atomic"

	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

var (
	metricMultiBlockIteratorWorkDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compaction_mbi_results_dropped",
		Help:      "The amount of spans that were processed but dropped",
	}, []string{"level"})
)

type multiblockIterator struct {
	mergerFactory model.MergerFactory
	bookmarks     []*bookmark
	dataEncoding  string
	level         string
	resultsCh     chan iteratorResult
	quitCh        chan struct{}
	err           atomic.Error
}

var _ Iterator = (*multiblockIterator)(nil)

type iteratorResult struct {
	id     common.ID
	object []byte
}

// NewMultiblockIterator Creates a new multiblock iterator. Iterates concurrently in a separate goroutine and results are buffered.
// Traces are deduped and combined using the object combiner.
func NewMultiblockIterator(ctx context.Context, inputs []Iterator, bufferSize int, dataEncoding, level string, mergerFactory model.MergerFactory) Iterator {
	i := multiblockIterator{
		mergerFactory: mergerFactory,
		dataEncoding:  dataEncoding,
		level:         level,
		resultsCh:     make(chan iteratorResult, bufferSize),
		quitCh:        make(chan struct{}, 1),
	}

	for _, iter := range inputs {
		i.bookmarks = append(i.bookmarks, newBookmark(iter))
	}

	go i.iterate(ctx)

	return &i
}

// Close iterator, signals goroutine to exit if still running.
func (i *multiblockIterator) Close() {
	select {
	// Signal goroutine to quit. Non-blocking, handles if already
	// signalled or goroutine not listening to channel.
	case i.quitCh <- struct{}{}:
	default:
		return
	}
}

// Next returns the next values or error.  Blocking read when data not yet available.
func (i *multiblockIterator) Next(ctx context.Context) (common.ID, []byte, error) {
	if err := i.err.Load(); err != nil {
		return nil, nil, err
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case res, ok := <-i.resultsCh:
		if !ok {
			// Closed due to error?
			if err := i.err.Load(); err != nil {
				return nil, nil, err
			}
			return nil, nil, io.EOF
		}

		return res.id, res.object, nil
	}
}

// allDone iterates the current bookmark list and checks if any of them still contain data.
func (i *multiblockIterator) allDone(ctx context.Context) bool {
	for _, b := range i.bookmarks {
		if !b.done(ctx) {
			return false
		}
	}
	return true
}

func (i *multiblockIterator) iterate(ctx context.Context) {
	defer close(i.resultsCh)

	// Until all the iterators have either errored, or reached EOF.
	for !i.allDone(ctx) {
		var lowestID []byte
		var currentMerger model.ObjectMerger
		var bookmarkToClear *bookmark
		var workCount int

		// Get the current state of each bookmark, and find the lowest ID
		// that is currently in the list. In each block, IDs are unique,
		// and are sorted lexicographically ascending. Whenever a lower ID
		// is found, start merging objects with the same ID from the rest
		// of the blocks.
		for _, b := range i.bookmarks {
			currentID, currentObject, err := b.current(ctx)
			if err == io.EOF {
				continue
			} else if err != nil {
				i.err.Store(err)
				return
			}

			comparison := bytes.Compare(currentID, lowestID)
			foundLowerID := comparison == -1
			shouldMerge := comparison == 0
			if currentMerger == nil || foundLowerID {
				metricMultiBlockIteratorWorkDropped.WithLabelValues(i.level).Add(float64(workCount))
				workCount = 0
				currentMerger = i.mergerFactory(i.dataEncoding, currentID, currentObject)

				// Track the ID for comparisons in further iterations of this loop.
				lowestID = currentID

				// The current bookmark is stored to be cleared at the end of this loop. If it were cleared
				// prior to that, there could be data loss if a lower ID is found in one of the next iterations.
				bookmarkToClear = b
			} else if shouldMerge {
				merged, err := currentMerger.Merge(i.dataEncoding, currentObject)
				if err != nil {
					level.Error(log.Logger).Log("msg", "error occurred during merging of objects, ignoring failures", "err", err.Error())
				}
				workCount += merged
				b.clear()
			}
		}

		// This means that all blocks being iterated were at EOF. Any other error would
		// have resulted in an early return.
		if currentMerger == nil {
			return
		}

		// Get the final merged result.
		mergedObject, err := currentMerger.Result(i.dataEncoding)
		if err != nil {
			i.err.Store(fmt.Errorf("failed to serialize merged result object: %w", err))
			return
		}

		// The array backing the currentID slice is still owned by the underlying iterator
		// buffer, so it must be copied out, otherwise when the result is read, the buffer
		// will most likely be filled with new data. The mergedObject however is already escaped
		// by the result call, and by virtue of being deserialized and then serialized back to
		// bytes, so it can be used directly.
		res := iteratorResult{
			id:     append([]byte(nil), currentMerger.ID()...),
			object: mergedObject,
		}

		// Now that this was definitely the object being worked on, the first bookmark for the
		// current object can be cleared. The nil check is not technically required, since
		// if it is nil, currentMerger must also be nil and this code path will not be hit,
		// but do so anyways for sanity and safety.
		if bookmarkToClear != nil {
			bookmarkToClear.clear()
		}

		select {
		case <-ctx.Done():
			i.err.Store(ctx.Err())
			return

		case <-i.quitCh:
			// Signalled to quit early
			return

		case i.resultsCh <- res:
			// Send results. Blocks until available buffer in channel
			// created by receiving in Next()
		}
	}
}

type bookmark struct {
	iter Iterator

	currentID     []byte
	currentObject []byte
	currentErr    error
}

func newBookmark(iter Iterator) *bookmark {
	return &bookmark{
		iter: iter,
	}
}

func (b *bookmark) current(ctx context.Context) ([]byte, []byte, error) {
	if len(b.currentID) != 0 && len(b.currentObject) != 0 {
		return b.currentID, b.currentObject, nil
	}

	if b.currentErr != nil {
		return nil, nil, b.currentErr
	}

	b.currentID, b.currentObject, b.currentErr = b.iter.Next(ctx)
	return b.currentID, b.currentObject, b.currentErr
}

func (b *bookmark) done(ctx context.Context) bool {
	_, _, err := b.current(ctx)

	return err != nil
}

func (b *bookmark) clear() {
	b.currentID = nil
	b.currentObject = nil
}
