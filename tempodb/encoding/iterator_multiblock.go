package encoding

import (
	"bytes"
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"

	"github.com/grafana/tempo/tempodb/encoding/common"

	"github.com/uber-go/atomic"
)

var (
	metricMultiBlockIteratorResultsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compaction_mbi_results_sent",
		Help:      "Total number of results returned from the multiblock iterator.",
	}, []string{})
	metricMultiBlockIteratorWorkDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compaction_mbi_results_dropped",
		Help:      "The amount of spans that were processed but dropped",
	}, []string{})
)

type multiblockIterator struct {
	combiner     common.ObjectCombiner
	bookmarks    []*bookmark
	dataEncoding string
	resultsCh    chan iteratorResult
	quitCh       chan struct{}
	err          atomic.Error
}

var _ Iterator = (*multiblockIterator)(nil)

type iteratorResult struct {
	id     common.ID
	object []byte
}

// NewMultiblockIterator Creates a new multiblock iterator. Iterates concurrently in a separate goroutine and results are buffered.
// Traces are deduped and combined using the object combiner.
func NewMultiblockIterator(ctx context.Context, inputs []Iterator, bufferSize int, combiner common.ObjectCombiner, dataEncoding string) Iterator {
	i := multiblockIterator{
		combiner:     combiner,
		dataEncoding: dataEncoding,
		resultsCh:    make(chan iteratorResult, bufferSize),
		quitCh:       make(chan struct{}, 1),
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

	for !i.allDone(ctx) {
		var lowestID []byte
		var lowestObject []byte
		var lowestBookmark *bookmark

		// find lowest ID of the new object
		var count int
		for _, b := range i.bookmarks {
			currentID, currentObject, err := b.current(ctx)
			if err == io.EOF {
				continue
			} else if err != nil {
				i.err.Store(err)
				return
			}

			comparison := bytes.Compare(currentID, lowestID)

			if comparison == 0 {
				lowestObject, _ = i.combiner.Combine(i.dataEncoding, currentObject, lowestObject)
				b.clear()
				count++
			} else if len(lowestID) == 0 || comparison == -1 {
				metricMultiBlockIteratorWorkDropped.With(nil).Add(float64(count))
				count = 0
				lowestID = currentID
				lowestObject = currentObject
				lowestBookmark = b
			}
		}

		if len(lowestID) == 0 || len(lowestObject) == 0 || lowestBookmark == nil {
			i.err.Store(errors.New("failed to find a lowest object in compaction"))
			return
		}

		// Copy slices allows data to escape the iterators
		res := iteratorResult{
			id:     append([]byte(nil), lowestID...),
			object: append([]byte(nil), lowestObject...),
		}

		lowestBookmark.clear()

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
