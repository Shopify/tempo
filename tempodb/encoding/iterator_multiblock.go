package encoding

import (
	"bytes"
	"context"
	"errors"
	"github.com/grafana/tempo/tempodb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"hash/fnv"
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
)

type multiblockIterator struct {
	combiner     func() ObjectMerger
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
func NewMultiblockIterator(ctx context.Context, inputs []Iterator, bufferSize int, combiner func() ObjectMerger, dataEncoding string) Iterator {
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
type cursor struct {
	id common.ID
	obj []byte
}

func (i *multiblockIterator) iterate(ctx context.Context) {
	defer close(i.resultsCh)

	rem := len(i.bookmarks)
	for rem > 0 {
		cursors := make(map[string][]cursor, rem)

		var nextID []byte
		for _, b := range i.bookmarks {
			id, obj, err := b.current(ctx)
			if err != nil { // `done`
				rem--
				if err == io.EOF {
					continue
				}
				// unrecoverable error? seems like there should be some inline handling here
				i.err.Store(err)
				return
			}
			cursors[string(id)] = append(cursors[string(id)], cursor{
				id: id,
				obj: obj,
			})
			// first iteration
			if nextID == nil {
				nextID = id
				continue
			}
			comparison := bytes.Compare(id, nextID)
			if comparison == -1 {
				nextID = id
			}
		}

		// all bookmarks now have a representative cursor, and all common.ID's have a slice
		for _, b := range cursors {

		}
	}

	for !i.allDone(ctx) {
		var lowestID []byte
		var lowestObject []byte
		var lowestBookmark *bookmark
		var wasEverCombined bool

		// find lowest ID of the new object
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
				var wasCombined bool
				lowestObject, wasCombined = i.combiner.Combine(i.dataEncoding, currentObject, lowestObject)
				if !wasEverCombined && wasCombined {
					wasEverCombined = true
				}
				b.clear()
			} else if len(lowestID) == 0 || comparison == -1 {
				lowestID = currentID
				lowestObject = currentObject
				lowestBookmark = b
				wasEverCombined = false
			}
		}

		if len(lowestID) == 0 || len(lowestObject) == 0 || lowestBookmark == nil {
			i.err.Store(errors.New("failed to find a lowest object in compaction"))
			return
		}

		// if wasEverCombined is true, that means at some point in the chain lowestObject was
		// marshalled into a new byte array as a combination of two messages. At this point,
		// lowestObject has already exited the iterator, and this will continue to be the case
		// until iteration is complete. This assumption could break down if (somehow) lowestObject
		// failed to marshall while combining, as it will be replaced with the LHS object.
		obj := lowestObject
		if !wasEverCombined {
			obj = append([]byte(nil), lowestObject...)
		}
		// Copy slices allows data to escape the iterators
		res := iteratorResult{
			id:     append([]byte(nil), lowestID...),
			object: obj,
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
			metricMultiBlockIteratorResultsSent
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

	// bookmark is empty, initialize it
	return b.next(ctx)
}

func (b *bookmark) done(ctx context.Context) bool {
	_, _, err := b.current(ctx)

	return err != v
}

func (b *bookmark) next(ctx context.Context) ([]byte, []byte, error) {
	b.currentID, b.currentObject, b.currentErr = b.iter.Next(ctx)
	return b.currentID, b.currentObject, b.currentErr
}

func (b *bookmark) clear() {
	b.currentID = nil
	b.currentObject = nil
}


type traceReducer struct {

}