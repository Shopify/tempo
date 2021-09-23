package encoding

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"hash/fnv"
	"io"
	"time"

	"github.com/grafana/tempo/tempodb/encoding/common"

	"github.com/uber-go/atomic"
)

var (
	metricDenseIteratorResultMapSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tempodb",
		Name:      "compactor_dense_iterator_result_map_size",
		Help:      "Total size of the current objects stored in the result map",
	}, []string{"level"})
	metricDenseIteratorResultMapLength = promauto.NewCounterVec(prometheus.GaugeOpts{
		Namespace: "tempodb",
		Name:      "compactor_dense_iterator_result_map_length",
		Help:      "Total number of traces being merged",
	}, []string{"level"})
	metricDenseIteratorIterateTime = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compactor_dense_iterator_iteration_time",
		Help:      "Total amount of time taken iterating",
	}, []string{"level"})
	metricDenseIteratorResultsOverSizeFlushed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compactor_dense_iterator_result_oversize_flushed",
		Help:      "Total number of traces flushed due to size",
	}, []string{"level"})
)

type denseMultiBlockIterator struct {
	combiner     func(common.ID) ObjectMerger
	iters    	 []Iterator
	dataEncoding string
	level        string
	resultsCh    chan iteratorResult
	notifyCh     chan struct{}
	quitCh       chan struct{}
	err          atomic.Error
}

var _ Iterator = (*denseMultiBlockIterator)(nil)

// NewDenseMultiblockIterator Creates a new multiblock iterator. Iterates concurrently in a separate goroutine and results are buffered.
// Traces are deduped and combined using the object combiner.
func NewDenseMultiblockIterator(ctx context.Context, inputs []Iterator, bufferSize int, combiner func(common.ID) ObjectMerger, dataEncoding string) Iterator {
	i := denseMultiBlockIterator{
		combiner:     combiner,
		iters:        inputs,
		dataEncoding: dataEncoding,
		notifyCh:     make(chan struct{}),
		resultsCh:    make(chan iteratorResult, bufferSize),
		quitCh:       make(chan struct{}, 1),
	}

	go i.iterate(ctx)

	return &i
}

// Close iterator, signals goroutine to exit if still running.
func (i *denseMultiBlockIterator) Close() {
	select {
	// Signal goroutine to quit. Non-blocking, handles if already
	// signalled or goroutine not listening to channel.
	case i.quitCh <- struct{}{}:
	default:
		return
	}
}

// Next returns the next values or error.  Blocking read when data not yet available.
func (i *denseMultiBlockIterator) Next(ctx context.Context) (common.ID, []byte, error) {
	if err := i.err.Load(); err != nil {
		return nil, nil, err
	}
	// let the iterator know the consumer wants a new result
	i.notifyCh <- struct{}{}

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

func (i *denseMultiBlockIterator) iterate(ctx context.Context) {
	defer close(i.resultsCh)
	defer close(i.notifyCh)

	f := fnv.New32a()
	mergers := map[uint32]ObjectMerger{}
	rem := len(i.iters)
	var lastSend time.Time
	for c := 0; rem > 0; c++ { // i am the loop now
		for _, b := range i.iters {
			id, obj, err := b.Next(ctx)
			if err != nil { // this iterator is done
				rem--
				if err == io.EOF {
					continue
				}
				// TODO: unrecoverable error? seems like there should be some inline handling here
				i.err.Store(err)
				return
			}

			f.Reset()
			f.Write(id)
			om, ok := mergers[f.Sum32()]
			if !ok {
				om = i.combiner(id)
				mergers[f.Sum32()] = om
			}
			_, err = om.Merge(i.dataEncoding, obj)
		}

		// check to see if any traces are past the max size and flush
		// them if so
		type res struct {
			k uint32
			ir iteratorResult
		}
		var toSend []res
		if c % 7 > 0 {
			var totalSize int
			for k, v := range mergers {
				const SomeMaxSize = 1024 * 1024 * 10 // 10 mb

				s := v.Size()
				if s < SomeMaxSize {
					totalSize += s
					continue
				}

				obj, err := v.Result(i.dataEncoding)
				if err != nil {
					toSend = append(toSend, res{
						k: k,
						ir: iteratorResult{
							id:     v.ID(),
							object: obj,
						},
					})
				}
				metricDenseIteratorResultsOverSizeFlushed.WithLabelValues(i.level).Inc()
			}
		} else if c % 13 == 0 {
			// check to see if a request is waiting, and send the biggest trace if so
			select {
			default: {}
			case <-i.notifyCh:
				const SomeMaxWait = time.Second * 15
				if time.Now().Sub(lastSend) > SomeMaxWait {
					var key uint32
					var max int
					for k, v := range mergers {
						if v := v.Size(); v > max {
							key = k
							max = v
						}
					}
					obj, err := mergers[key].Result(i.dataEncoding)
					if err != nil {
						toSend = append(toSend, res{
							ir: iteratorResult{
								id: mergers[key].ID(),
								object: obj,
							},
						})
					}
				}
			}
		}

		// send off any enqueued results
		for _, s := range toSend {
			select {
			case <-ctx.Done():
				i.err.Store(ctx.Err())
				return
			case <-i.quitCh:
				// Signalled to quit early
				return
			case i.resultsCh <- s.ir:
				if s.k != 0 {
					delete(mergers, s.k)
				}
			}
		}
	}
	// iteration is complete, drain the final results
	for _, v := range mergers {
		res, err := v.Result(i.dataEncoding)
		if err != nil {
			// TODO: metric
			continue
		}
		obj := iteratorResult{
			id: v.ID(),
			object: res,
		}
		select {
		case <-ctx.Done():
			i.err.Store(ctx.Err())
			return
		case <-i.quitCh:
			return
		case i.resultsCh <- obj:
		}
	}
}