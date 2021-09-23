package encoding

import (
	"encoding/binary"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"hash"
	"hash/fnv"

	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
)

var (
	metricDenseIteratorMergedObjects = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempodb",
		Name:      "compactor_dense_iterator_merged_objects",
		Help:      "Total amount of objects merged",
	}, []string{"level"})
)

type ObjectMerger interface {
	ID() common.ID
	Merge(dataEncoding string, objs ...[]byte) (bool, error)
	Size() int
	Result(dataEncoding string) ([]byte, error)
}

type objectMerger struct {
	lazyCurrent []byte
	current *tempopb.Trace
	id      common.ID
	level   string
	spans   map[uint32]struct{}
}

// extractSpans creates a set which contains all spans that already exist in the merged structure
func extractSpans(t *tempopb.Trace) map[uint32]struct{} {
	m := make(map[uint32]struct{})
	h := fnv.New32()
	buffer := make([]byte, 4)
	for _, batch := range t.Batches {
		for _, ils := range batch.InstrumentationLibrarySpans {
			for _, span := range ils.Spans {
				m[tokenForID(h, buffer, int32(span.Kind), span.SpanId)] = struct{}{}
			}
		}
	}
	return m
}

func (m objectMerger) ID() common.ID {
	return m.id
}

func NewMerger(level string, id common.ID) *objectMerger {
	return &objectMerger{
		level: level,
		id:    id,
	}
}

func (m *objectMerger) Merge(dataEncoding string, objs ...[]byte) (bool, error) {
	if len(objs) == 0 {
		return false, nil
	}
	var err error
start:
	for m.current == nil && len(objs) > 0 {
		m.lazyCurrent = objs[0]
		break
	}
	objs = objs[1:]
	if m.current == nil || err != nil {
		return false, err
	}

	if len(m.lazyCurrent) > 0 && len(objs) > 0 {
		m.current, err = model.Unmarshal(objs[0], dataEncoding)
		if err != nil {
			// just retry at this point
			m.lazyCurrent = nil
			m.current = nil
			goto start
		}
		m.spans = extractSpans(m.current)
	}

	h := fnv.New32()
	buffer := make([]byte, 4)
	// TODO?: the number of merge calls could be reduced by splitting
	// the list into pairs and divide-and-conquering it, but a
	// map would be need to be made for each temporary pair, and it
	// could possibly increase the amount of spans that need to be processed
	for _, o := range objs {
		trace, err := model.Unmarshal(o, dataEncoding)
		if err != nil {
			continue // TODO: log? something?
		}

		for _, batch := range trace.Batches {
			notFoundILS := batch.InstrumentationLibrarySpans[:0]

			for _, ils := range batch.InstrumentationLibrarySpans {
				notFoundSpans := ils.Spans[:0]
				for _, span := range ils.Spans {
					_, ok := m.spans[tokenForID(h, buffer, int32(span.Kind), span.SpanId)]
					if !ok {
						notFoundSpans = append(notFoundSpans, span)
					}
				}
				if len(notFoundSpans) > 0 {
					ils.Spans = notFoundSpans
					notFoundILS = append(notFoundILS, ils)
				}
			}
			if len(notFoundILS) > 0 {
				batch.InstrumentationLibrarySpans = notFoundILS
				m.current.Batches = append(m.current.Batches, batch)
			}
		}
		metricDenseIteratorMergedObjects.WithLabelValues(m.level).Inc()
	}

	return true, nil
}

func (m objectMerger) Result(dataEncoding string) ([]byte, error) {
	model.SortTrace(m.current)
	return model.Marshal(m.current, dataEncoding)
}

func (m objectMerger) Size() int {
	return m.current.Size()
}

// tokenForID returns a uint32 token for use in a hash map given a span id and span kind
//  buffer must be a 4 byte slice and is reused for writing the span kind to the hashing function
//  kind is used along with the actual id b/c in zipkin traces span id is not guaranteed to be unique
//  as it is shared between client and server spans.
func tokenForID(h hash.Hash32, buffer []byte, kind int32, b []byte) uint32 {
	binary.LittleEndian.PutUint32(buffer, uint32(kind))

	h.Reset()
	_, _ = h.Write(b)
	_, _ = h.Write(buffer)
	return h.Sum32()
}
