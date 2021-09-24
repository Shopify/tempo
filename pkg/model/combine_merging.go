package model

import (
	"bytes"
	"hash/fnv"

	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type ObjectMerger interface {
	ID() common.ID
	Merge(dataEncoding string, objs ...[]byte) (int, error)
	Result(dataEncoding string) ([]byte, error)
}

type objectMerger struct {
	lazyCurrent  []byte
	lazyEncoding string
	current      *tempopb.Trace
	id           common.ID
	spans        map[uint32]struct{}
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

var _ = MergerFactory(NewMerger)

var Warning = errors.New("warning")

type MergerFactory func(encoding string, id []byte, initial []byte) ObjectMerger

func NewMerger(encoding string, id []byte, initialObj []byte) ObjectMerger {
	return &objectMerger{
		id:           id,
		lazyEncoding: encoding,
		lazyCurrent:  initialObj,
	}
}

func (m *objectMerger) Merge(dataEncoding string, objs ...[]byte) (int, error) {
	// Nothing to do.
	if len(objs) == 0 {
		return 0, nil
	}

	// Lazy initialization has already happened. Since it doesn't matter *which* object
	// is unmarshalled first, simply append it to the current object list.
	var rem [][]byte
	if len(m.lazyCurrent) > 0 {
		// Filter out any passed objects that are equal to the originally initialized value.
		for _, obj := range objs {
			if !bytes.Equal(m.lazyCurrent, obj) {
				rem = append(rem, obj)
			}
		}
		// If all objects were equal, there is nothing to do.
		if len(rem) == 0 {
			return 0, nil
		}

		// Now that the objects are filtered, the lazyCurrent object needs to be unmarshalled with its own
		// encoding prior to use, as it may be different than the current dataEncoding.
		decoded, err := Unmarshal(m.lazyCurrent, m.lazyEncoding)
		if err != nil {
			// The lazy object failed to be unmarshalled, so it was invalid.
		} else {
			m.current = decoded
		}
		m.lazyCurrent = nil
	} else {
		rem = objs
	}

	// Find the first object which successfully unmarshals.
	var err error
	for m.current == nil && len(rem) > 0 {
		m.current, err = Unmarshal(rem[0], dataEncoding)
		if err != nil {
			level.Error(log.Logger).Log("msg", "error unmarshalling trace proto, skipping", "err", err.Error())
			rem = rem[1:]
		}
	}
	// Something bad happened.
	if err != nil {
		return 0, err
	}
	// Simply couldn't find one, which is weird, because there were no errors
	// and there were objects left to check. Sanity check.
	if m.current == nil {
		return 0, errors.New("all objects failed to unmarshal but no errors")
	}

	// An initial object has now been found, extract the spans from it to form the
	// base existence set.
	m.spans = extractSpans(m.current)

	// Create a hasher and a buffer to be used while hashing the span + kind combo.
	h := fnv.New32()
	buffer := make([]byte, 4)

	// Iterate the remaining objects, merging each batch of spans in that
	// do not already exist.
	var mergeCount int
	for _, o := range rem {
		trace, err := Unmarshal(o, dataEncoding)
		if err != nil {
			level.Error(log.Logger).Log("msg", "error unmarshalling trace proto, skipping", "err", err.Error())
			continue
		}
		mergeCount++
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
	}

	return mergeCount, nil
}

func (m *objectMerger) Result(dataEncoding string) ([]byte, error) {
	// If the merger was only lazy initialized, return a copy of the byte slice to ensure
	// data correctly escapes the underlying iterator.
	if m.lazyCurrent != nil {
		return append([]byte(nil), m.lazyCurrent...), nil
	}
	if m.current == nil {
		return marshal(&tempopb.Trace{}, dataEncoding)
	}
	// Otherwise, sort the merged trace, and serialize it.
	SortTrace(m.current)
	return marshal(m.current, dataEncoding)
}
