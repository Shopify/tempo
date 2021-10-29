package model

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/test"
)

func BenchmarkMergeIdenticalTraces(b *testing.B) {
	for _, obs := range []int{2, 3, 5, 8} {
		b.Run(fmt.Sprint(obs), func(b *testing.B) {
			bs := traceCount(1)
			for i := 0; i < obs; i++ {
				bs = append(bs, bs[0])
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := NewMerger("", nil, bs[0])
				m.Merge("", bs[1:]...)
				m.Result("")
			}
		})
	}
}

func BenchmarkMergeTraces(b *testing.B) {
	for _, obs := range []int{2, 3, 5, 8} {
		b.Run(fmt.Sprint(obs), func(b *testing.B) {
			bs := traceCount(obs)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := &objectMerger{
					lazyCurrent: bs[0],
				}
				m.Merge("", bs[1:]...)
				m.Result("")
			}
		})
	}
}

func traceCount(i int) (out [][]byte) {
	for ; i > 0; i-- {
		t := test.MakeTraceWithSpanCount(20, 30, []byte{0x01, byte(i)})
		b1, _ := proto.Marshal(t)
		out = append(out, b1)
	}
	return
}

func TestMerge(t *testing.T) {
	t1 := test.MakeTrace(10, []byte{0x01, 0x02})
	t2 := test.MakeTrace(10, []byte{0x01, 0x03})

	SortTrace(t1)
	SortTrace(t2)

	// split t2 into two traces
	t2a := &tempopb.Trace{}
	t2b := &tempopb.Trace{}
	for _, b := range t2.Batches {
		if rand.Int()%2 == 0 {
			t2a.Batches = append(t2a.Batches, b)
		} else {
			t2b.Batches = append(t2b.Batches, b)
		}
	}

	tests := []struct {
		name          string
		trace1        *tempopb.Trace
		trace2        *tempopb.Trace
		expected      *tempopb.Trace
		expectError   bool
		expectedCount int
	}{
		{
			name:          "same trace",
			trace1:        t1,
			trace2:        t1,
			expected:      t1,
			expectedCount: 0,
		},
		{
			name:          "t2 is bad",
			trace1:        t1,
			trace2:        nil,
			expected:      t1,
			expectError:   false,
			expectedCount: 0,
		},
		{
			name:          "t1 is bad",
			trace1:        nil,
			trace2:        t2,
			expected:      t2,
			expectError:   false,
			expectedCount: 1,
		},
		{
			name:          "combine trace",
			trace1:        t2a,
			trace2:        t2b,
			expected:      t2,
			expectedCount: 1,
		},
		{
			name:          "both bad",
			trace1:        nil,
			trace2:        nil,
			expected:      nil,
			expectError:   true,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		for _, enc1 := range allEncodings {
			for _, enc2 := range allEncodings {
				t.Run(fmt.Sprintf("%s:%s:%s", tt.name, enc1, enc2), func(t *testing.T) {
					tt := tt
					var b1 []byte
					var b2 []byte
					if tt.trace1 != nil { // nil means substitute garbage data
						b1 = mustMarshal(tt.trace1, enc1)
					} else {
						b1 = []byte{0x01, 0x02}
					}
					if tt.trace2 != nil { // nil means substitute garbage data
						b2 = mustMarshal(tt.trace2, enc2)
					} else {
						b2 = []byte{0x01, 0x02, 0x03}
					}

					// When the encodings are different, at least one merge must happen. This is needed
					// because when the encodings and the traces are the same, there will be no merges, but when
					// the encodings are different, one merge will happen.
					if enc1 != enc2 && tt.trace1 != nil && tt.trace2 != nil {
						if tt.expectedCount == 0 {
							tt.expectedCount = 1
						}
					}

					m := NewMerger(enc1, nil, b1)
					mergeCount, err := m.Merge(enc2, b2)
					if tt.expectError {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, tt.expectedCount, mergeCount)
					}

					res, err := m.Result(enc1)
					assert.NoError(t, err)
					if tt.expected != nil {
						expected := mustMarshal(tt.expected, enc1)
						assert.Equal(t, expected, res)
					}
				})
			}
		}
	}
}

func TestMergeNils(t *testing.T) {
	test := test.MakeTrace(1, nil)
	SortTrace(test)

	for _, enc1 := range allEncodings {
		for _, enc2 := range allEncodings {
			t.Run(fmt.Sprintf("%s:%s", enc1, enc2), func(t *testing.T) {
				// both nil
				actualBytes, _, err := CombineTraceBytes(nil, nil, enc1, enc2)
				require.NoError(t, err)
				assert.Equal(t, []byte(nil), actualBytes)

				testBytes1, err := marshal(test, enc1)
				require.NoError(t, err)
				testBytes2, err := marshal(test, enc2)
				require.NoError(t, err)

				// objB nil
				actualBytes, _, err = CombineTraceBytes(testBytes1, nil, enc1, enc2)
				require.NoError(t, err)

				actual, err := Unmarshal(actualBytes, enc1)
				require.NoError(t, err)
				assert.Equal(t, test, actual)

				// objA nil
				actualBytes, _, err = CombineTraceBytes(nil, testBytes2, enc1, enc2)
				require.NoError(t, err)

				actual, err = Unmarshal(actualBytes, enc1)
				require.NoError(t, err)
				assert.Equal(t, test, actual)
			})
		}
	}
}
