package frontend

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	t    *testing.T
	resp *tempopb.Trace
	auth bool
}

func (ts testServer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	b1, err := proto.Marshal(ts.resp)
	assert.NoError(ts.t, err)
	rw.Write(b1)
	if ts.auth {
		user, pass, _ := r.BasicAuth()
		assert.Equal(ts.t, "user1", user)
		assert.Equal(ts.t, "pass1", pass)
	}
	if r.Body != nil {
		r.Body.Close()
	}
}

type rt struct{}

func (rt) RoundTrip(r *http.Request) (*http.Response, error) {
	return http.DefaultTransport.RoundTrip(r)
}

func TestFederation(t *testing.T) {
	tr1 := test.MakeTrace(10, []byte{0x01, 0x02})
	tr2 := test.MakeTrace(10, []byte{0x01, 0x03})
	t1 := testServer{resp: tr1, t: t, auth: true}
	t2 := testServer{resp: tr2, t: t, auth: false}

	ts1 := httptest.NewServer(t1)
	ts2 := httptest.NewServer(t2)
	defer ts1.Close()
	defer ts2.Close()

	ups, err := parseUpstreams([]UpstreamConfig{{"user1", "pass1", ts1.URL}, {Host: ts2.URL}})
	require.NoError(t, err)

	sw := FederatingWare(ups, log.Logger)
	h := sw.Wrap(&rt{})

	rsp, err := h.RoundTrip(httptest.NewRequest("GET", "http://doesnt/matter", nil))
	require.NoError(t, err)

	bs, err := ioutil.ReadAll(rsp.Body)
	require.NoError(t, err)

	combined, _, _, _ := model.CombineTraceProtos(tr1, tr2)
	wantBs, err := proto.Marshal(combined)
	assert.NoError(t, err)
	assert.Equal(t, wantBs, bs)
}
