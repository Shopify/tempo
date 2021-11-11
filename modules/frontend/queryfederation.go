package frontend

import (
	"bytes"
	"github.com/grafana/tempo/pkg/api"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/grafana/tempo/pkg/tempopb"

	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/grafana/tempo/pkg/model"
	"github.com/weaveworks/common/user"
)

func FederatingWare(upstreams []*url.URL, logger kitlog.Logger) Middleware {
	return MiddlewareFunc(func(next http.RoundTripper) http.RoundTripper {
		return federatingHandler{
			next:      next,
			upstreams: upstreams,
			logger:    logger,
		}
	})
}

type federatingHandler struct {
	next      http.RoundTripper
	upstreams []*url.URL
	logger    kitlog.Logger
}

// RoundTrip implements RoundTripper
func (s federatingHandler) RoundTrip(r *http.Request) (*http.Response, error) {
	reqs := make([]*http.Request, len(s.upstreams))

	for i, up := range s.upstreams {
		reqs[i] = r.Clone(r.Context())
		password, ok := up.User.Password()
		if ok {
			reqs[i].SetBasicAuth(up.User.Username(), password)
			reqs[i].URL.User = up.User
		}
		reqs[i].URL.Scheme = up.Scheme
		reqs[i].URL.Host = up.Host
		reqs[i].Host = up.Host
		reqs[i].Header.Set(user.OrgIDHeaderName, "0")
		reqs[i].Header.Set(api.HeaderAccept, api.HeaderAcceptProtobuf)
	}

	// execute requests
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}

	var overallError error
	overallTrace := &tempopb.Trace{}
	statusCode := http.StatusNotFound
	statusMsg := "trace not found"

	for _, req := range reqs {
		wg.Add(1)
		go func(innerR *http.Request) {
			defer wg.Done()

			resp, err := s.next.RoundTrip(innerR)

			mtx.Lock()
			defer mtx.Unlock()
			if err != nil {
				overallError = err
			}

			if shouldQuit(r.Context(), statusCode, overallError) {
				return
			}

			// check http error
			if err != nil {
				_ = level.Error(s.logger).Log("msg", "error querying proxy target", "url", innerR.RequestURI, "err", err)
				overallError = err
				return
			}

			// if the status code is anything but happy, save the error and pass it down the line
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
				// todo: if we cancel the parent context here will it shortcircuit the other queries and fail fast?
				statusCode = resp.StatusCode
				bytesMsg, err := io.ReadAll(resp.Body)
				if err != nil {
					_ = level.Error(s.logger).Log("msg", "error reading response body status != ok", "url", innerR.RequestURI, "err", err)
				}
				statusMsg = string(bytesMsg)
				return
			}

			// read the body
			buff, err := io.ReadAll(resp.Body)
			if err != nil {
				_ = level.Error(s.logger).Log("msg", "error reading response body status == ok", "url", innerR.RequestURI, "err", err)
				overallError = err
				return
			}

			// marshal into a trace to combine.
			// todo: better define responsibilities between middleware. the parent middleware in frontend.go actually sets the header
			//  which forces the body here to be a proto encoded tempopb.Trace{}
			traceResp := &tempopb.Trace{}
			err = proto.Unmarshal(buff, traceResp)
			if err != nil {
				_ = level.Error(s.logger).Log("msg", "error unmarshalling response", "url", innerR.RequestURI, "err", err, "body", string(buff))
				overallError = err
				return
			}

			// if not found bail
			if resp.StatusCode == http.StatusNotFound {
				return
			}

			// happy path
			statusCode = http.StatusOK
			overallTrace, _, _, _ = model.CombineTraceProtos(overallTrace, traceResp)
		}(req)
	}
	wg.Wait()

	if overallError != nil {
		return nil, overallError
	}

	if overallTrace == nil || statusCode != http.StatusOK {
		// translate non-404s into 500s. if, for instance, we get a 400 back from an internal component
		// it means that we created a bad request. 400 should not be propagated back to the user b/c
		// the bad request was due to a bug on our side, so return 500 instead.
		if statusCode != http.StatusNotFound {
			statusCode = 500
		}

		return &http.Response{
			StatusCode: statusCode,
			Body:       io.NopCloser(strings.NewReader(statusMsg)),
			Header:     http.Header{},
		}, nil
	}

	buff, err := proto.Marshal(&tempopb.TraceByIDResponse{
		Trace: overallTrace,
	})
	if err != nil {
		_ = level.Error(s.logger).Log("msg", "error marshalling response to proto", "err", err)
		return nil, err
	}

	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{},
		Body:          io.NopCloser(bytes.NewReader(buff)),
		ContentLength: int64(len(buff)),
	}, nil
}
