package frontend

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/util"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("tempo-federator/federator")

// NewTraceByIDTripperware returns a Tripperware configured with a middleware to split requests
func NewTraceByIDTripperware(cfg FederatorConfig, logger log.Logger, registerer prometheus.Registerer) (queryrange.Tripperware, error) {
	level.Info(logger).Log("msg", "creating tripperware in query frontend to shard queries")
	metricFactory := promauto.With(registerer)
	queriesPerTenant := metricFactory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempo",
		Name:      "query_federator_queries_total",
		Help:      "Total queries received per tenant.",
	}, []string{"tenant"})

	qryResp := metricFactory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tempo",
		Name:      "query_responses_total",
		Help:      "Total counts of requests, grouped by method and code",
	}, []string{"code", "method"})

	qryDurs := metricFactory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tempo",
		Name:      "query_response_durations",
		Help:      "Histogram of query response times, grouped by method and code",
		Buckets:   prometheus.DefBuckets,
	}, []string{"code", "method"})

	upstreams, err := parseUpstreams(cfg.Upstreams)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse upstreams: %w", err)
	}

	return func(next http.RoundTripper) http.RoundTripper {
		next = promhttp.InstrumentRoundTripperCounter(qryResp, next)
		next = promhttp.InstrumentRoundTripperDuration(qryDurs, next)

		// We're constructing middleware in this statement, each middleware wraps the next one from left-to-right
		// - the Deduper dedupes Span IDs for Zipkin support
		// - the FederatingWare splits the request and sends it to all configured upstreams
		// - the RetryWare retries requests that have failed (error or http status 500)
		rt := NewRoundTripper(next,
			newDeduper(logger),
			FederatingWare(upstreams, logger),
			newRetryWare(cfg.MaxRetries, registerer),
		)

		return RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
			start := time.Now()
			ctx, span := tracer.Start(r.Context(), "frontend.FederatingWare")
			defer span.End()

			// ensure the request context contains the tracing details
			r = r.WithContext(ctx)

			orgID, _ := user.ExtractOrgID(r.Context())
			traceId, err := api.ParseTraceID(r)
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       ioutil.NopCloser(strings.NewReader(err.Error())),
					Header:     http.Header{},
				}, nil
			}

			span.SetAttributes(
				attribute.String("orgID", orgID),
				attribute.String("traceID", util.TraceIDToHexString(traceId)),
			)

			queriesPerTenant.WithLabelValues(orgID).Inc()

			// check marshalling format
			marshallingFormat := api.HeaderAcceptJSON
			if r.Header.Get(api.HeaderAccept) == api.HeaderAcceptProtobuf {
				marshallingFormat = api.HeaderAcceptProtobuf
			}

			// Enforce all communication internal to Tempo to be in protobuf bytes
			r.Header.Set(api.HeaderAccept, api.HeaderAcceptProtobuf)

			resp, err := rt.RoundTrip(r)
			if err != nil {
				level.Error(logger).Log("err", err.Error(), "msg", "failed to make request")
			}

			if resp != nil && resp.StatusCode == http.StatusOK {
				// if request is for application/json, unmarshal into proto object and re-marshal into json bytes
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				traceObject := &tempopb.TraceByIDResponse{}
				if err != nil {
					level.Info(logger).Log("asd", "asd")
					return nil, fmt.Errorf("error reading response body at query frontend: %w", err)
				} else if err = proto.Unmarshal(body, traceObject); err != nil {
					return nil, fmt.Errorf("cannot unmarshal trace: %w", err)
				}

				//if traceObject.Metrics.FailedBlocks > 0 {
				//	resp.StatusCode = http.StatusPartialContent
				//}

				if marshallingFormat == api.HeaderAcceptJSON {
					var jsonTrace bytes.Buffer
					marshaller := &jsonpb.Marshaler{}
					if err = marshaller.Marshal(&jsonTrace, traceObject.Trace); err != nil {
						return nil, fmt.Errorf("cannot marshal trace: %w", err)
					}
					resp.Body = ioutil.NopCloser(bytes.NewReader(jsonTrace.Bytes()))
				} else {
					traceBuffer, err := proto.Marshal(traceObject.Trace)
					if err != nil {
						return nil, err
					}
					resp.Body = io.NopCloser(bytes.NewReader(traceBuffer))
				}
			}
			statusCode := http.StatusInternalServerError
			var contentLength int64 = 0
			if resp != nil {
				statusCode = resp.StatusCode
				contentLength = resp.ContentLength
			} else if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
				statusCode = int(httpResp.Code)
				contentLength = int64(len(httpResp.Body))
			}
			level.Info(logger).Log(
				"tenant", orgID,
				"method", r.Method,
				"trace_id", trace.SpanContextFromContext(ctx).TraceID(),
				"url", r.URL.RequestURI(),
				"duration", time.Since(start).String(),
				"response_size", contentLength,
				"status", statusCode,
			)

			return resp, err
		})
	}, nil
}

// NewSearchTripperware creates a new frontend middleware to handle search and search tags requests.
func NewSearchTripperware(cfg FederatorConfig, logger log.Logger) (queryrange.Tripperware, error) {
	upstreams, err := parseUpstreams(cfg.Upstreams)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse upstreams: %w", err)
	}
	return func(next http.RoundTripper) http.RoundTripper {
		rt := NewRoundTripper(next,
			FederatingWare(upstreams, logger),
		)
		return RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
			orgID, _ := user.ExtractOrgID(r.Context())

			r.Header.Set(user.OrgIDHeaderName, orgID)
			r.RequestURI = querierPrefix + r.RequestURI

			resp, err := rt.RoundTrip(r)

			return resp, err
		})
	}, nil
}

func parseUpstreams(upstreams []UpstreamConfig) ([]*url.URL, error) {
	var ups []*url.URL
	for _, u := range upstreams {
		ur, err := url.Parse(u.Host)
		if err != nil {
			return nil, fmt.Errorf("invalid upstream: %w", err)
		}
		if u.Username != "" && u.Password != "" {
			ur.User = url.UserPassword(u.Username, u.Password)
		}
		ups = append(ups, ur)
	}
	if len(ups) == 0 {
		return nil, fmt.Errorf("no valid upstreams")
	}
	return ups, nil
}
