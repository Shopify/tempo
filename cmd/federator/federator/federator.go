package app

import (
	"context"
	"flag"
	"fmt"
	"github.com/grafana/tempo/modules/frontend"
	"github.com/grafana/tempo/pkg/api"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	cortex_transport "github.com/cortexproject/cortex/pkg/frontend/transport"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/drone/envsubst"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"gopkg.in/yaml.v3"
)

func Run() error {
	var cfg frontend.FederatorConfig
	conf := flag.String("config", "config.yml", "/path/to/config.yml")
	cfg.Server.RegisterFlags(flag.CommandLine)
	flag.Parse()

	if *conf != "" {
		if bs, err := ioutil.ReadFile(*conf); err != nil {
			return err
		} else if subbedBs, err := envsubst.EvalEnv(string(bs)); err != nil {
			return err
		} else if err := yaml.Unmarshal([]byte(subbedBs), &cfg); err != nil {
			return err
		}
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if len(cfg.Upstreams) == 0 {
		cfg.Upstreams = []frontend.UpstreamConfig{{Host: "query-frontend:3100"}}
	}
	if cfg.Server.HTTPListenPort == 80 {
		cfg.Server.HTTPListenPort = 3200
	}
	cfg.Handler.QueryStatsEnabled = true

	if !cfg.DisableJaeger {
		shutdown, err := initTracing()
		defer shutdown()
		if err != nil {
			return err
		}
	}

	srv, err := server.New(cfg.Server)
	if err != nil {
		return err
	}
	defer srv.Shutdown()

	ttw, err := frontend.NewTraceByIDTripperware(cfg, log.Logger, prometheus.NewRegistry())
	if err != nil {
		return err
	}
	tracesHandler := cortex_transport.NewHandler(cfg.Handler, ttw(otelhttp.NewTransport(nil)), log.Logger, prometheus.NewRegistry())

	stw, err := frontend.NewSearchTripperware(cfg, log.Logger)
	if err != nil {
		return err
	}
	searchHandler := cortex_transport.NewHandler(cfg.Handler, stw(otelhttp.NewTransport(nil)), log.Logger, prometheus.NewRegistry())

	// http query endpoint
	srv.HTTP.Handle(api.PathTraces, tracesHandler)
	srv.HTTP.Handle(api.PathSearch, searchHandler)
	srv.HTTP.Handle(api.PathSearchTags, searchHandler)
	srv.HTTP.Handle(api.PathSearchTagValues, searchHandler)
	srv.HTTP.Handle(api.PathEcho, echoHandler())

	return srv.Run()
}

func echoHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "echo", http.StatusOK)
	}
}

func initTracing() (func(), error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint())
	if err != nil {
		return func() {}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resources, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("tempo-federator"),
		),
		resource.WithDetectors(&gcp.GKE{}),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise trace resuorces: %w", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resources),
	)
	otel.SetTracerProvider(tp)

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			level.Error(log.Logger).Log("msg", "OpenTelemetry trace provider failed to shutdown", "err", err)
			os.Exit(1)
		}
	}

	propagator := propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{})
	otel.SetTextMapPropagator(propagator)

	return shutdown, nil
}
