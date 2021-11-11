package frontend

import (
	"github.com/cortexproject/cortex/pkg/frontend/transport"
	"github.com/weaveworks/common/server"
)

type FederatorConfig struct {
	Upstreams     []UpstreamConfig        `yaml:"upstreams"`
	MaxRetries    int                     `yaml:"max_retries"`
	Server        server.Config           `yaml:"server"`
	Handler       transport.HandlerConfig `yaml:"handler"`
	DisableJaeger bool                    `yaml:"disable_jaeger"`
}

type UpstreamConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
}
