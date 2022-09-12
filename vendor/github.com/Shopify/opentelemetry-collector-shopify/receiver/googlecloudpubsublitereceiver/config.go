// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecloudpubsublitereceiver

import (
	"time"

	"cloud.google.com/go/pubsublite/pscompat"
	"go.opentelemetry.io/collector/config"
)

const typeStr = "googlecloudpubsublite"
const gzipCompression = "gzip"

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`

	Project      string `mapstructure:"project"`
	Subscription string `mapstructure:"subscription"`
	Location     string `mapstructure:"location"`

	// See pscompat.ReceiveSettings for a detailed overview of these fields
	MaxOutstandingMessages int           `mapstructure:"max_outstanding_messages"`
	MaxOutstandingBytes    int           `mapstructure:"max_outstanding_bytes"`
	Timeout                time.Duration `mapstructure:"timeout"`
	Partitions             []int         `mapstructure:"partitions"`
}

func createDefaultConfig() config.Receiver {
	return &Config{
		ReceiverSettings:       config.NewReceiverSettings(config.NewComponentID(typeStr)),
		MaxOutstandingMessages: pscompat.DefaultReceiveSettings.MaxOutstandingMessages,
		MaxOutstandingBytes:    pscompat.DefaultReceiveSettings.MaxOutstandingBytes,
		Timeout:                pscompat.DefaultReceiveSettings.Timeout,
		Partitions:             pscompat.DefaultReceiveSettings.Partitions,
	}
}
