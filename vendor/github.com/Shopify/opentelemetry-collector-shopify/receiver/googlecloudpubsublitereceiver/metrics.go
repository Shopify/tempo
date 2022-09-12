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
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	mReceivedBundles             = stats.Int64("pslite_rx_bundles", "count of bundles", stats.UnitDimensionless)
	mSpansPerBundle              = stats.Int64("pslite_rx_spans_per_bundle", "number of spans per bundle", stats.UnitDimensionless)
	mMetricsPerBundle            = stats.Int64("pslite_rx_metrics_per_bundle", "number of metrics per bundle", stats.UnitDimensionless)
	mLogsPerBundle               = stats.Int64("pslite_rx_logs_per_bundle", "number of logs per bundle", stats.UnitDimensionless)
	mPubsubMessageSize           = stats.Int64("pslite_rx_msg_size", "", stats.UnitBytes)
	mPubSubDeserializationErrors = stats.Int64("pslite_rx_deserialization_errors", "", stats.UnitDimensionless)
	partitionKey                 = tag.MustNewKey("partition")
)

func init() {
	views := []*view.View{
		{
			Measure:     mReceivedBundles,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{partitionKey},
		},
		{
			Measure:     mSpansPerBundle,
			Aggregation: view.Distribution(0.0, 64, 128, 256, 512, 1024, 2048, 4096, 8096),
		},
		{
			Measure:     mMetricsPerBundle,
			Aggregation: view.Distribution(0.0, 64, 128, 256, 512, 1024, 2048, 4096, 8096),
		},
		{
			Measure:     mLogsPerBundle,
			Aggregation: view.Distribution(0.0, 64, 128, 256, 512, 1024, 2048, 4096, 8096),
		},
		{
			Measure:     mPubsubMessageSize,
			Aggregation: view.Distribution(0.0, 8096, 8096*2, 8096*4, 8096*8, 8096*16),
		},
		{
			Measure:     mPubSubDeserializationErrors,
			Aggregation: view.Sum(),
		},
	}

	err := view.Register(views...)
	if err != nil {
		panic(err)
	}
}
