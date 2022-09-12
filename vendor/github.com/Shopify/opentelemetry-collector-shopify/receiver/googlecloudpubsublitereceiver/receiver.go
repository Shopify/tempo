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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

// pubsubliteReceiver receives protobuf encoded OTLP data from Google Cloud PubSub Lite
type pubsubliteReceiver struct {
	Config
	logger *zap.Logger

	client   *pscompat.SubscriberClient
	shutdown context.CancelFunc

	tracesConsumer    consumer.Traces
	tracesUnmarshaler pdata.TracesUnmarshaler
}

// decodeTraces decodes a given pubsub.Message as traces, optionally using the compression specified
// in the `content-encoding` message attribute.
func (receiver *pubsubliteReceiver) decodeTraces(m *pubsub.Message) (pdata.Traces, error) {
	var err error
	var bs []byte

	bs = m.Data
	if m.Attributes["content-encoding"] == gzipCompression {
		bs, err = receiver.decompressData(m.Data)
		if err != nil {
			return pdata.Traces{}, err
		}
	}
	return receiver.tracesUnmarshaler.UnmarshalTraces(bs)
}

func (receiver *pubsubliteReceiver) decompressData(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			receiver.logger.Warn("couldn't close message reader", zap.Error(err))
		}
	}()
	required := gzipDecompressedSize(data)
	defer reader.Close()
	bs := make([]byte, required)
	_, err = io.ReadAtLeast(reader, bs, required)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (receiver *pubsubliteReceiver) processMessage(ctx context.Context, m *pubsub.Message) {
	// Need partition ID for some stats
	metadata, err := pscompat.ParseMessageMetadata(m.ID)
	if err == nil {
		ctx, _ = tag.New(ctx,
			tag.Insert(partitionKey, strconv.Itoa(metadata.Partition)),
		)
	}
	stats.Record(ctx, mReceivedBundles.M(1), mPubsubMessageSize.M(int64(len(m.Data))))
	defer m.Ack()

	traces, err := receiver.decodeTraces(m)
	if err != nil {
		stats.Record(ctx, mPubSubDeserializationErrors.M(1))
		receiver.logger.Error("couldn't deserialize trace data", zap.Error(err))
		return
	}

	stats.Record(ctx, mSpansPerBundle.M(int64(traces.SpanCount())))

	err = receiver.tracesConsumer.ConsumeTraces(ctx, traces)
	if err != nil {
		receiver.logger.Error("ConsumeTracesExt", zap.Error(err))
		return
	}
}

// Start the receiver by starting the PubSub Lite receiver
func (receiver *pubsubliteReceiver) Start(_ context.Context, host component.Host) error {
	id := strings.Join([]string{"projects", receiver.Project, "locations", receiver.Location, "subscriptions", receiver.Subscription}, "/")
	sub, err := pscompat.NewSubscriberClientWithSettings(context.Background(), id, pscompat.ReceiveSettings{
		MaxOutstandingMessages: receiver.MaxOutstandingMessages,
		MaxOutstandingBytes:    receiver.MaxOutstandingBytes,
		Timeout:                receiver.Timeout,
		Partitions:             receiver.Partitions,
	})
	if err != nil {
		return fmt.Errorf("failed creating the gRPC client to Pubsub: %w", err)
	}
	receiver.client = sub
	receiver.tracesUnmarshaler = otlp.NewProtobufTracesUnmarshaler()

	// Start receiving spans from PubSub Lite. Receive can only be canceled via
	// context cancellation, so store the cancel func on the receiver itself to
	// be used during Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	receiver.shutdown = cancel
	go func() {
		if err := receiver.client.Receive(ctx, receiver.processMessage); err != nil {
			host.ReportFatalError(err)
		}
	}()
	return nil
}

// Shutdown terminates the PubSub subscriber
func (receiver *pubsubliteReceiver) Shutdown(context.Context) error {
	if receiver.shutdown != nil {
		receiver.shutdown()
		receiver.shutdown = nil
	}
	return nil
}

// gzipDecompressedSize finds the decompressed size of a gzip byte array for preallocation
func gzipDecompressedSize(buf []byte) int {
	last := len(buf)
	if last < 4 {
		return len(buf)
	}
	return int(binary.LittleEndian.Uint32(buf[last-4 : last]))
}
