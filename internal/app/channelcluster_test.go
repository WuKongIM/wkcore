package app

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	obsmetrics "github.com/WuKongIM/WuKongIM/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestAppChannelClusterUpdatesObservabilityMetrics(t *testing.T) {
	key := channel.ChannelKey("room")
	meta := channel.Meta{
		Key:    key,
		ID:     channel.ChannelID{ID: "room", Type: 2},
		Status: channel.StatusActive,
	}
	service := &stubChannelService{
		appendResult: channel.AppendResult{MessageID: 9, MessageSeq: 10},
		fetchResult:  channel.FetchResult{},
	}
	runtime := &stubChannelRuntime{}
	registry := obsmetrics.New(1, "node-1")
	cluster := &appChannelCluster{
		service: service,
		runtime: runtime,
		metrics: registry,
	}

	require.NoError(t, cluster.ApplyMeta(meta))

	_, err := cluster.Append(context.Background(), channel.AppendRequest{ChannelID: meta.ID})
	require.NoError(t, err)

	_, err = cluster.Fetch(context.Background(), channel.FetchRequest{ChannelID: meta.ID, Limit: 1, MaxBytes: 1})
	require.NoError(t, err)

	require.Equal(t, int64(1), registry.Channel.Snapshot().ActiveChannels)

	families, err := registry.Gather()
	require.NoError(t, err)
	appendTotal := requireMetricFamilyByName(t, families, "wukongim_channel_append_total")
	require.Len(t, appendTotal.Metric, 1)
	require.Equal(t, float64(1), appendTotal.Metric[0].GetCounter().GetValue())

	require.NoError(t, cluster.RemoveLocal(key))
	require.Equal(t, int64(0), registry.Channel.Snapshot().ActiveChannels)
}

type stubChannelService struct {
	meta         channel.Meta
	appendResult channel.AppendResult
	appendErr    error
	fetchResult  channel.FetchResult
	fetchErr     error
}

func (s *stubChannelService) ApplyMeta(meta channel.Meta) error {
	s.meta = meta
	return nil
}

func (s *stubChannelService) Append(context.Context, channel.AppendRequest) (channel.AppendResult, error) {
	return s.appendResult, s.appendErr
}

func (s *stubChannelService) Fetch(context.Context, channel.FetchRequest) (channel.FetchResult, error) {
	return s.fetchResult, s.fetchErr
}

func (s *stubChannelService) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	return channel.ChannelRuntimeStatus{}, nil
}

func (s *stubChannelService) MetaSnapshot(channel.ChannelKey) (channel.Meta, bool) {
	return s.meta, s.meta.Key != ""
}

func (s *stubChannelService) RestoreMeta(_ channel.ChannelKey, meta channel.Meta, _ bool) {
	s.meta = meta
}

type stubChannelRuntime struct {
	upserts []channel.Meta
	removes []channel.ChannelKey
}

func (s *stubChannelRuntime) UpsertMeta(meta channel.Meta) error {
	s.upserts = append(s.upserts, meta)
	return nil
}

func (s *stubChannelRuntime) RemoveChannel(key channel.ChannelKey) error {
	s.removes = append(s.removes, key)
	return nil
}

func requireMetricFamilyByName(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}
