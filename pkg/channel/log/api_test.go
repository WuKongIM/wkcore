package log_test

import (
	"context"
	"errors"
	"testing"

	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
)

func TestNewValidatesRequiredDependencies(t *testing.T) {
	_, err := channellog.New(channellog.Config{})
	if !errors.Is(err, channellog.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestClusterSurfaceExposesMetaAppendFetchStatus(t *testing.T) {
	var c channellog.Cluster

	compileClusterSurface := func(cluster channellog.Cluster) {
		_ = cluster.ApplyMeta(channellog.ChannelMeta{})
		_, _ = cluster.Append(context.Background(), channellog.AppendRequest{})
		_, _ = cluster.Fetch(context.Background(), channellog.FetchRequest{})
		_, _ = cluster.Status(channellog.ChannelKey{})
	}

	_ = compileClusterSurface
	_ = c
}
