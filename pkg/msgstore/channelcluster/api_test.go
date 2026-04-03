package channelcluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
)

func TestNewValidatesRequiredDependencies(t *testing.T) {
	_, err := channelcluster.New(channelcluster.Config{})
	if !errors.Is(err, channelcluster.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestClusterSurfaceExposesMetaSendFetchStatus(t *testing.T) {
	var c channelcluster.Cluster

	compileClusterSurface := func(cluster channelcluster.Cluster) {
		_ = cluster.ApplyMeta(channelcluster.ChannelMeta{})
		_, _ = cluster.Send(context.Background(), channelcluster.SendRequest{})
		_, _ = cluster.Fetch(context.Background(), channelcluster.FetchRequest{})
		_, _ = cluster.Status(channelcluster.ChannelKey{})
	}

	_ = compileClusterSurface
	_ = c
}
