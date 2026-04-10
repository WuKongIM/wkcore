package node_test

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
)

func TestNewRuntimeValidatesRequiredDependencies(t *testing.T) {
	_, err := isrnode.New(isrnode.Config{})
	if !errors.Is(err, isrnode.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestRuntimeSurfaceUsesChannelKey(t *testing.T) {
	var rt isrnode.Runtime
	meta := isr.ChannelMeta{ChannelKey: isr.ChannelKey("group-1")}

	compileRuntimeSurface := func(r isrnode.Runtime) {
		_ = r.EnsureChannel(meta)
		_ = r.ApplyMeta(meta)
		_ = r.RemoveChannel(meta.ChannelKey)
		_, _ = r.Channel(meta.ChannelKey)
	}

	_ = compileRuntimeSurface
	_ = rt
}
