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

func TestRuntimeSurfaceUsesGroupKey(t *testing.T) {
	var rt isrnode.Runtime
	meta := isr.GroupMeta{GroupKey: isr.GroupKey("group-1")}

	compileRuntimeSurface := func(r isrnode.Runtime) {
		_ = r.EnsureGroup(meta)
		_ = r.ApplyMeta(meta)
		_ = r.RemoveGroup(meta.GroupKey)
		_, _ = r.Group(meta.GroupKey)
	}

	_ = compileRuntimeSurface
	_ = rt
}
