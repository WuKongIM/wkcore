package isrnode_test

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
)

func TestNewRuntimeValidatesRequiredDependencies(t *testing.T) {
	_, err := isrnode.New(isrnode.Config{})
	if !errors.Is(err, isrnode.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestRuntimeSurfaceExposesReconcileAndLookup(t *testing.T) {
	var rt isrnode.Runtime
	var meta isr.GroupMeta

	compileRuntimeSurface := func(r isrnode.Runtime) {
		_ = r.EnsureGroup(meta)
		_ = r.ApplyMeta(meta)
		_ = r.RemoveGroup(meta.GroupID)
		_, _ = r.Group(meta.GroupID)
	}

	_ = compileRuntimeSurface
	_ = rt
}
