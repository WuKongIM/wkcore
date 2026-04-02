package multiisr_test

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
	"github.com/WuKongIM/WuKongIM/pkg/multiisr"
)

func TestNewRuntimeValidatesRequiredDependencies(t *testing.T) {
	_, err := multiisr.New(multiisr.Config{})
	if !errors.Is(err, multiisr.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestRuntimeSurfaceExposesReconcileAndLookup(t *testing.T) {
	var rt multiisr.Runtime
	var meta isr.GroupMeta

	compileRuntimeSurface := func(r multiisr.Runtime) {
		_ = r.EnsureGroup(meta)
		_ = r.ApplyMeta(meta)
		_ = r.RemoveGroup(meta.GroupID)
		_, _ = r.Group(meta.GroupID)
	}

	_ = compileRuntimeSurface
	_ = rt
}
