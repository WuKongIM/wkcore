package replica

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestNewReplicaTruncatesDirtyTailToCheckpointHW(t *testing.T) {
	env := newTestEnv(t)
	env.log.leo = 5
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = channel.Checkpoint{Epoch: 3, LogStartOffset: 0, HW: 3}

	r := newReplicaFromEnv(t, env)
	st := r.Status()
	require.Equal(t, uint64(3), st.LEO)
	require.Equal(t, uint64(3), st.HW)
	require.Equal(t, []uint64{3}, env.log.truncateCalls)
}

func TestNewReplicaRejectsCheckpointBeyondLEO(t *testing.T) {
	env := newTestEnv(t)
	env.log.leo = 2
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = channel.Checkpoint{LogStartOffset: 0, HW: 3}

	_, err := NewReplica(env.config())
	if !errors.Is(err, channel.ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func TestNewReplicaBootstrapsEmptyState(t *testing.T) {
	env := newTestEnv(t)
	r := newReplicaFromEnv(t, env)
	st := r.Status()
	require.Equal(t, uint64(0), st.LogStartOffset)
	require.Equal(t, uint64(0), st.HW)
	require.Equal(t, uint64(0), st.LEO)
}
