package replica

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestInstallSnapshotPersistsCheckpointAndState(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.leo = 8
	snap := channel.Snapshot{ChannelKey: "group-10", Epoch: 7, EndOffset: 8, Payload: []byte("snap")}

	require.NoError(t, env.replica.InstallSnapshot(context.Background(), snap))
	state := env.replica.Status()
	require.Equal(t, uint64(8), state.HW)
	require.Equal(t, uint64(8), state.LogStartOffset)
	require.Equal(t, uint64(8), env.checkpoints.lastStored().HW)
}

func TestInstallSnapshotRejectsLogStoreBehindSnapshotEndOffset(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.leo = 5
	err := env.replica.InstallSnapshot(context.Background(), channel.Snapshot{ChannelKey: "group-10", Epoch: 7, EndOffset: 8})
	if !errors.Is(err, channel.ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}
