package transport

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	wktransport "github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/stretchr/testify/require"
)

type statusOnlyFetchService struct {
	channel *statusOnlyChannel
}

func (s statusOnlyFetchService) ServeFetch(context.Context, runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
	return runtime.FetchResponseEnvelope{}, nil
}

func (s statusOnlyFetchService) Channel(key channel.ChannelKey) (channel.HandlerChannel, bool) {
	if s.channel == nil || s.channel.key != key {
		return nil, false
	}
	return s.channel, true
}

type statusOnlyChannel struct {
	key    channel.ChannelKey
	status channel.ReplicaState
}

func (c *statusOnlyChannel) ID() channel.ChannelKey {
	return c.key
}

func (c *statusOnlyChannel) Meta() channel.Meta {
	return channel.Meta{}
}

func (c *statusOnlyChannel) Status() channel.ReplicaState {
	return c.status
}

func (c *statusOnlyChannel) Append(context.Context, []channel.Record) (channel.CommitResult, error) {
	return channel.CommitResult{}, nil
}

func TestHandleProgressAckRPCDoesNotAdvertiseFinalLeaderHWWhileCommitNotReady(t *testing.T) {
	makeTransport := func(status channel.ReplicaState) (*Transport, error) {
		client := wktransport.NewClient(wktransport.NewPool(staticDiscovery{
			addrs: map[uint64]string{},
		}, 1, time.Second))

		service := statusOnlyFetchService{
			channel: &statusOnlyChannel{
				key:    "group-1",
				status: status,
			},
		}
		return New(Options{
			LocalNode:    1,
			Client:       client,
			RPCMux:       wktransport.NewRPCMux(),
			FetchService: service,
		})
	}

	run := func(t *testing.T, status channel.ReplicaState, wantLeaderHW uint64) {
		t.Helper()

		adapter, err := makeTransport(status)
		require.NoError(t, err)
		defer adapter.client.Stop()

		body, err := encodeProgressAck(runtime.ProgressAckEnvelope{
			ChannelKey:  "group-1",
			Epoch:       3,
			ReplicaID:   2,
			MatchOffset: 8,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		respBody, err := adapter.handleProgressAckRPC(ctx, body)
		require.NoError(t, err)
		require.Less(t, time.Since(start), 20*time.Millisecond)

		resp, err := decodeProgressAckResponse(respBody)
		require.NoError(t, err)
		require.Equal(t, wantLeaderHW, resp.LeaderHW)
	}

	t.Run("not ready", func(t *testing.T) {
		status := channel.ReplicaState{
			Epoch: 3,
			HW:    8,
			LEO:   8,
		}
		setOptionalReplicaStateField(t, &status, "CheckpointHW", uint64(3))
		setOptionalReplicaStateField(t, &status, "CommitReady", false)
		run(t, status, 0)
	})

	t.Run("ready", func(t *testing.T) {
		status := channel.ReplicaState{
			Epoch: 3,
			HW:    8,
			LEO:   8,
		}
		setOptionalReplicaStateField(t, &status, "CheckpointHW", uint64(8))
		setOptionalReplicaStateField(t, &status, "CommitReady", true)
		run(t, status, 8)
	})
}

func setOptionalReplicaStateField(t *testing.T, state *channel.ReplicaState, field string, value any) {
	t.Helper()

	got := reflect.ValueOf(state).Elem().FieldByName(field)
	if !got.IsValid() {
		return
	}
	switch got.Kind() {
	case reflect.Bool:
		boolValue, ok := value.(bool)
		require.True(t, ok, "expected bool value for %s", field)
		got.SetBool(boolValue)
	case reflect.Uint64:
		uintValue, ok := value.(uint64)
		require.True(t, ok, "expected uint64 value for %s", field)
		got.SetUint(uintValue)
	default:
		t.Fatalf("ReplicaState.%s kind = %s, unsupported for test setup", field, got.Kind())
	}
}
