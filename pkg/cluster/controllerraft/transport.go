package controllerraft

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	msgTypeControllerRaft uint8  = 2
	controllerShardKey    uint64 = 1
)

type transport struct {
	client *nodetransport.Client
}

func newTransport(pool *nodetransport.Pool) *transport {
	return &transport{
		client: nodetransport.NewClient(pool),
	}
}

func (t *transport) Close() {
	if t == nil || t.client == nil {
		return
	}
	t.client.Stop()
}

func (t *transport) Send(ctx context.Context, messages []raftpb.Message) error {
	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		// Match multiraft transport semantics: transient send failures are not fatal
		// to the local raft loop and are retried by later raft traffic.
		_ = t.client.Send(uint64(msg.To), controllerShardKey, msgTypeControllerRaft, data)
	}
	return nil
}
