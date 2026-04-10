package cluster

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

// raftTransport adapts transport.Client to multiraft.Transport.
type raftTransport struct {
	client *transport.Client
}

func (t *raftTransport) Send(ctx context.Context, batch []multiraft.Envelope) error {
	for _, env := range batch {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		data, err := env.Message.Marshal()
		if err != nil {
			return err
		}
		body := encodeRaftBody(uint64(env.GroupID), data)
		// Individual send failures are silently skipped — the raft layer
		// handles retransmission. Only context cancellation is propagated.
		_ = t.client.Send(uint64(env.Message.To), uint64(env.GroupID), msgTypeRaft, body)
	}
	return nil
}
