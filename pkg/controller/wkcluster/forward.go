package wkcluster

import (
	"context"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

func (c *Cluster) forwardToLeader(ctx context.Context, leaderID multiraft.NodeID, groupID multiraft.GroupID, cmd []byte) error {
	payload := encodeForwardPayload(uint64(groupID), cmd)
	resp, err := c.fwdClient.RPC(ctx, uint64(leaderID), uint64(groupID), payload)
	if err != nil {
		return err
	}
	errCode, _, decodeErr := decodeForwardResp(resp)
	if decodeErr != nil {
		return fmt.Errorf("decode forward response: %w", decodeErr)
	}
	switch errCode {
	case errCodeOK:
		return nil
	case errCodeNotLeader:
		return ErrNotLeader
	case errCodeTimeout:
		return nodetransport.ErrTimeout
	case errCodeNoGroup:
		return ErrGroupNotFound
	default:
		return fmt.Errorf("unknown forward error code: %d", errCode)
	}
}

// handleForwardRPC is the server-side RPC handler for forwarded proposals.
func (c *Cluster) handleForwardRPC(ctx context.Context, body []byte) ([]byte, error) {
	groupID, cmd, err := decodeForwardPayload(body)
	if err != nil {
		return encodeForwardResp(errCodeNoGroup, nil), nil
	}
	if c.stopped.Load() {
		return encodeForwardResp(errCodeTimeout, nil), nil
	}
	_, err = c.runtime.Status(multiraft.GroupID(groupID))
	if err != nil {
		return encodeForwardResp(errCodeNoGroup, nil), nil
	}
	future, err := c.runtime.Propose(ctx, multiraft.GroupID(groupID), cmd)
	if err != nil {
		return encodeForwardResp(errCodeNotLeader, nil), nil
	}
	result, err := future.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return encodeForwardResp(errCodeTimeout, nil), nil
		}
		return encodeForwardResp(errCodeNotLeader, nil), nil
	}
	return encodeForwardResp(errCodeOK, result.Data), nil
}
