package proxy

import (
	"context"
	"errors"
	"fmt"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const (
	rpcStatusOK        = "ok"
	rpcStatusNotLeader = "not_leader"
	rpcStatusNoLeader  = "no_leader"
	rpcStatusNoGroup   = "no_group"
	rpcStatusNotFound  = "not_found"
)

type authoritativeRPCResponse interface {
	rpcStatus() string
	rpcLeaderID() uint64
}

type rpcStatusEncoder func(status string, leaderID uint64) ([]byte, error)

func (s *Store) shouldServeGroupLocally(groupID multiraft.GroupID) bool {
	if s.cluster == nil || s.singleLocalPeerGroup(groupID) {
		return true
	}
	leaderID, err := s.cluster.LeaderOf(groupID)
	return err == nil && s.cluster.IsLocal(leaderID)
}

func callAuthoritativeRPC[T authoritativeRPCResponse](
	ctx context.Context,
	s *Store,
	groupID multiraft.GroupID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
) (T, error) {
	var zero T

	if s.cluster == nil {
		return zero, fmt.Errorf("metastore: cluster not configured")
	}

	peers := s.cluster.PeersForGroup(groupID)
	if len(peers) == 0 {
		return zero, raftcluster.ErrGroupNotFound
	}

	tried := make(map[multiraft.NodeID]struct{}, len(peers))
	candidates := append([]multiraft.NodeID(nil), peers...)
	var lastErr error

	for len(candidates) > 0 {
		peer := candidates[0]
		candidates = candidates[1:]
		if _, ok := tried[peer]; ok {
			continue
		}
		tried[peer] = struct{}{}

		body, err := s.cluster.RPCService(ctx, peer, groupID, serviceID, payload)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := decode(body)
		if err != nil {
			lastErr = err
			continue
		}

		switch resp.rpcStatus() {
		case rpcStatusOK, rpcStatusNotFound:
			return resp, nil
		case rpcStatusNotLeader:
			if leaderID := multiraft.NodeID(resp.rpcLeaderID()); leaderID != 0 {
				if _, ok := tried[leaderID]; !ok {
					candidates = append([]multiraft.NodeID{leaderID}, candidates...)
				}
				continue
			}
		case rpcStatusNoLeader:
			lastErr = raftcluster.ErrNoLeader
			continue
		case rpcStatusNoGroup:
			lastErr = raftcluster.ErrGroupNotFound
			continue
		default:
			lastErr = fmt.Errorf("metastore: unexpected rpc status %q", resp.rpcStatus())
			continue
		}
	}

	if lastErr != nil {
		return zero, lastErr
	}
	return zero, raftcluster.ErrNoLeader
}

func (s *Store) handleAuthoritativeRPC(groupID multiraft.GroupID, encode rpcStatusEncoder) ([]byte, bool, error) {
	leaderID, err := s.cluster.LeaderOf(groupID)
	switch {
	case errors.Is(err, raftcluster.ErrGroupNotFound):
		body, encodeErr := encode(rpcStatusNoGroup, 0)
		return body, true, encodeErr
	case err != nil:
		body, encodeErr := encode(rpcStatusNoLeader, 0)
		return body, true, encodeErr
	case !s.cluster.IsLocal(leaderID):
		body, encodeErr := encode(rpcStatusNotLeader, uint64(leaderID))
		return body, true, encodeErr
	default:
		return nil, false, nil
	}
}
