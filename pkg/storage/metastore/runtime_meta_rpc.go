package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

const runtimeMetaRPCServiceID uint8 = 3

const (
	runtimeMetaRPCGet  = "get"
	runtimeMetaRPCList = "list"
)

const (
	runtimeMetaRPCStatusOK        = "ok"
	runtimeMetaRPCStatusNotLeader = "not_leader"
	runtimeMetaRPCStatusNoLeader  = "no_leader"
	runtimeMetaRPCStatusNoGroup   = "no_group"
	runtimeMetaRPCStatusNotFound  = "not_found"
)

type runtimeMetaRPCRequest struct {
	Op          string `json:"op"`
	GroupID     uint64 `json:"group_id"`
	ChannelID   string `json:"channel_id,omitempty"`
	ChannelType int64  `json:"channel_type,omitempty"`
}

type runtimeMetaRPCResponse struct {
	Status   string                      `json:"status"`
	LeaderID uint64                      `json:"leader_id,omitempty"`
	Meta     *metadb.ChannelRuntimeMeta  `json:"meta,omitempty"`
	Metas    []metadb.ChannelRuntimeMeta `json:"metas,omitempty"`
}

func (s *Store) getChannelRuntimeMetaAuthoritative(ctx context.Context, groupID multiraft.GroupID, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	if s.cluster == nil {
		return s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, channelID, channelType)
	}
	if s.singleLocalPeerGroup(groupID) {
		return s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, channelID, channelType)
	}
	if leaderID, err := s.cluster.LeaderOf(groupID); err == nil && s.cluster.IsLocal(leaderID) {
		return s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, channelID, channelType)
	}

	resp, err := s.callRuntimeMetaRPC(ctx, groupID, runtimeMetaRPCRequest{
		Op:          runtimeMetaRPCGet,
		GroupID:     uint64(groupID),
		ChannelID:   channelID,
		ChannelType: channelType,
	})
	if err != nil {
		return metadb.ChannelRuntimeMeta{}, err
	}
	if resp.Meta == nil {
		return metadb.ChannelRuntimeMeta{}, metadb.ErrNotFound
	}
	return *resp.Meta, nil
}

func (s *Store) listChannelRuntimeMetaAuthoritative(ctx context.Context, groupID multiraft.GroupID) ([]metadb.ChannelRuntimeMeta, error) {
	if s.cluster == nil {
		return s.db.ListChannelRuntimeMeta(ctx)
	}
	if s.singleLocalPeerGroup(groupID) {
		metas, err := s.db.ListChannelRuntimeMeta(ctx)
		if err != nil {
			return nil, err
		}
		return filterChannelRuntimeMetaByGroup(s.cluster, groupID, metas), nil
	}
	if leaderID, err := s.cluster.LeaderOf(groupID); err == nil && s.cluster.IsLocal(leaderID) {
		metas, err := s.db.ListChannelRuntimeMeta(ctx)
		if err != nil {
			return nil, err
		}
		return filterChannelRuntimeMetaByGroup(s.cluster, groupID, metas), nil
	}

	resp, err := s.callRuntimeMetaRPC(ctx, groupID, runtimeMetaRPCRequest{
		Op:      runtimeMetaRPCList,
		GroupID: uint64(groupID),
	})
	if err != nil {
		return nil, err
	}
	return append([]metadb.ChannelRuntimeMeta(nil), resp.Metas...), nil
}

func (s *Store) callRuntimeMetaRPC(ctx context.Context, groupID multiraft.GroupID, req runtimeMetaRPCRequest) (runtimeMetaRPCResponse, error) {
	if s.cluster == nil {
		return runtimeMetaRPCResponse{}, fmt.Errorf("metastore: cluster not configured")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return runtimeMetaRPCResponse{}, err
	}

	peers := s.cluster.PeersForGroup(groupID)
	if len(peers) == 0 {
		return runtimeMetaRPCResponse{}, raftcluster.ErrGroupNotFound
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

		body, err := s.cluster.RPCService(ctx, peer, groupID, runtimeMetaRPCServiceID, payload)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := decodeRuntimeMetaRPCResponse(body)
		if err != nil {
			lastErr = err
			continue
		}

		switch resp.Status {
		case runtimeMetaRPCStatusOK, runtimeMetaRPCStatusNotFound:
			return resp, nil
		case runtimeMetaRPCStatusNotLeader:
			if resp.LeaderID != 0 {
				leaderID := multiraft.NodeID(resp.LeaderID)
				if _, ok := tried[leaderID]; !ok {
					candidates = append([]multiraft.NodeID{leaderID}, candidates...)
				}
				continue
			}
		case runtimeMetaRPCStatusNoLeader:
			lastErr = raftcluster.ErrNoLeader
			continue
		case runtimeMetaRPCStatusNoGroup:
			lastErr = raftcluster.ErrGroupNotFound
			continue
		default:
			lastErr = fmt.Errorf("metastore: unexpected runtime meta rpc status %q", resp.Status)
			continue
		}
	}

	if lastErr != nil {
		return runtimeMetaRPCResponse{}, lastErr
	}
	return runtimeMetaRPCResponse{}, raftcluster.ErrNoLeader
}

func (s *Store) handleRuntimeMetaRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req runtimeMetaRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	groupID := multiraft.GroupID(req.GroupID)
	leaderID, err := s.cluster.LeaderOf(groupID)
	switch {
	case errors.Is(err, raftcluster.ErrGroupNotFound):
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{Status: runtimeMetaRPCStatusNoGroup})
	case err != nil:
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{Status: runtimeMetaRPCStatusNoLeader})
	case !s.cluster.IsLocal(leaderID):
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status:   runtimeMetaRPCStatusNotLeader,
			LeaderID: uint64(leaderID),
		})
	}

	switch req.Op {
	case runtimeMetaRPCGet:
		meta, err := s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, req.ChannelID, req.ChannelType)
		if errors.Is(err, metadb.ErrNotFound) {
			return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{Status: runtimeMetaRPCStatusNotFound})
		}
		if err != nil {
			return nil, err
		}
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status: runtimeMetaRPCStatusOK,
			Meta:   &meta,
		})
	case runtimeMetaRPCList:
		metas, err := s.db.ListChannelRuntimeMeta(ctx)
		if err != nil {
			return nil, err
		}
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status: runtimeMetaRPCStatusOK,
			Metas:  filterChannelRuntimeMetaByGroup(s.cluster, groupID, metas),
		})
	default:
		return nil, fmt.Errorf("metastore: unknown runtime meta rpc op %q", req.Op)
	}
}

func filterChannelRuntimeMetaByGroup(cluster *raftcluster.Cluster, groupID multiraft.GroupID, metas []metadb.ChannelRuntimeMeta) []metadb.ChannelRuntimeMeta {
	filtered := make([]metadb.ChannelRuntimeMeta, 0, len(metas))
	for _, meta := range metas {
		if cluster.SlotForKey(meta.ChannelID) != groupID {
			continue
		}
		filtered = append(filtered, meta)
	}
	return filtered
}

func (s *Store) singleLocalPeerGroup(groupID multiraft.GroupID) bool {
	if s.cluster == nil {
		return false
	}
	peers := s.cluster.PeersForGroup(groupID)
	return len(peers) == 1 && s.cluster.IsLocal(peers[0])
}

func encodeRuntimeMetaRPCResponse(resp runtimeMetaRPCResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func decodeRuntimeMetaRPCResponse(body []byte) (runtimeMetaRPCResponse, error) {
	var resp runtimeMetaRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return runtimeMetaRPCResponse{}, err
	}
	return resp, nil
}
