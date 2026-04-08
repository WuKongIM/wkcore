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
	runtimeMetaRPCGet      = "get"
	runtimeMetaRPCBatchGet = "batch_get"
	runtimeMetaRPCList     = "list"
)

type runtimeMetaRPCRequest struct {
	Op          string `json:"op"`
	GroupID     uint64 `json:"group_id"`
	ChannelID   string `json:"channel_id,omitempty"`
	ChannelType int64  `json:"channel_type,omitempty"`
	Keys        []metadb.ConversationKey `json:"keys,omitempty"`
}

type runtimeMetaRPCResponse struct {
	Status   string                      `json:"status"`
	LeaderID uint64                      `json:"leader_id,omitempty"`
	Meta     *metadb.ChannelRuntimeMeta  `json:"meta,omitempty"`
	Metas    []metadb.ChannelRuntimeMeta `json:"metas,omitempty"`
}

func (r runtimeMetaRPCResponse) rpcStatus() string {
	return r.Status
}

func (r runtimeMetaRPCResponse) rpcLeaderID() uint64 {
	return r.LeaderID
}

func (s *Store) getChannelRuntimeMetaAuthoritative(ctx context.Context, groupID multiraft.GroupID, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	if s.shouldServeGroupLocally(groupID) {
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
	if s.shouldServeGroupLocally(groupID) {
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

func (s *Store) BatchGetChannelRuntimeMetas(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error) {
	if len(keys) == 0 {
		return map[metadb.ConversationKey]metadb.ChannelRuntimeMeta{}, nil
	}

	grouped := make(map[multiraft.GroupID][]metadb.ConversationKey, len(keys))
	for _, key := range keys {
		groupID := s.cluster.SlotForKey(key.ChannelID)
		grouped[groupID] = append(grouped[groupID], key)
	}

	out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(keys))
	for groupID, groupKeys := range grouped {
		metasByKey, err := s.batchGetChannelRuntimeMetaAuthoritative(ctx, groupID, groupKeys)
		if err != nil {
			return nil, err
		}
		for key, meta := range metasByKey {
			out[key] = meta
		}
	}
	return out, nil
}

func (s *Store) callRuntimeMetaRPC(ctx context.Context, groupID multiraft.GroupID, req runtimeMetaRPCRequest) (runtimeMetaRPCResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return runtimeMetaRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, s, groupID, runtimeMetaRPCServiceID, payload, decodeRuntimeMetaRPCResponse)
}

func (s *Store) handleRuntimeMetaRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req runtimeMetaRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	groupID := multiraft.GroupID(req.GroupID)
	if statusBody, handled, err := s.handleAuthoritativeRPC(groupID, func(status string, leaderID uint64) ([]byte, error) {
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status:   status,
			LeaderID: leaderID,
		})
	}); handled || err != nil {
		return statusBody, err
	}

	switch req.Op {
	case runtimeMetaRPCGet:
		meta, err := s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, req.ChannelID, req.ChannelType)
		if errors.Is(err, metadb.ErrNotFound) {
			return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{Status: rpcStatusNotFound})
		}
		if err != nil {
			return nil, err
		}
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status: rpcStatusOK,
			Meta:   &meta,
		})
	case runtimeMetaRPCBatchGet:
		out := make([]metadb.ChannelRuntimeMeta, 0, len(req.Keys))
		for _, key := range req.Keys {
			meta, err := s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, key.ChannelID, key.ChannelType)
			if errors.Is(err, metadb.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			out = append(out, meta)
		}
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status: rpcStatusOK,
			Metas:  out,
		})
	case runtimeMetaRPCList:
		metas, err := s.db.ListChannelRuntimeMeta(ctx)
		if err != nil {
			return nil, err
		}
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status: rpcStatusOK,
			Metas:  filterChannelRuntimeMetaByGroup(s.cluster, groupID, metas),
		})
	default:
		return nil, fmt.Errorf("metastore: unknown runtime meta rpc op %q", req.Op)
	}
}

func (s *Store) batchGetChannelRuntimeMetaAuthoritative(ctx context.Context, groupID multiraft.GroupID, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error) {
	if s.shouldServeGroupLocally(groupID) {
		out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(keys))
		for _, key := range keys {
			meta, err := s.db.ForSlot(uint64(groupID)).GetChannelRuntimeMeta(ctx, key.ChannelID, key.ChannelType)
			if errors.Is(err, metadb.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			out[key] = meta
		}
		return out, nil
	}

	resp, err := s.callRuntimeMetaRPC(ctx, groupID, runtimeMetaRPCRequest{
		Op:      runtimeMetaRPCBatchGet,
		GroupID: uint64(groupID),
		Keys:    keys,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(resp.Metas))
	for _, meta := range resp.Metas {
		out[metadb.ConversationKey{ChannelID: meta.ChannelID, ChannelType: meta.ChannelType}] = meta
	}
	return out, nil
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
