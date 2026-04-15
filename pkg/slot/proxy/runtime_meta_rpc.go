package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const runtimeMetaRPCServiceID uint8 = 3

const (
	runtimeMetaRPCGet      = "get"
	runtimeMetaRPCBatchGet = "batch_get"
	runtimeMetaRPCList     = "list"
)

type runtimeMetaRPCRequest struct {
	Op          string                   `json:"op"`
	SlotID      uint64                   `json:"slot_id"`
	ChannelID   string                   `json:"channel_id,omitempty"`
	ChannelType int64                    `json:"channel_type,omitempty"`
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

func (s *Store) getChannelRuntimeMetaAuthoritative(ctx context.Context, slotID multiraft.SlotID, hashSlot uint16, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	if s.shouldServeSlotLocally(slotID) {
		return s.db.ForHashSlot(hashSlot).GetChannelRuntimeMeta(ctx, channelID, channelType)
	}

	resp, err := s.callRuntimeMetaRPC(ctx, slotID, runtimeMetaRPCRequest{
		Op:          runtimeMetaRPCGet,
		SlotID:      uint64(slotID),
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

func (s *Store) listChannelRuntimeMetaAuthoritative(ctx context.Context, slotID multiraft.SlotID) ([]metadb.ChannelRuntimeMeta, error) {
	if s.cluster == nil {
		return s.db.ListChannelRuntimeMeta(ctx)
	}
	if s.shouldServeSlotLocally(slotID) {
		metas, err := s.db.ListChannelRuntimeMeta(ctx)
		if err != nil {
			return nil, err
		}
		return filterChannelRuntimeMetaBySlot(s.cluster, slotID, metas), nil
	}

	resp, err := s.callRuntimeMetaRPC(ctx, slotID, runtimeMetaRPCRequest{
		Op:     runtimeMetaRPCList,
		SlotID: uint64(slotID),
	})
	if err != nil {
		// Managed slots are opened lazily. If a slot has not been bootstrapped
		// yet, treat it as currently having no runtime metadata and let the next
		// refresh pick it up once the controller brings the slot online.
		if errors.Is(err, raftcluster.ErrSlotNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return append([]metadb.ChannelRuntimeMeta(nil), resp.Metas...), nil
}

func (s *Store) BatchGetChannelRuntimeMetas(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error) {
	if len(keys) == 0 {
		return map[metadb.ConversationKey]metadb.ChannelRuntimeMeta{}, nil
	}

	grouped := make(map[multiraft.SlotID][]metadb.ConversationKey, len(keys))
	for _, key := range keys {
		slotID := s.cluster.SlotForKey(key.ChannelID)
		grouped[slotID] = append(grouped[slotID], key)
	}

	out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(keys))
	for slotID, groupKeys := range grouped {
		metasByKey, err := s.batchGetChannelRuntimeMetaAuthoritative(ctx, slotID, groupKeys)
		if err != nil {
			return nil, err
		}
		for key, meta := range metasByKey {
			out[key] = meta
		}
	}
	return out, nil
}

func (s *Store) callRuntimeMetaRPC(ctx context.Context, slotID multiraft.SlotID, req runtimeMetaRPCRequest) (runtimeMetaRPCResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return runtimeMetaRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, s, slotID, runtimeMetaRPCServiceID, payload, decodeRuntimeMetaRPCResponse)
}

func (s *Store) handleRuntimeMetaRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req runtimeMetaRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	slotID := multiraft.SlotID(req.SlotID)
	if statusBody, handled, err := s.handleAuthoritativeRPC(slotID, func(status string, leaderID uint64) ([]byte, error) {
		return encodeRuntimeMetaRPCResponse(runtimeMetaRPCResponse{
			Status:   status,
			LeaderID: leaderID,
		})
	}); handled || err != nil {
		return statusBody, err
	}

	switch req.Op {
	case runtimeMetaRPCGet:
		hashSlot := hashSlotForKey(s.cluster, req.ChannelID)
		meta, err := s.db.ForHashSlot(hashSlot).GetChannelRuntimeMeta(ctx, req.ChannelID, req.ChannelType)
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
			hashSlot := hashSlotForKey(s.cluster, key.ChannelID)
			meta, err := s.db.ForHashSlot(hashSlot).GetChannelRuntimeMeta(ctx, key.ChannelID, key.ChannelType)
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
			Metas:  filterChannelRuntimeMetaBySlot(s.cluster, slotID, metas),
		})
	default:
		return nil, fmt.Errorf("metastore: unknown runtime meta rpc op %q", req.Op)
	}
}

func (s *Store) batchGetChannelRuntimeMetaAuthoritative(ctx context.Context, slotID multiraft.SlotID, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error) {
	if s.shouldServeSlotLocally(slotID) {
		out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(keys))
		for _, key := range keys {
			hashSlot := hashSlotForKey(s.cluster, key.ChannelID)
			meta, err := s.db.ForHashSlot(hashSlot).GetChannelRuntimeMeta(ctx, key.ChannelID, key.ChannelType)
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

	resp, err := s.callRuntimeMetaRPC(ctx, slotID, runtimeMetaRPCRequest{
		Op:     runtimeMetaRPCBatchGet,
		SlotID: uint64(slotID),
		Keys:   keys,
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

func filterChannelRuntimeMetaBySlot(cluster *raftcluster.Cluster, slotID multiraft.SlotID, metas []metadb.ChannelRuntimeMeta) []metadb.ChannelRuntimeMeta {
	filtered := make([]metadb.ChannelRuntimeMeta, 0, len(metas))
	for _, meta := range metas {
		if cluster.SlotForKey(meta.ChannelID) != slotID {
			continue
		}
		filtered = append(filtered, meta)
	}
	return filtered
}

func (s *Store) singleLocalPeerSlot(slotID multiraft.SlotID) bool {
	if s.cluster == nil {
		return false
	}
	peers := s.cluster.PeersForSlot(slotID)
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
