package proxy

import (
	"context"
	"encoding/json"
	"fmt"

	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const channelUpdateLogRPCServiceID uint8 = 12

const (
	channelUpdateLogRPCBatchGet = "batch_get"
	channelUpdateLogRPCUpsert   = "upsert"
	channelUpdateLogRPCDelete   = "delete"
)

type channelUpdateLogRPCRequest struct {
	Op      string                    `json:"op"`
	GroupID uint64                    `json:"group_id"`
	Keys    []metadb.ConversationKey  `json:"keys,omitempty"`
	Entries []metadb.ChannelUpdateLog `json:"entries,omitempty"`
}

type channelUpdateLogRPCResponse struct {
	Status   string                    `json:"status"`
	LeaderID uint64                    `json:"leader_id,omitempty"`
	Entries  []metadb.ChannelUpdateLog `json:"entries,omitempty"`
}

func (r channelUpdateLogRPCResponse) rpcStatus() string {
	return r.Status
}

func (r channelUpdateLogRPCResponse) rpcLeaderID() uint64 {
	return r.LeaderID
}

func (s *Store) BatchGetChannelUpdateLogs(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	if len(keys) == 0 {
		return map[metadb.ConversationKey]metadb.ChannelUpdateLog{}, nil
	}

	grouped := make(map[multiraft.GroupID][]metadb.ConversationKey, len(keys))
	for _, key := range keys {
		groupID := s.cluster.SlotForKey(key.ChannelID)
		grouped[groupID] = append(grouped[groupID], key)
	}

	entries := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(keys))
	for groupID, groupKeys := range grouped {
		groupEntries, err := s.batchGetChannelUpdateLogsAuthoritative(ctx, groupID, groupKeys)
		if err != nil {
			return nil, err
		}
		for key, entry := range groupEntries {
			entries[key] = entry
		}
	}
	return entries, nil
}

func (s *Store) UpsertChannelUpdateLogs(ctx context.Context, entries []metadb.ChannelUpdateLog) error {
	if len(entries) == 0 {
		return nil
	}

	grouped := make(map[multiraft.GroupID][]metadb.ChannelUpdateLog, len(entries))
	for _, entry := range entries {
		groupID := s.cluster.SlotForKey(entry.ChannelID)
		grouped[groupID] = append(grouped[groupID], entry)
	}

	for groupID, groupEntries := range grouped {
		if s.shouldServeGroupLocally(groupID) {
			cmd := metafsm.EncodeUpsertChannelUpdateLogsCommand(groupEntries)
			if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
				return err
			}
			continue
		}
		if _, err := s.callChannelUpdateLogRPC(ctx, groupID, channelUpdateLogRPCRequest{
			Op:      channelUpdateLogRPCUpsert,
			GroupID: uint64(groupID),
			Entries: groupEntries,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteChannelUpdateLogs(ctx context.Context, keys []metadb.ConversationKey) error {
	if len(keys) == 0 {
		return nil
	}

	grouped := make(map[multiraft.GroupID][]metadb.ConversationKey, len(keys))
	for _, key := range keys {
		groupID := s.cluster.SlotForKey(key.ChannelID)
		grouped[groupID] = append(grouped[groupID], key)
	}

	for groupID, groupKeys := range grouped {
		if s.shouldServeGroupLocally(groupID) {
			cmd := metafsm.EncodeDeleteChannelUpdateLogsCommand(groupKeys)
			if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
				return err
			}
			continue
		}
		if _, err := s.callChannelUpdateLogRPC(ctx, groupID, channelUpdateLogRPCRequest{
			Op:      channelUpdateLogRPCDelete,
			GroupID: uint64(groupID),
			Keys:    groupKeys,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) batchGetChannelUpdateLogsAuthoritative(ctx context.Context, groupID multiraft.GroupID, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	if s.shouldServeGroupLocally(groupID) {
		return s.batchGetChannelUpdateLogsLocal(ctx, uint64(groupID), keys)
	}

	resp, err := s.callChannelUpdateLogRPC(ctx, groupID, channelUpdateLogRPCRequest{
		Op:      channelUpdateLogRPCBatchGet,
		GroupID: uint64(groupID),
		Keys:    keys,
	})
	if err != nil {
		return nil, err
	}

	entries := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(resp.Entries))
	for _, entry := range resp.Entries {
		key := metadb.ConversationKey{ChannelID: entry.ChannelID, ChannelType: entry.ChannelType}
		entries[key] = entry
	}
	return entries, nil
}

func (s *Store) callChannelUpdateLogRPC(ctx context.Context, groupID multiraft.GroupID, req channelUpdateLogRPCRequest) (channelUpdateLogRPCResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return channelUpdateLogRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, s, groupID, channelUpdateLogRPCServiceID, payload, decodeChannelUpdateLogRPCResponse)
}

func (s *Store) handleChannelUpdateLogRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req channelUpdateLogRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	groupID := multiraft.GroupID(req.GroupID)
	if statusBody, handled, err := s.handleAuthoritativeRPC(groupID, func(status string, leaderID uint64) ([]byte, error) {
		return encodeChannelUpdateLogRPCResponse(channelUpdateLogRPCResponse{
			Status:   status,
			LeaderID: leaderID,
		})
	}); handled || err != nil {
		return statusBody, err
	}

	switch req.Op {
	case channelUpdateLogRPCBatchGet:
		entriesByKey, err := s.batchGetChannelUpdateLogsLocal(ctx, uint64(groupID), req.Keys)
		if err != nil {
			return nil, err
		}
		entries := make([]metadb.ChannelUpdateLog, 0, len(entriesByKey))
		for _, entry := range entriesByKey {
			entries = append(entries, entry)
		}
		return encodeChannelUpdateLogRPCResponse(channelUpdateLogRPCResponse{
			Status:  rpcStatusOK,
			Entries: entries,
		})
	case channelUpdateLogRPCUpsert:
		cmd := metafsm.EncodeUpsertChannelUpdateLogsCommand(req.Entries)
		if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
			return nil, err
		}
		return encodeChannelUpdateLogRPCResponse(channelUpdateLogRPCResponse{Status: rpcStatusOK})
	case channelUpdateLogRPCDelete:
		cmd := metafsm.EncodeDeleteChannelUpdateLogsCommand(req.Keys)
		if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
			return nil, err
		}
		return encodeChannelUpdateLogRPCResponse(channelUpdateLogRPCResponse{Status: rpcStatusOK})
	default:
		return nil, fmt.Errorf("metastore: unknown channel update log rpc op %q", req.Op)
	}
}

func encodeChannelUpdateLogRPCResponse(resp channelUpdateLogRPCResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func decodeChannelUpdateLogRPCResponse(body []byte) (channelUpdateLogRPCResponse, error) {
	var resp channelUpdateLogRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return channelUpdateLogRPCResponse{}, err
	}
	return resp, nil
}

func (s *Store) batchGetChannelUpdateLogsLocal(ctx context.Context, slot uint64, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	entries := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(keys))
	missing := make([]metadb.ConversationKey, 0, len(keys))

	if s.channelUpdateOverlay != nil {
		hotEntries, err := s.channelUpdateOverlay.BatchGetHotChannelUpdates(ctx, keys)
		if err == nil {
			for key, entry := range hotEntries {
				entries[key] = entry
			}
		}
	}

	for _, key := range keys {
		if _, ok := entries[key]; ok {
			continue
		}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return entries, nil
	}

	coldEntries, err := s.db.ForSlot(slot).BatchGetChannelUpdateLogs(ctx, missing)
	if err != nil {
		return nil, err
	}
	for key, entry := range coldEntries {
		entries[key] = entry
	}
	return entries, nil
}
