package metastore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
)

const userConversationStateRPCServiceID uint8 = 5

const (
	userConversationStateRPCGet      = "get"
	userConversationStateRPCList     = "list_active"
	userConversationStateRPCScanPage = "scan_page"
	userConversationStateRPCTouch    = "touch_active"
	userConversationStateRPCClear    = "clear_active"
)

type userConversationStateRPCRequest struct {
	Op          string                               `json:"op"`
	GroupID     uint64                               `json:"group_id"`
	UID         string                               `json:"uid,omitempty"`
	ChannelID   string                               `json:"channel_id,omitempty"`
	ChannelType int64                                `json:"channel_type,omitempty"`
	After       *metadb.ConversationCursor           `json:"after,omitempty"`
	Limit       int                                  `json:"limit,omitempty"`
	Patches     []metadb.UserConversationActivePatch `json:"patches,omitempty"`
	Keys        []metadb.ConversationKey             `json:"keys,omitempty"`
}

type userConversationStateRPCResponse struct {
	Status   string                         `json:"status"`
	LeaderID uint64                         `json:"leader_id,omitempty"`
	State    *metadb.UserConversationState  `json:"state,omitempty"`
	States   []metadb.UserConversationState `json:"states,omitempty"`
	Cursor   metadb.ConversationCursor      `json:"cursor,omitempty"`
	Done     bool                           `json:"done,omitempty"`
}

func (r userConversationStateRPCResponse) rpcStatus() string {
	return r.Status
}

func (r userConversationStateRPCResponse) rpcLeaderID() uint64 {
	return r.LeaderID
}

func (s *Store) GetUserConversationState(ctx context.Context, uid, channelID string, channelType int64) (metadb.UserConversationState, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.getUserConversationStateAuthoritative(ctx, groupID, uid, channelID, channelType)
}

func (s *Store) ListUserConversationActive(ctx context.Context, uid string, limit int) ([]metadb.UserConversationState, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.listUserConversationActiveAuthoritative(ctx, groupID, uid, limit)
}

func (s *Store) ScanUserConversationStatePage(ctx context.Context, uid string, after metadb.ConversationCursor, limit int) ([]metadb.UserConversationState, metadb.ConversationCursor, bool, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.scanUserConversationStatePageAuthoritative(ctx, groupID, uid, after, limit)
}

func (s *Store) TouchUserConversationActiveAt(ctx context.Context, patches []metadb.UserConversationActivePatch) error {
	if len(patches) == 0 {
		return nil
	}

	grouped, err := s.groupUserConversationActivePatchesByGroup(patches)
	if err != nil {
		return err
	}
	for groupID, groupPatches := range grouped {
		if s.shouldServeGroupLocally(groupID) {
			cmd := metafsm.EncodeTouchUserConversationActiveAtCommand(groupPatches)
			if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
				return err
			}
			continue
		}
		if _, err := s.callUserConversationStateRPC(ctx, groupID, userConversationStateRPCRequest{
			Op:      userConversationStateRPCTouch,
			GroupID: uint64(groupID),
			Patches: groupPatches,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ClearUserConversationActiveAt(ctx context.Context, uid string, keys []metadb.ConversationKey) error {
	if len(keys) == 0 {
		return nil
	}

	groupID := s.cluster.SlotForKey(uid)
	if s.shouldServeGroupLocally(groupID) {
		cmd := metafsm.EncodeClearUserConversationActiveAtCommand(uid, keys)
		return s.cluster.Propose(ctx, groupID, cmd)
	}

	_, err := s.callUserConversationStateRPC(ctx, groupID, userConversationStateRPCRequest{
		Op:      userConversationStateRPCClear,
		GroupID: uint64(groupID),
		UID:     uid,
		Keys:    keys,
	})
	return err
}

func (s *Store) getUserConversationStateAuthoritative(ctx context.Context, groupID multiraft.GroupID, uid, channelID string, channelType int64) (metadb.UserConversationState, error) {
	if s.shouldServeGroupLocally(groupID) {
		return s.db.ForSlot(uint64(groupID)).GetUserConversationState(ctx, uid, channelID, channelType)
	}

	resp, err := s.callUserConversationStateRPC(ctx, groupID, userConversationStateRPCRequest{
		Op:          userConversationStateRPCGet,
		GroupID:     uint64(groupID),
		UID:         uid,
		ChannelID:   channelID,
		ChannelType: channelType,
	})
	if err != nil {
		return metadb.UserConversationState{}, err
	}
	if resp.State == nil {
		return metadb.UserConversationState{}, metadb.ErrNotFound
	}
	return *resp.State, nil
}

func (s *Store) listUserConversationActiveAuthoritative(ctx context.Context, groupID multiraft.GroupID, uid string, limit int) ([]metadb.UserConversationState, error) {
	if s.shouldServeGroupLocally(groupID) {
		return s.db.ForSlot(uint64(groupID)).ListUserConversationActive(ctx, uid, limit)
	}

	resp, err := s.callUserConversationStateRPC(ctx, groupID, userConversationStateRPCRequest{
		Op:      userConversationStateRPCList,
		GroupID: uint64(groupID),
		UID:     uid,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	return append([]metadb.UserConversationState(nil), resp.States...), nil
}

func (s *Store) scanUserConversationStatePageAuthoritative(ctx context.Context, groupID multiraft.GroupID, uid string, after metadb.ConversationCursor, limit int) ([]metadb.UserConversationState, metadb.ConversationCursor, bool, error) {
	if s.shouldServeGroupLocally(groupID) {
		return s.db.ForSlot(uint64(groupID)).ListUserConversationStatePage(ctx, uid, after, limit)
	}

	resp, err := s.callUserConversationStateRPC(ctx, groupID, userConversationStateRPCRequest{
		Op:      userConversationStateRPCScanPage,
		GroupID: uint64(groupID),
		UID:     uid,
		After:   &after,
		Limit:   limit,
	})
	if err != nil {
		return nil, metadb.ConversationCursor{}, false, err
	}
	return append([]metadb.UserConversationState(nil), resp.States...), resp.Cursor, resp.Done, nil
}

func (s *Store) callUserConversationStateRPC(ctx context.Context, groupID multiraft.GroupID, req userConversationStateRPCRequest) (userConversationStateRPCResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return userConversationStateRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, s, groupID, userConversationStateRPCServiceID, payload, decodeUserConversationStateRPCResponse)
}

func (s *Store) handleUserConversationStateRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req userConversationStateRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	groupID := multiraft.GroupID(req.GroupID)
	if statusBody, handled, err := s.handleAuthoritativeRPC(groupID, func(status string, leaderID uint64) ([]byte, error) {
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{
			Status:   status,
			LeaderID: leaderID,
		})
	}); handled || err != nil {
		return statusBody, err
	}

	shard := s.db.ForSlot(uint64(groupID))
	switch req.Op {
	case userConversationStateRPCGet:
		state, err := shard.GetUserConversationState(ctx, req.UID, req.ChannelID, req.ChannelType)
		if err == metadb.ErrNotFound {
			return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{Status: rpcStatusNotFound})
		}
		if err != nil {
			return nil, err
		}
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{
			Status: rpcStatusOK,
			State:  &state,
		})
	case userConversationStateRPCList:
		states, err := shard.ListUserConversationActive(ctx, req.UID, req.Limit)
		if err != nil {
			return nil, err
		}
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{
			Status: rpcStatusOK,
			States: states,
		})
	case userConversationStateRPCScanPage:
		var after metadb.ConversationCursor
		if req.After != nil {
			after = *req.After
		}
		states, cursor, done, err := shard.ListUserConversationStatePage(ctx, req.UID, after, req.Limit)
		if err != nil {
			return nil, err
		}
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{
			Status: rpcStatusOK,
			States: states,
			Cursor: cursor,
			Done:   done,
		})
	case userConversationStateRPCTouch:
		cmd := metafsm.EncodeTouchUserConversationActiveAtCommand(req.Patches)
		if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
			return nil, err
		}
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{Status: rpcStatusOK})
	case userConversationStateRPCClear:
		cmd := metafsm.EncodeClearUserConversationActiveAtCommand(req.UID, req.Keys)
		if err := s.cluster.Propose(ctx, groupID, cmd); err != nil {
			return nil, err
		}
		return encodeUserConversationStateRPCResponse(userConversationStateRPCResponse{Status: rpcStatusOK})
	default:
		return nil, fmt.Errorf("metastore: unknown user conversation state rpc op %q", req.Op)
	}
}

func encodeUserConversationStateRPCResponse(resp userConversationStateRPCResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func decodeUserConversationStateRPCResponse(body []byte) (userConversationStateRPCResponse, error) {
	var resp userConversationStateRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return userConversationStateRPCResponse{}, err
	}
	return resp, nil
}

func (s *Store) groupUserConversationActivePatchesByGroup(patches []metadb.UserConversationActivePatch) (map[multiraft.GroupID][]metadb.UserConversationActivePatch, error) {
	grouped := make(map[multiraft.GroupID][]metadb.UserConversationActivePatch, len(patches))
	for _, patch := range patches {
		if patch.UID == "" {
			return nil, fmt.Errorf("metastore: empty uid in touch patch")
		}
		groupID := s.cluster.SlotForKey(patch.UID)
		grouped[groupID] = append(grouped[groupID], patch)
	}
	return grouped, nil
}
