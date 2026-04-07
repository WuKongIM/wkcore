package metastore

import (
	"context"
	"encoding/json"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

const subscriberRPCServiceID uint8 = 10

type subscriberRPCRequest struct {
	GroupID     uint64 `json:"group_id"`
	ChannelID   string `json:"channel_id"`
	ChannelType int64  `json:"channel_type"`
	Snapshot    bool   `json:"snapshot,omitempty"`
	AfterUID    string `json:"after_uid,omitempty"`
	Limit       int    `json:"limit"`
}

type subscriberRPCResponse struct {
	Status     string   `json:"status"`
	LeaderID   uint64   `json:"leader_id,omitempty"`
	UIDs       []string `json:"uids,omitempty"`
	NextCursor string   `json:"next_cursor,omitempty"`
	Done       bool     `json:"done"`
}

func (r subscriberRPCResponse) rpcStatus() string {
	return r.Status
}

func (r subscriberRPCResponse) rpcLeaderID() uint64 {
	return r.LeaderID
}

func (s *Store) listChannelSubscribersAuthoritative(ctx context.Context, groupID multiraft.GroupID, channelID string, channelType int64, afterUID string, limit int) ([]string, string, bool, error) {
	if s.shouldServeGroupLocally(groupID) {
		return s.db.ForSlot(uint64(groupID)).ListSubscribersPage(ctx, channelID, channelType, afterUID, limit)
	}

	resp, err := s.callSubscriberRPC(ctx, groupID, subscriberRPCRequest{
		GroupID:     uint64(groupID),
		ChannelID:   channelID,
		ChannelType: channelType,
		AfterUID:    afterUID,
		Limit:       limit,
	})
	if err != nil {
		return nil, "", false, err
	}
	return append([]string(nil), resp.UIDs...), resp.NextCursor, resp.Done, nil
}

func (s *Store) SnapshotChannelSubscribers(ctx context.Context, channelID string, channelType int64) ([]string, error) {
	groupID := s.cluster.SlotForKey(channelID)
	if s.shouldServeGroupLocally(groupID) {
		return s.db.ForSlot(uint64(groupID)).ListSubscribersSnapshot(ctx, channelID, channelType)
	}

	resp, err := s.callSubscriberRPC(ctx, groupID, subscriberRPCRequest{
		GroupID:     uint64(groupID),
		ChannelID:   channelID,
		ChannelType: channelType,
		Snapshot:    true,
	})
	if err != nil {
		return nil, err
	}
	return append([]string(nil), resp.UIDs...), nil
}

func (s *Store) callSubscriberRPC(ctx context.Context, groupID multiraft.GroupID, req subscriberRPCRequest) (subscriberRPCResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return subscriberRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, s, groupID, subscriberRPCServiceID, payload, decodeSubscriberRPCResponse)
}

func (s *Store) handleSubscriberRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req subscriberRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	groupID := multiraft.GroupID(req.GroupID)
	if statusBody, handled, err := s.handleAuthoritativeRPC(groupID, func(status string, leaderID uint64) ([]byte, error) {
		return encodeSubscriberRPCResponse(subscriberRPCResponse{
			Status:   status,
			LeaderID: leaderID,
		})
	}); handled || err != nil {
		return statusBody, err
	}

	if req.Snapshot {
		uids, err := s.db.ForSlot(uint64(groupID)).ListSubscribersSnapshot(ctx, req.ChannelID, req.ChannelType)
		if err != nil {
			return nil, err
		}
		return encodeSubscriberRPCResponse(subscriberRPCResponse{
			Status: rpcStatusOK,
			UIDs:   uids,
			Done:   true,
		})
	}

	uids, nextCursor, done, err := s.db.ForSlot(uint64(groupID)).ListSubscribersPage(ctx, req.ChannelID, req.ChannelType, req.AfterUID, req.Limit)
	if err != nil {
		return nil, err
	}
	return encodeSubscriberRPCResponse(subscriberRPCResponse{
		Status:     rpcStatusOK,
		UIDs:       uids,
		NextCursor: nextCursor,
		Done:       done,
	})
}

func encodeSubscriberRPCResponse(resp subscriberRPCResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func decodeSubscriberRPCResponse(body []byte) (subscriberRPCResponse, error) {
	var resp subscriberRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return subscriberRPCResponse{}, err
	}
	return resp, nil
}
