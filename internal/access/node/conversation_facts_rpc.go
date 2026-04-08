package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

const (
	conversationFactsOpLatest = "latest"
	conversationFactsOpRecent = "recent"
)

type conversationFactsRequest struct {
	Op       string                  `json:"op"`
	Key      channellog.ChannelKey   `json:"key"`
	Keys     []channellog.ChannelKey `json:"keys,omitempty"`
	Limit    int                     `json:"limit,omitempty"`
	MaxBytes int                     `json:"max_bytes,omitempty"`
}

type conversationFactsEntry struct {
	Key      channellog.ChannelKey `json:"key"`
	Messages []channellog.Message  `json:"messages,omitempty"`
}

type conversationFactsResponse struct {
	Status   string                   `json:"status"`
	Messages []channellog.Message     `json:"messages,omitempty"`
	Entries  []conversationFactsEntry `json:"entries,omitempty"`
}

func (a *Adapter) handleConversationFactsRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req conversationFactsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	var (
		messages []channellog.Message
		entries  []conversationFactsEntry
		err      error
	)
	if len(req.Keys) > 0 {
		entries = make([]conversationFactsEntry, 0, len(req.Keys))
		for _, key := range req.Keys {
			entry := conversationFactsEntry{Key: key}
			switch req.Op {
			case conversationFactsOpLatest:
				var msg channellog.Message
				var ok bool
				msg, ok, err = loadLatestConversationMessage(ctx, a.channelLog, key, req.MaxBytes)
				if ok {
					entry.Messages = []channellog.Message{msg}
				}
			case conversationFactsOpRecent:
				entry.Messages, err = loadRecentConversationMessages(ctx, a.channelLog, key, req.Limit, req.MaxBytes)
			default:
				return nil, fmt.Errorf("access/node: unknown conversation facts op %q", req.Op)
			}
			if errors.Is(err, channellog.ErrChannelNotFound) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		return encodeConversationFactsResponse(conversationFactsResponse{
			Status:  rpcStatusOK,
			Entries: entries,
		})
	}

	switch req.Op {
	case conversationFactsOpLatest:
		var msg channellog.Message
		var ok bool
		msg, ok, err = loadLatestConversationMessage(ctx, a.channelLog, req.Key, req.MaxBytes)
		if ok {
			messages = []channellog.Message{msg}
		}
	case conversationFactsOpRecent:
		messages, err = loadRecentConversationMessages(ctx, a.channelLog, req.Key, req.Limit, req.MaxBytes)
	default:
		return nil, fmt.Errorf("access/node: unknown conversation facts op %q", req.Op)
	}
	if errors.Is(err, channellog.ErrChannelNotFound) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return encodeConversationFactsResponse(conversationFactsResponse{
		Status:   rpcStatusOK,
		Messages: messages,
	})
}

func loadLatestConversationMessage(ctx context.Context, cluster channellog.Cluster, key channellog.ChannelKey, maxBytes int) (channellog.Message, bool, error) {
	if cluster == nil {
		return channellog.Message{}, false, channellog.ErrStaleMeta
	}
	status, err := cluster.Status(key)
	if err != nil {
		return channellog.Message{}, false, err
	}
	if status.CommittedSeq == 0 {
		return channellog.Message{}, false, nil
	}

	fetch, err := cluster.Fetch(ctx, channellog.FetchRequest{
		Key:      key,
		FromSeq:  status.CommittedSeq,
		Limit:    1,
		MaxBytes: maxBytes,
	})
	if err != nil {
		return channellog.Message{}, false, err
	}
	if len(fetch.Messages) == 0 {
		return channellog.Message{}, false, nil
	}
	return fetch.Messages[0], true, nil
}

func loadRecentConversationMessages(ctx context.Context, cluster channellog.Cluster, key channellog.ChannelKey, limit, maxBytes int) ([]channellog.Message, error) {
	if cluster == nil || limit <= 0 {
		return nil, nil
	}
	status, err := cluster.Status(key)
	if err != nil {
		return nil, err
	}
	if status.CommittedSeq == 0 {
		return nil, nil
	}

	fromSeq := uint64(1)
	if status.CommittedSeq >= uint64(limit) {
		fromSeq = status.CommittedSeq - uint64(limit) + 1
	}
	fetch, err := cluster.Fetch(ctx, channellog.FetchRequest{
		Key:      key,
		FromSeq:  fromSeq,
		Limit:    limit,
		MaxBytes: maxBytes,
	})
	if err != nil {
		return nil, err
	}
	return append([]channellog.Message(nil), fetch.Messages...), nil
}

func encodeConversationFactsResponse(resp conversationFactsResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func decodeConversationFactsResponse(body []byte) (conversationFactsResponse, error) {
	var resp conversationFactsResponse
	err := json.Unmarshal(body, &resp)
	return resp, err
}
