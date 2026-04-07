package conversation

import (
	"context"
	"sort"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

type syncCandidate struct {
	key           ConversationKey
	state         metadb.UserConversationState
	hasState      bool
	update        *metadb.ChannelUpdateLog
	clientLastSeq uint64
	overlay       bool
}

type syncConversationView struct {
	key              ConversationKey
	state            metadb.UserConversationState
	displayUpdatedAt int64
	conversation     SyncConversation
}

func (a *App) Sync(ctx context.Context, query SyncQuery) (SyncResult, error) {
	if a == nil {
		return SyncResult{}, nil
	}
	if query.Limit <= 0 {
		return SyncResult{}, nil
	}

	candidates := make(map[ConversationKey]*syncCandidate)

	active, err := a.states.ListUserConversationActive(ctx, query.UID, a.activeScanLimit)
	if err != nil {
		return SyncResult{}, err
	}
	if err := a.addActiveCandidates(ctx, query.UID, active, candidates); err != nil {
		return SyncResult{}, err
	}

	if err := a.addOverlayCandidates(ctx, query, candidates); err != nil {
		return SyncResult{}, err
	}
	if query.Version > 0 {
		if err := a.addIncrementalCandidates(ctx, query, candidates); err != nil {
			return SyncResult{}, err
		}
	}

	keys := filterCandidateKeys(candidates, query.ExcludeChannelTypes)
	latestByKey, err := a.facts.LoadLatestMessages(ctx, keys)
	if err != nil {
		return SyncResult{}, err
	}

	views := make([]syncConversationView, 0, len(keys))
	for _, key := range keys {
		latest, ok := latestByKey[key]
		if !ok || latest.MessageSeq == 0 {
			continue
		}

		candidate := candidates[key]
		if candidate.overlay && candidate.clientLastSeq >= latest.MessageSeq {
			continue
		}

		baseReadedTo := maxUint64(candidate.state.ReadSeq, candidate.state.DeletedToSeq)
		if latest.MessageSeq <= candidate.state.DeletedToSeq {
			continue
		}

		unread := 0
		if latest.MessageSeq > baseReadedTo {
			unread = int(latest.MessageSeq - baseReadedTo)
		}
		readedTo := baseReadedTo
		if latest.FromUID == query.UID {
			unread = 0
			readedTo = latest.MessageSeq
		}
		if query.OnlyUnread && unread == 0 {
			continue
		}

		displayUpdatedAt := time.Unix(int64(latest.Timestamp), 0).UnixNano()
		if candidate.update != nil && candidate.update.UpdatedAt > 0 {
			displayUpdatedAt = candidate.update.UpdatedAt
		}
		syncUpdatedAt := displayUpdatedAt
		if candidate.state.UpdatedAt > syncUpdatedAt {
			syncUpdatedAt = candidate.state.UpdatedAt
		}

		views = append(views, syncConversationView{
			key:              key,
			state:            candidate.state,
			displayUpdatedAt: displayUpdatedAt,
			conversation: SyncConversation{
				ChannelID:       displayChannelID(query.UID, key),
				ChannelType:     key.ChannelType,
				Unread:          unread,
				Timestamp:       int64(latest.Timestamp),
				LastMsgSeq:      uint32(latest.MessageSeq),
				LastClientMsgNo: latest.ClientMsgNo,
				ReadedToMsgSeq:  uint32(readedTo),
				Version:         syncUpdatedAt,
			},
		})
	}

	sort.Slice(views, func(i, j int) bool {
		if views[i].displayUpdatedAt != views[j].displayUpdatedAt {
			return views[i].displayUpdatedAt > views[j].displayUpdatedAt
		}
		if views[i].key.ChannelType != views[j].key.ChannelType {
			return views[i].key.ChannelType < views[j].key.ChannelType
		}
		return views[i].key.ChannelID < views[j].key.ChannelID
	})

	if len(views) > query.Limit {
		views = views[:query.Limit]
	}

	if query.MsgCount > 0 {
		for i := range views {
			recents, err := a.facts.LoadRecentMessages(ctx, views[i].key, query.MsgCount)
			if err != nil {
				return SyncResult{}, err
			}
			recents = filterVisibleRecents(recents, views[i].state.DeletedToSeq)
			sort.Slice(recents, func(left, right int) bool {
				return recents[left].MessageSeq > recents[right].MessageSeq
			})
			views[i].conversation.Recents = recents
		}
	}

	result := SyncResult{Conversations: make([]SyncConversation, 0, len(views))}
	for _, view := range views {
		result.Conversations = append(result.Conversations, view.conversation)
	}
	return result, nil
}

func (a *App) addActiveCandidates(ctx context.Context, uid string, states []metadb.UserConversationState, candidates map[ConversationKey]*syncCandidate) error {
	if len(states) == 0 {
		return nil
	}

	keys := make([]metadb.ConversationKey, 0, len(states))
	for _, state := range states {
		keys = append(keys, metadbConversationKey(state.ChannelID, uint8(state.ChannelType)))
	}
	updates, err := a.channelUpdate.BatchGetChannelUpdateLogs(ctx, keys)
	if err != nil {
		return err
	}

	coldKeys := make([]metadb.ConversationKey, 0, len(states))
	for _, state := range states {
		key := conversationKey(state.ChannelID, uint8(state.ChannelType))
		update, ok := updates[metadbConversationKey(state.ChannelID, uint8(state.ChannelType))]
		if ok && a.isCold(update.LastMsgAt) {
			coldKeys = append(coldKeys, metadbConversationKey(state.ChannelID, uint8(state.ChannelType)))
			continue
		}

		candidate := ensureCandidate(candidates, key)
		candidate.state = state
		candidate.hasState = true
		if ok {
			updateCopy := update
			candidate.update = &updateCopy
		}
	}

	if len(coldKeys) > 0 {
		keysCopy := append([]metadb.ConversationKey(nil), coldKeys...)
		a.async(func() {
			_ = a.states.ClearUserConversationActiveAt(context.Background(), uid, keysCopy)
		})
	}
	return nil
}

func (a *App) addOverlayCandidates(ctx context.Context, query SyncQuery, candidates map[ConversationKey]*syncCandidate) error {
	for key, lastSeq := range query.LastMsgSeqs {
		candidate := ensureCandidate(candidates, key)
		candidate.clientLastSeq = lastSeq
		if candidate.hasState {
			candidate.overlay = false
			continue
		}

		state, err := a.states.GetUserConversationState(ctx, query.UID, key.ChannelID, int64(key.ChannelType))
		switch err {
		case nil:
			candidate.state = state
			candidate.hasState = true
			candidate.overlay = false
		case metadb.ErrNotFound:
			candidate.state = metadb.UserConversationState{
				UID:         query.UID,
				ChannelID:   key.ChannelID,
				ChannelType: int64(key.ChannelType),
			}
			candidate.overlay = true
		default:
			return err
		}
	}
	return nil
}

func (a *App) addIncrementalCandidates(ctx context.Context, query SyncQuery, candidates map[ConversationKey]*syncCandidate) error {
	var after metadb.ConversationCursor
	for {
		page, next, done, err := a.states.ScanUserConversationStatePage(ctx, query.UID, after, a.channelProbeBatchSize)
		if err != nil {
			return err
		}
		if len(page) > 0 {
			keys := make([]metadb.ConversationKey, 0, len(page))
			for _, state := range page {
				keys = append(keys, metadbConversationKey(state.ChannelID, uint8(state.ChannelType)))
			}
			updates, err := a.channelUpdate.BatchGetChannelUpdateLogs(ctx, keys)
			if err != nil {
				return err
			}

			for _, state := range page {
				key := conversationKey(state.ChannelID, uint8(state.ChannelType))
				update, ok := updates[metadbConversationKey(state.ChannelID, uint8(state.ChannelType))]
				if state.UpdatedAt > query.Version {
					candidate := ensureCandidate(candidates, key)
					candidate.state = state
					candidate.hasState = true
				}
				if ok && update.UpdatedAt > query.Version && !a.isCold(update.LastMsgAt) {
					candidate := ensureCandidate(candidates, key)
					candidate.state = state
					candidate.hasState = true
					updateCopy := update
					candidate.update = &updateCopy
				}
			}
		}

		if done {
			return nil
		}
		after = next
	}
}

func (a *App) isCold(lastMsgAt int64) bool {
	if lastMsgAt <= 0 {
		return false
	}
	return lastMsgAt <= a.now().Add(-a.coldThreshold).UnixNano()
}

func ensureCandidate(candidates map[ConversationKey]*syncCandidate, key ConversationKey) *syncCandidate {
	candidate, ok := candidates[key]
	if ok {
		return candidate
	}
	candidate = &syncCandidate{key: key}
	candidates[key] = candidate
	return candidate
}

func filterCandidateKeys(candidates map[ConversationKey]*syncCandidate, excluded []uint8) []ConversationKey {
	if len(candidates) == 0 {
		return nil
	}
	blocked := make(map[uint8]struct{}, len(excluded))
	for _, channelType := range excluded {
		blocked[channelType] = struct{}{}
	}
	keys := make([]ConversationKey, 0, len(candidates))
	for key := range candidates {
		if _, ok := blocked[key.ChannelType]; ok {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func filterVisibleRecents(recents []channellog.Message, deletedToSeq uint64) []channellog.Message {
	if deletedToSeq == 0 {
		return append([]channellog.Message(nil), recents...)
	}
	out := make([]channellog.Message, 0, len(recents))
	for _, msg := range recents {
		if msg.MessageSeq <= deletedToSeq {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func conversationKey(channelID string, channelType uint8) ConversationKey {
	return ConversationKey{
		ChannelID:   channelID,
		ChannelType: channelType,
	}
}

func metadbConversationKey(channelID string, channelType uint8) metadb.ConversationKey {
	return metadb.ConversationKey{
		ChannelID:   channelID,
		ChannelType: int64(channelType),
	}
}

func displayChannelID(uid string, key ConversationKey) string {
	if key.ChannelType != wkframe.ChannelTypePerson {
		return key.ChannelID
	}
	left, right, err := runtimechannelid.DecodePersonChannel(key.ChannelID)
	if err != nil {
		return key.ChannelID
	}
	if left == uid {
		return right
	}
	if right == uid {
		return left
	}
	return key.ChannelID
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
