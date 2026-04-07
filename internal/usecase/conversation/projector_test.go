package conversation

import (
	"context"
	"sort"
	"testing"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

func TestProjectorCoalescesMultipleMessagesIntoSingleFlushEntry(t *testing.T) {
	store := newProjectorStoreStub()
	projector := NewProjector(ProjectorOptions{
		Store:         store,
		FlushInterval: time.Hour,
		DirtyLimit:    8,
		ColdThreshold: 30 * 24 * time.Hour,
		Now:           func() time.Time { return time.Unix(100, 0) },
		Async:         func(fn func()) { fn() },
	})

	msg1 := channellog.Message{ChannelID: "g1", ChannelType: 2, MessageSeq: 10, ClientMsgNo: "c1", Timestamp: 100}
	msg2 := channellog.Message{ChannelID: "g1", ChannelType: 2, MessageSeq: 11, ClientMsgNo: "c2", Timestamp: 101}
	require.NoError(t, projector.SubmitCommitted(context.Background(), msg1))
	require.NoError(t, projector.SubmitCommitted(context.Background(), msg2))
	require.NoError(t, projector.Flush(context.Background()))

	require.Len(t, store.updates, 1)
	require.Equal(t, metadb.ChannelUpdateLog{
		ChannelID:       "g1",
		ChannelType:     2,
		UpdatedAt:       time.Unix(101, 0).UnixNano(),
		LastMsgSeq:      11,
		LastClientMsgNo: "c2",
		LastMsgAt:       time.Unix(101, 0).UnixNano(),
	}, store.updates[0])
}

func TestProjectorColdWakeupTouchesPersonConversationStates(t *testing.T) {
	now := time.Unix(40*24*60*60, 0)
	store := newProjectorStoreStub()
	channelID := runtimechannelid.EncodePersonChannel("u1", "u2")
	store.cold[metadb.ConversationKey{ChannelID: channelID, ChannelType: int64(wkframe.ChannelTypePerson)}] = metadb.ChannelUpdateLog{
		ChannelID:   channelID,
		ChannelType: int64(wkframe.ChannelTypePerson),
		LastMsgAt:   now.Add(-31 * 24 * time.Hour).UnixNano(),
	}

	projector := NewProjector(ProjectorOptions{
		Store:         store,
		FlushInterval: time.Hour,
		DirtyLimit:    8,
		ColdThreshold: 30 * 24 * time.Hour,
		Now:           func() time.Time { return now },
		Async:         func(fn func()) { fn() },
	})

	msg := channellog.Message{
		ChannelID:   channelID,
		ChannelType: wkframe.ChannelTypePerson,
		MessageSeq:  10,
		ClientMsgNo: "c1",
		Timestamp:   int32(now.Unix()),
	}
	require.NoError(t, projector.SubmitCommitted(context.Background(), msg))
	require.ElementsMatch(t, []metadb.UserConversationActivePatch{
		{UID: "u1", ChannelID: channelID, ChannelType: int64(wkframe.ChannelTypePerson), ActiveAt: time.Unix(int64(msg.Timestamp), 0).UnixNano()},
		{UID: "u2", ChannelID: channelID, ChannelType: int64(wkframe.ChannelTypePerson), ActiveAt: time.Unix(int64(msg.Timestamp), 0).UnixNano()},
	}, store.touches)

	require.NoError(t, projector.SubmitCommitted(context.Background(), channellog.Message{
		ChannelID:   channelID,
		ChannelType: wkframe.ChannelTypePerson,
		MessageSeq:  11,
		ClientMsgNo: "c2",
		Timestamp:   int32(now.Add(time.Second).Unix()),
	}))
	require.Len(t, store.touches, 2)
}

func TestProjectorColdWakeupPagesGroupSubscribers(t *testing.T) {
	now := time.Unix(40*24*60*60, 0)
	store := newProjectorStoreStub()
	key := metadb.ConversationKey{ChannelID: "g1", ChannelType: 2}
	store.cold[key] = metadb.ChannelUpdateLog{
		ChannelID:   "g1",
		ChannelType: 2,
		LastMsgAt:   now.Add(-31 * 24 * time.Hour).UnixNano(),
	}
	store.subscribers[key] = []string{"u1", "u2", "u3"}

	projector := NewProjector(ProjectorOptions{
		Store:              store,
		FlushInterval:      time.Hour,
		DirtyLimit:         8,
		ColdThreshold:      30 * 24 * time.Hour,
		SubscriberPageSize: 2,
		Now:                func() time.Time { return now },
		Async:              func(fn func()) { fn() },
	})

	msg := channellog.Message{
		ChannelID:   "g1",
		ChannelType: 2,
		MessageSeq:  10,
		ClientMsgNo: "c1",
		Timestamp:   int32(now.Unix()),
	}
	require.NoError(t, projector.SubmitCommitted(context.Background(), msg))
	require.Equal(t, []subscriberListCall{
		{ChannelID: "g1", ChannelType: 2, AfterUID: "", Limit: 2},
		{ChannelID: "g1", ChannelType: 2, AfterUID: "u2", Limit: 2},
	}, store.subscriberCalls)
	require.ElementsMatch(t, []metadb.UserConversationActivePatch{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ActiveAt: time.Unix(int64(msg.Timestamp), 0).UnixNano()},
		{UID: "u2", ChannelID: "g1", ChannelType: 2, ActiveAt: time.Unix(int64(msg.Timestamp), 0).UnixNano()},
		{UID: "u3", ChannelID: "g1", ChannelType: 2, ActiveAt: time.Unix(int64(msg.Timestamp), 0).UnixNano()},
	}, store.touches)
}

func TestProjectorOverlayReadWinsBeforeColdFlush(t *testing.T) {
	store := newProjectorStoreStub()
	key := metadb.ConversationKey{ChannelID: "g1", ChannelType: 2}
	store.cold[key] = metadb.ChannelUpdateLog{
		ChannelID:       "g1",
		ChannelType:     2,
		UpdatedAt:       time.Unix(90, 0).UnixNano(),
		LastMsgSeq:      9,
		LastClientMsgNo: "cold",
		LastMsgAt:       time.Unix(90, 0).UnixNano(),
	}

	projector := NewProjector(ProjectorOptions{
		Store:         store,
		FlushInterval: time.Hour,
		DirtyLimit:    8,
		ColdThreshold: 30 * 24 * time.Hour,
		Now:           func() time.Time { return time.Unix(100, 0) },
		Async:         func(fn func()) { fn() },
	})

	msg := channellog.Message{ChannelID: "g1", ChannelType: 2, MessageSeq: 10, ClientMsgNo: "hot", Timestamp: 100}
	require.NoError(t, projector.SubmitCommitted(context.Background(), msg))

	got, err := projector.BatchGetHotChannelUpdates(context.Background(), []metadb.ConversationKey{key})
	require.NoError(t, err)
	require.Equal(t, map[metadb.ConversationKey]metadb.ChannelUpdateLog{
		key: {
			ChannelID:       "g1",
			ChannelType:     2,
			UpdatedAt:       time.Unix(100, 0).UnixNano(),
			LastMsgSeq:      10,
			LastClientMsgNo: "hot",
			LastMsgAt:       time.Unix(100, 0).UnixNano(),
		},
	}, got)
	require.Empty(t, store.updates)
}

type projectorStoreStub struct {
	cold            map[metadb.ConversationKey]metadb.ChannelUpdateLog
	updates         []metadb.ChannelUpdateLog
	touches         []metadb.UserConversationActivePatch
	subscribers     map[metadb.ConversationKey][]string
	subscriberCalls []subscriberListCall
}

type subscriberListCall struct {
	ChannelID   string
	ChannelType int64
	AfterUID    string
	Limit       int
}

func newProjectorStoreStub() *projectorStoreStub {
	return &projectorStoreStub{
		cold:        make(map[metadb.ConversationKey]metadb.ChannelUpdateLog),
		subscribers: make(map[metadb.ConversationKey][]string),
	}
}

func (s *projectorStoreStub) BatchGetChannelUpdateLogs(_ context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	out := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(keys))
	for _, key := range keys {
		if entry, ok := s.cold[key]; ok {
			out[key] = entry
		}
	}
	return out, nil
}

func (s *projectorStoreStub) UpsertChannelUpdateLogs(_ context.Context, entries []metadb.ChannelUpdateLog) error {
	for _, entry := range entries {
		key := metadb.ConversationKey{ChannelID: entry.ChannelID, ChannelType: entry.ChannelType}
		s.cold[key] = entry
		s.updates = append(s.updates, entry)
	}
	sort.Slice(s.updates, func(i, j int) bool {
		if s.updates[i].ChannelType != s.updates[j].ChannelType {
			return s.updates[i].ChannelType < s.updates[j].ChannelType
		}
		return s.updates[i].ChannelID < s.updates[j].ChannelID
	})
	return nil
}

func (s *projectorStoreStub) TouchUserConversationActiveAt(_ context.Context, patches []metadb.UserConversationActivePatch) error {
	s.touches = append(s.touches, patches...)
	return nil
}

func (s *projectorStoreStub) ListChannelSubscribers(_ context.Context, channelID string, channelType int64, afterUID string, limit int) ([]string, string, bool, error) {
	s.subscriberCalls = append(s.subscriberCalls, subscriberListCall{
		ChannelID:   channelID,
		ChannelType: channelType,
		AfterUID:    afterUID,
		Limit:       limit,
	})

	uids := append([]string(nil), s.subscribers[metadb.ConversationKey{ChannelID: channelID, ChannelType: channelType}]...)
	start := 0
	if afterUID != "" {
		for i, uid := range uids {
			if uid == afterUID {
				start = i + 1
				break
			}
		}
	}
	if start >= len(uids) {
		return nil, afterUID, true, nil
	}
	end := start + limit
	if end > len(uids) {
		end = len(uids)
	}
	page := append([]string(nil), uids[start:end]...)
	next := afterUID
	if len(page) > 0 {
		next = page[len(page)-1]
	}
	return page, next, end == len(uids), nil
}
