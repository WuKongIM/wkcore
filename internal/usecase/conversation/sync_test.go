package conversation

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

func TestSyncVersionZeroUsesWorkingSetAndClientOverlay(t *testing.T) {
	now := time.Unix(250, 0)
	repo := newConversationSyncRepoStub()

	repo.active = []metadb.UserConversationState{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ReadSeq: 5, ActiveAt: 200, UpdatedAt: 10},
	}
	repo.states[metadbKey("g1", 2)] = repo.active[0]
	repo.channelUpdates[metadbKey("g1", 2)] = metadb.ChannelUpdateLog{
		ChannelID:       "g1",
		ChannelType:     2,
		UpdatedAt:       time.Unix(150, 0).UnixNano(),
		LastMsgSeq:      12,
		LastClientMsgNo: "c1",
		LastMsgAt:       time.Unix(150, 0).UnixNano(),
	}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 12, "u2", 150, "c1")
	repo.latest[key("g2", 2)] = testMessage("g2", 2, 8, "u3", 200, "c2")

	app := New(Options{
		States:        repo,
		ChannelUpdate: repo,
		Facts:         repo,
		Now:           func() time.Time { return now },
		ColdThreshold: 30 * 24 * time.Hour,
		Async:         func(fn func()) { fn() },
	})

	got, err := app.Sync(context.Background(), SyncQuery{
		UID:         "u1",
		LastMsgSeqs: map[ConversationKey]uint64{key("g2", 2): 3},
		Limit:       10,
		MsgCount:    0,
	})
	require.NoError(t, err)
	require.Equal(t, []SyncConversation{
		{
			ChannelID:       "g2",
			ChannelType:     2,
			Unread:          8,
			Timestamp:       200,
			LastMsgSeq:      8,
			LastClientMsgNo: "c2",
			ReadedToMsgSeq:  0,
			Version:         time.Unix(200, 0).UnixNano(),
		},
		{
			ChannelID:       "g1",
			ChannelType:     2,
			Unread:          7,
			Timestamp:       150,
			LastMsgSeq:      12,
			LastClientMsgNo: "c1",
			ReadedToMsgSeq:  5,
			Version:         time.Unix(150, 0).UnixNano(),
		},
	}, got.Conversations)
	require.Empty(t, repo.recentLoads)
}

func TestSyncVersionPositiveScansUserDirectoryForStateAndChannelDeltas(t *testing.T) {
	repo := newConversationSyncRepoStub()
	repo.directory = []metadb.UserConversationState{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ReadSeq: 3, UpdatedAt: time.Unix(250, 0).UnixNano()},
		{UID: "u1", ChannelID: "g2", ChannelType: 2, ReadSeq: 1, UpdatedAt: 10},
		{UID: "u1", ChannelID: "g3", ChannelType: 2, ReadSeq: 1, UpdatedAt: 10},
	}
	for _, state := range repo.directory {
		repo.states[metadbKey(state.ChannelID, uint8(state.ChannelType))] = state
	}
	repo.channelUpdates[metadbKey("g2", 2)] = metadb.ChannelUpdateLog{
		ChannelID:       "g2",
		ChannelType:     2,
		UpdatedAt:       time.Unix(300, 0).UnixNano(),
		LastMsgSeq:      20,
		LastClientMsgNo: "c2",
		LastMsgAt:       time.Now().UnixNano(),
	}
	repo.channelUpdates[metadbKey("g3", 2)] = metadb.ChannelUpdateLog{
		ChannelID:       "g3",
		ChannelType:     2,
		UpdatedAt:       time.Unix(20, 0).UnixNano(),
		LastMsgSeq:      30,
		LastClientMsgNo: "c3",
		LastMsgAt:       time.Now().UnixNano(),
	}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 10, "u2", 100, "c1")
	repo.latest[key("g2", 2)] = testMessage("g2", 2, 20, "u2", 200, "c2")
	repo.latest[key("g3", 2)] = testMessage("g3", 2, 30, "u2", 300, "c3")

	app := New(Options{
		States:                repo,
		ChannelUpdate:         repo,
		Facts:                 repo,
		Now:                   time.Now,
		ColdThreshold:         30 * 24 * time.Hour,
		ChannelProbeBatchSize: 2,
		Async:                 func(fn func()) { fn() },
	})

	got, err := app.Sync(context.Background(), SyncQuery{
		UID:      "u1",
		Version:  time.Unix(200, 0).UnixNano(),
		Limit:    10,
		MsgCount: 0,
	})
	require.NoError(t, err)
	require.Equal(t, []SyncConversation{
		{
			ChannelID:       "g2",
			ChannelType:     2,
			Unread:          19,
			Timestamp:       200,
			LastMsgSeq:      20,
			LastClientMsgNo: "c2",
			ReadedToMsgSeq:  1,
			Version:         time.Unix(300, 0).UnixNano(),
		},
		{
			ChannelID:       "g1",
			ChannelType:     2,
			Unread:          7,
			Timestamp:       100,
			LastMsgSeq:      10,
			LastClientMsgNo: "c1",
			ReadedToMsgSeq:  3,
			Version:         time.Unix(250, 0).UnixNano(),
		},
	}, got.Conversations)
	require.ElementsMatch(t, []metadb.ConversationKey{
		{ChannelID: "g1", ChannelType: 2},
		{ChannelID: "g2", ChannelType: 2},
		{ChannelID: "g3", ChannelType: 2},
	}, flattenChannelUpdateLoads(repo.channelUpdateLoads))
}

func TestSyncColdRowsAreExcludedAndDemotedAsync(t *testing.T) {
	now := time.Unix(40*24*60*60, 0)
	repo := newConversationSyncRepoStub()
	repo.active = []metadb.UserConversationState{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ActiveAt: 200, UpdatedAt: 10},
	}
	repo.states[metadbKey("g1", 2)] = repo.active[0]
	repo.channelUpdates[metadbKey("g1", 2)] = metadb.ChannelUpdateLog{
		ChannelID:   "g1",
		ChannelType: 2,
		UpdatedAt:   20,
		LastMsgAt:   now.Add(-31 * 24 * time.Hour).UnixNano(),
	}

	app := New(Options{
		States:        repo,
		ChannelUpdate: repo,
		Facts:         repo,
		Now:           func() time.Time { return now },
		ColdThreshold: 30 * 24 * time.Hour,
		Async:         func(fn func()) { fn() },
	})

	got, err := app.Sync(context.Background(), SyncQuery{UID: "u1", Limit: 100})
	require.NoError(t, err)
	require.Empty(t, got.Conversations)
	require.Equal(t, []metadb.ConversationKey{{ChannelID: "g1", ChannelType: 2}}, repo.clearedActive)
}

func TestSyncAppliesOnlyUnreadDeleteLineAndStableLimitOrdering(t *testing.T) {
	repo := newConversationSyncRepoStub()
	repo.active = []metadb.UserConversationState{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ReadSeq: 5, ActiveAt: 400, UpdatedAt: 10},
		{UID: "u1", ChannelID: "g2", ChannelType: 2, ReadSeq: 4, DeletedToSeq: 6, ActiveAt: 390, UpdatedAt: 10},
		{UID: "u1", ChannelID: "g3", ChannelType: 2, ReadSeq: 3, ActiveAt: 380, UpdatedAt: 10},
		{UID: "u1", ChannelID: "g4", ChannelType: 2, ReadSeq: 1, ActiveAt: 370, UpdatedAt: 10},
	}
	for _, state := range repo.active {
		repo.states[metadbKey(state.ChannelID, uint8(state.ChannelType))] = state
	}
	for _, channelID := range []string{"g1", "g2", "g3", "g4"} {
		repo.channelUpdates[metadbKey(channelID, 2)] = metadb.ChannelUpdateLog{
			ChannelID:   channelID,
			ChannelType: 2,
			UpdatedAt:   400,
			LastMsgAt:   time.Now().UnixNano(),
		}
	}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 7, "u2", 300, "c1")
	repo.latest[key("g2", 2)] = testMessage("g2", 2, 6, "u2", 350, "c2")
	repo.latest[key("g3", 2)] = testMessage("g3", 2, 4, "u1", 300, "c3")
	repo.latest[key("g4", 2)] = testMessage("g4", 2, 5, "u4", 250, "c4")

	app := New(Options{
		States:        repo,
		ChannelUpdate: repo,
		Facts:         repo,
		Now:           time.Now,
		ColdThreshold: 30 * 24 * time.Hour,
		Async:         func(fn func()) { fn() },
	})

	got, err := app.Sync(context.Background(), SyncQuery{
		UID:        "u1",
		OnlyUnread: true,
		Limit:      1,
		MsgCount:   0,
	})
	require.NoError(t, err)
	require.Equal(t, []SyncConversation{
		{
			ChannelID:       "g1",
			ChannelType:     2,
			Unread:          2,
			Timestamp:       300,
			LastMsgSeq:      7,
			LastClientMsgNo: "c1",
			ReadedToMsgSeq:  5,
			Version:         400,
		},
	}, got.Conversations)
	require.Empty(t, repo.recentLoads)
}

func TestSyncLoadsRecentsOnlyForFinalLimitedWindow(t *testing.T) {
	repo := newConversationSyncRepoStub()
	repo.active = []metadb.UserConversationState{
		{UID: "u1", ChannelID: "g1", ChannelType: 2, ActiveAt: 300},
		{UID: "u1", ChannelID: "g2", ChannelType: 2, ActiveAt: 200},
		{UID: "u1", ChannelID: "g3", ChannelType: 2, ActiveAt: 100},
	}
	for _, state := range repo.active {
		repo.states[metadbKey(state.ChannelID, uint8(state.ChannelType))] = state
	}
	repo.channelUpdates[metadbKey("g1", 2)] = metadb.ChannelUpdateLog{ChannelID: "g1", ChannelType: 2, UpdatedAt: 300, LastMsgAt: time.Now().UnixNano()}
	repo.channelUpdates[metadbKey("g2", 2)] = metadb.ChannelUpdateLog{ChannelID: "g2", ChannelType: 2, UpdatedAt: 200, LastMsgAt: time.Now().UnixNano()}
	repo.channelUpdates[metadbKey("g3", 2)] = metadb.ChannelUpdateLog{ChannelID: "g3", ChannelType: 2, UpdatedAt: 100, LastMsgAt: time.Now().UnixNano()}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 30, "u2", 300, "c1")
	repo.latest[key("g2", 2)] = testMessage("g2", 2, 20, "u2", 200, "c2")
	repo.latest[key("g3", 2)] = testMessage("g3", 2, 10, "u2", 100, "c3")
	repo.recents[key("g1", 2)] = []channellog.Message{
		testMessage("g1", 2, 30, "u2", 300, "c1"),
		testMessage("g1", 2, 29, "u2", 299, "c1-1"),
	}
	repo.recents[key("g2", 2)] = []channellog.Message{
		testMessage("g2", 2, 20, "u2", 200, "c2"),
		testMessage("g2", 2, 19, "u2", 199, "c2-1"),
	}
	repo.recents[key("g3", 2)] = []channellog.Message{
		testMessage("g3", 2, 10, "u2", 100, "c3"),
		testMessage("g3", 2, 9, "u2", 99, "c3-1"),
	}

	app := New(Options{
		States:        repo,
		ChannelUpdate: repo,
		Facts:         repo,
		Now:           time.Now,
		ColdThreshold: 30 * 24 * time.Hour,
		Async:         func(fn func()) { fn() },
	})

	got, err := app.Sync(context.Background(), SyncQuery{
		UID:      "u1",
		Limit:    2,
		MsgCount: 2,
	})
	require.NoError(t, err)
	require.Len(t, got.Conversations, 2)
	require.Equal(t, []ConversationKey{key("g1", 2), key("g2", 2)}, repo.recentLoads)
	require.Equal(t, []channellog.Message{
		testMessage("g1", 2, 30, "u2", 300, "c1"),
		testMessage("g1", 2, 29, "u2", 299, "c1-1"),
	}, got.Conversations[0].Recents)
	require.Equal(t, []channellog.Message{
		testMessage("g2", 2, 20, "u2", 200, "c2"),
		testMessage("g2", 2, 19, "u2", 199, "c2-1"),
	}, got.Conversations[1].Recents)
}

type conversationSyncRepoStub struct {
	active             []metadb.UserConversationState
	directory          []metadb.UserConversationState
	states             map[metadb.ConversationKey]metadb.UserConversationState
	channelUpdates     map[metadb.ConversationKey]metadb.ChannelUpdateLog
	latest             map[ConversationKey]channellog.Message
	recents            map[ConversationKey][]channellog.Message
	clearedActive      []metadb.ConversationKey
	channelUpdateLoads [][]metadb.ConversationKey
	recentLoads        []ConversationKey
}

func newConversationSyncRepoStub() *conversationSyncRepoStub {
	return &conversationSyncRepoStub{
		states:         make(map[metadb.ConversationKey]metadb.UserConversationState),
		channelUpdates: make(map[metadb.ConversationKey]metadb.ChannelUpdateLog),
		latest:         make(map[ConversationKey]channellog.Message),
		recents:        make(map[ConversationKey][]channellog.Message),
	}
}

func (r *conversationSyncRepoStub) GetUserConversationState(_ context.Context, uid, channelID string, channelType int64) (metadb.UserConversationState, error) {
	state, ok := r.states[metadb.ConversationKey{ChannelID: channelID, ChannelType: channelType}]
	if !ok {
		return metadb.UserConversationState{}, metadb.ErrNotFound
	}
	return state, nil
}

func (r *conversationSyncRepoStub) ListUserConversationActive(_ context.Context, _ string, limit int) ([]metadb.UserConversationState, error) {
	out := append([]metadb.UserConversationState(nil), r.active...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *conversationSyncRepoStub) ScanUserConversationStatePage(_ context.Context, _ string, after metadb.ConversationCursor, limit int) ([]metadb.UserConversationState, metadb.ConversationCursor, bool, error) {
	dir := append([]metadb.UserConversationState(nil), r.directory...)
	sort.Slice(dir, func(i, j int) bool {
		if dir[i].ChannelType != dir[j].ChannelType {
			return dir[i].ChannelType < dir[j].ChannelType
		}
		return dir[i].ChannelID < dir[j].ChannelID
	})

	start := 0
	if after != (metadb.ConversationCursor{}) {
		for i, state := range dir {
			if state.ChannelType > after.ChannelType || (state.ChannelType == after.ChannelType && state.ChannelID > after.ChannelID) {
				start = i
				break
			}
			start = len(dir)
		}
	}
	if start >= len(dir) {
		return nil, after, true, nil
	}

	end := start + limit
	if end > len(dir) {
		end = len(dir)
	}
	page := append([]metadb.UserConversationState(nil), dir[start:end]...)
	cursor := metadb.ConversationCursor{
		ChannelID:   page[len(page)-1].ChannelID,
		ChannelType: page[len(page)-1].ChannelType,
	}
	return page, cursor, end == len(dir), nil
}

func (r *conversationSyncRepoStub) ClearUserConversationActiveAt(_ context.Context, _ string, keys []metadb.ConversationKey) error {
	r.clearedActive = append(r.clearedActive, keys...)
	return nil
}

func (r *conversationSyncRepoStub) BatchGetChannelUpdateLogs(_ context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	r.channelUpdateLoads = append(r.channelUpdateLoads, append([]metadb.ConversationKey(nil), keys...))
	out := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(keys))
	for _, key := range keys {
		entry, ok := r.channelUpdates[key]
		if ok {
			out[key] = entry
		}
	}
	return out, nil
}

func (r *conversationSyncRepoStub) LoadLatestMessages(_ context.Context, keys []ConversationKey) (map[ConversationKey]channellog.Message, error) {
	out := make(map[ConversationKey]channellog.Message, len(keys))
	for _, key := range keys {
		msg, ok := r.latest[key]
		if ok {
			out[key] = msg
		}
	}
	return out, nil
}

func (r *conversationSyncRepoStub) LoadRecentMessages(_ context.Context, key ConversationKey, limit int) ([]channellog.Message, error) {
	r.recentLoads = append(r.recentLoads, key)
	msgs := append([]channellog.Message(nil), r.recents[key]...)
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

func key(channelID string, channelType uint8) ConversationKey {
	return ConversationKey{
		ChannelID:   channelID,
		ChannelType: channelType,
	}
}

func metadbKey(channelID string, channelType uint8) metadb.ConversationKey {
	return metadb.ConversationKey{
		ChannelID:   channelID,
		ChannelType: int64(channelType),
	}
}

func testMessage(channelID string, channelType uint8, seq uint64, fromUID string, ts int32, clientMsgNo string) channellog.Message {
	return channellog.Message{
		ChannelID:   channelID,
		ChannelType: channelType,
		MessageSeq:  seq,
		FromUID:     fromUID,
		Timestamp:   ts,
		ClientMsgNo: clientMsgNo,
	}
}

func flattenChannelUpdateLoads(loads [][]metadb.ConversationKey) []metadb.ConversationKey {
	var out []metadb.ConversationKey
	for _, load := range loads {
		out = append(out, load...)
	}
	return out
}
