package app

import (
	"context"
	"errors"
	"sort"
	"testing"

	conversationusecase "github.com/WuKongIM/WuKongIM/internal/usecase/conversation"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

func TestChannelLogConversationFactsLoadLatestMessagesBatchesRemoteLoadsByOwner(t *testing.T) {
	remote := &recordingConversationFactsRemote{
		latestByNode: map[uint64]map[channellog.ChannelKey]channellog.Message{
			2: {
				{ChannelID: "g1", ChannelType: 2}: {ChannelID: "g1", ChannelType: 2, MessageSeq: 10},
				{ChannelID: "g2", ChannelType: 2}: {ChannelID: "g2", ChannelType: 2, MessageSeq: 20},
			},
			3: {
				{ChannelID: "g3", ChannelType: 2}: {ChannelID: "g3", ChannelType: 2, MessageSeq: 30},
			},
		},
		singularLatestErr: errors.New("unexpected singular latest load"),
	}
	metas := &staticConversationFactsMetas{
		metas: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			{ChannelID: "g1", ChannelType: 2}: {ChannelID: "g1", ChannelType: 2, Leader: 2},
			{ChannelID: "g2", ChannelType: 2}: {ChannelID: "g2", ChannelType: 2, Leader: 2},
			{ChannelID: "g3", ChannelType: 2}: {ChannelID: "g3", ChannelType: 2, Leader: 3},
		},
		singularErr: errors.New("unexpected singular meta lookup"),
	}
	facts := channelLogConversationFacts{
		cluster: staleConversationFactsCluster{},
		metas:   metas,
		remote: remote,
	}

	got, err := facts.LoadLatestMessages(context.Background(), []conversationusecase.ConversationKey{
		{ChannelID: "g1", ChannelType: 2},
		{ChannelID: "g2", ChannelType: 2},
		{ChannelID: "g3", ChannelType: 2},
	})
	require.NoError(t, err)
	require.Equal(t, map[conversationusecase.ConversationKey]channellog.Message{
		{ChannelID: "g1", ChannelType: 2}: {ChannelID: "g1", ChannelType: 2, MessageSeq: 10},
		{ChannelID: "g2", ChannelType: 2}: {ChannelID: "g2", ChannelType: 2, MessageSeq: 20},
		{ChannelID: "g3", ChannelType: 2}: {ChannelID: "g3", ChannelType: 2, MessageSeq: 30},
	}, got)
	require.Empty(t, remote.singularLatestCalls)
	require.Equal(t, []conversationFactsBatchCall{
		{NodeID: 2, Keys: []channellog.ChannelKey{{ChannelID: "g1", ChannelType: 2}, {ChannelID: "g2", ChannelType: 2}}},
		{NodeID: 3, Keys: []channellog.ChannelKey{{ChannelID: "g3", ChannelType: 2}}},
	}, normalizeConversationFactsBatchCalls(remote.latestBatchCalls))
	require.Equal(t, 1, metas.batchCalls)
}

func TestChannelLogConversationFactsSupportsBatchRecentLoadsByOwner(t *testing.T) {
	remote := &recordingConversationFactsRemote{
		recentsByNode: map[uint64]map[channellog.ChannelKey][]channellog.Message{
			2: {
				{ChannelID: "g1", ChannelType: 2}: {
					{ChannelID: "g1", ChannelType: 2, MessageSeq: 11},
					{ChannelID: "g1", ChannelType: 2, MessageSeq: 10},
				},
				{ChannelID: "g2", ChannelType: 2}: {
					{ChannelID: "g2", ChannelType: 2, MessageSeq: 21},
					{ChannelID: "g2", ChannelType: 2, MessageSeq: 20},
				},
			},
		},
		singularRecentErr: errors.New("unexpected singular recent load"),
	}
	metas := &staticConversationFactsMetas{
		metas: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			{ChannelID: "g1", ChannelType: 2}: {ChannelID: "g1", ChannelType: 2, Leader: 2},
			{ChannelID: "g2", ChannelType: 2}: {ChannelID: "g2", ChannelType: 2, Leader: 2},
		},
		singularErr: errors.New("unexpected singular meta lookup"),
	}
	facts := channelLogConversationFacts{
		cluster: staleConversationFactsCluster{},
		metas:   metas,
		remote: remote,
	}

	loader, ok := any(facts).(interface {
		LoadRecentMessagesBatch(context.Context, []conversationusecase.ConversationKey, int) (map[conversationusecase.ConversationKey][]channellog.Message, error)
	})
	require.True(t, ok, "channelLogConversationFacts should implement batch recent loading")

	got, err := loader.LoadRecentMessagesBatch(context.Background(), []conversationusecase.ConversationKey{
		{ChannelID: "g1", ChannelType: 2},
		{ChannelID: "g2", ChannelType: 2},
	}, 2)
	require.NoError(t, err)
	require.Equal(t, map[conversationusecase.ConversationKey][]channellog.Message{
		{ChannelID: "g1", ChannelType: 2}: {
			{ChannelID: "g1", ChannelType: 2, MessageSeq: 11},
			{ChannelID: "g1", ChannelType: 2, MessageSeq: 10},
		},
		{ChannelID: "g2", ChannelType: 2}: {
			{ChannelID: "g2", ChannelType: 2, MessageSeq: 21},
			{ChannelID: "g2", ChannelType: 2, MessageSeq: 20},
		},
	}, got)
	require.Empty(t, remote.singularRecentCalls)
	require.Equal(t, []conversationFactsBatchCall{
		{NodeID: 2, Keys: []channellog.ChannelKey{{ChannelID: "g1", ChannelType: 2}, {ChannelID: "g2", ChannelType: 2}}},
	}, normalizeConversationFactsBatchCalls(remote.recentBatchCalls))
	require.Equal(t, 1, metas.batchCalls)
}

type staleConversationFactsCluster struct{}

func (staleConversationFactsCluster) ApplyMeta(channellog.ChannelMeta) error {
	return nil
}

func (staleConversationFactsCluster) Append(context.Context, channellog.AppendRequest) (channellog.AppendResult, error) {
	return channellog.AppendResult{}, channellog.ErrStaleMeta
}

func (staleConversationFactsCluster) Fetch(context.Context, channellog.FetchRequest) (channellog.FetchResult, error) {
	return channellog.FetchResult{}, channellog.ErrStaleMeta
}

func (staleConversationFactsCluster) Status(channellog.ChannelKey) (channellog.ChannelRuntimeStatus, error) {
	return channellog.ChannelRuntimeStatus{}, channellog.ErrStaleMeta
}

type staticConversationFactsMetas struct {
	metas       map[channellog.ChannelKey]metadb.ChannelRuntimeMeta
	singularErr error
	batchCalls  int
}

func (s *staticConversationFactsMetas) GetChannelRuntimeMeta(_ context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	if s.singularErr != nil {
		return metadb.ChannelRuntimeMeta{}, s.singularErr
	}
	meta, ok := s.metas[channellog.ChannelKey{ChannelID: channelID, ChannelType: uint8(channelType)}]
	if !ok {
		return metadb.ChannelRuntimeMeta{}, metadb.ErrNotFound
	}
	return meta, nil
}

func (s *staticConversationFactsMetas) BatchGetChannelRuntimeMetas(_ context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error) {
	s.batchCalls++
	out := make(map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, len(keys))
	for _, key := range keys {
		meta, ok := s.metas[channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: uint8(key.ChannelType)}]
		if ok {
			out[key] = meta
		}
	}
	return out, nil
}

type conversationFactsBatchCall struct {
	NodeID uint64
	Keys   []channellog.ChannelKey
}

type recordingConversationFactsRemote struct {
	latestByNode        map[uint64]map[channellog.ChannelKey]channellog.Message
	recentsByNode       map[uint64]map[channellog.ChannelKey][]channellog.Message
	latestBatchCalls    []conversationFactsBatchCall
	recentBatchCalls    []conversationFactsBatchCall
	singularLatestCalls []channellog.ChannelKey
	singularRecentCalls []channellog.ChannelKey
	singularLatestErr   error
	singularRecentErr   error
}

func (r *recordingConversationFactsRemote) LoadLatestConversationMessage(_ context.Context, _ uint64, key channellog.ChannelKey, _ int) (channellog.Message, bool, error) {
	r.singularLatestCalls = append(r.singularLatestCalls, key)
	if r.singularLatestErr != nil {
		return channellog.Message{}, false, r.singularLatestErr
	}
	return channellog.Message{}, false, nil
}

func (r *recordingConversationFactsRemote) LoadRecentConversationMessages(_ context.Context, _ uint64, key channellog.ChannelKey, _ int, _ int) ([]channellog.Message, error) {
	r.singularRecentCalls = append(r.singularRecentCalls, key)
	if r.singularRecentErr != nil {
		return nil, r.singularRecentErr
	}
	return nil, nil
}

func (r *recordingConversationFactsRemote) LoadLatestConversationMessages(_ context.Context, nodeID uint64, keys []channellog.ChannelKey, _ int) (map[channellog.ChannelKey]channellog.Message, error) {
	r.latestBatchCalls = append(r.latestBatchCalls, conversationFactsBatchCall{
		NodeID: nodeID,
		Keys:   append([]channellog.ChannelKey(nil), keys...),
	})
	out := make(map[channellog.ChannelKey]channellog.Message, len(keys))
	for _, key := range keys {
		if msg, ok := r.latestByNode[nodeID][key]; ok {
			out[key] = msg
		}
	}
	return out, nil
}

func (r *recordingConversationFactsRemote) LoadRecentConversationMessagesBatch(_ context.Context, nodeID uint64, keys []channellog.ChannelKey, limit, _ int) (map[channellog.ChannelKey][]channellog.Message, error) {
	r.recentBatchCalls = append(r.recentBatchCalls, conversationFactsBatchCall{
		NodeID: nodeID,
		Keys:   append([]channellog.ChannelKey(nil), keys...),
	})
	out := make(map[channellog.ChannelKey][]channellog.Message, len(keys))
	for _, key := range keys {
		msgs := append([]channellog.Message(nil), r.recentsByNode[nodeID][key]...)
		if limit > 0 && len(msgs) > limit {
			msgs = msgs[:limit]
		}
		out[key] = msgs
	}
	return out, nil
}

func normalizeConversationFactsBatchCalls(calls []conversationFactsBatchCall) []conversationFactsBatchCall {
	out := make([]conversationFactsBatchCall, 0, len(calls))
	for _, call := range calls {
		keys := append([]channellog.ChannelKey(nil), call.Keys...)
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].ChannelType != keys[j].ChannelType {
				return keys[i].ChannelType < keys[j].ChannelType
			}
			return keys[i].ChannelID < keys[j].ChannelID
		})
		out = append(out, conversationFactsBatchCall{
			NodeID: call.NodeID,
			Keys:   keys,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})
	return out
}
