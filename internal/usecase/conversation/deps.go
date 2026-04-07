package conversation

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

type ConversationStateStore interface {
	GetUserConversationState(ctx context.Context, uid, channelID string, channelType int64) (metadb.UserConversationState, error)
	ListUserConversationActive(ctx context.Context, uid string, limit int) ([]metadb.UserConversationState, error)
	ScanUserConversationStatePage(ctx context.Context, uid string, after metadb.ConversationCursor, limit int) ([]metadb.UserConversationState, metadb.ConversationCursor, bool, error)
	ClearUserConversationActiveAt(ctx context.Context, uid string, keys []metadb.ConversationKey) error
}

type ChannelUpdateStore interface {
	BatchGetChannelUpdateLogs(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error)
}

type MessageFactsStore interface {
	LoadLatestMessages(ctx context.Context, keys []ConversationKey) (map[ConversationKey]channellog.Message, error)
	LoadRecentMessages(ctx context.Context, key ConversationKey, limit int) ([]channellog.Message, error)
}
