package conversation

import (
	"context"
	"sort"
	"sync"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

type Projector interface {
	Start() error
	Stop() error
	SubmitCommitted(ctx context.Context, msg channellog.Message) error
	BatchGetHotChannelUpdates(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error)
	Flush(ctx context.Context) error
}

type ProjectorOptions struct {
	Store              ProjectorStore
	FlushInterval      time.Duration
	DirtyLimit         int
	ColdThreshold      time.Duration
	SubscriberPageSize int
	Now                func() time.Time
	Async              func(func())
}

type projector struct {
	store              ProjectorStore
	flushInterval      time.Duration
	dirtyLimit         int
	coldThreshold      time.Duration
	subscriberPageSize int
	now                func() time.Time
	async              func(func())

	mu      sync.RWMutex
	hot     map[metadb.ConversationKey]metadb.ChannelUpdateLog
	dirty   map[metadb.ConversationKey]struct{}
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewProjector(opts ProjectorOptions) Projector {
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.DirtyLimit <= 0 {
		opts.DirtyLimit = defaultFlushDirtyLimit
	}
	if opts.ColdThreshold <= 0 {
		opts.ColdThreshold = defaultColdThreshold
	}
	if opts.SubscriberPageSize <= 0 {
		opts.SubscriberPageSize = defaultSubscriberPageSize
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Async == nil {
		opts.Async = func(fn func()) { go fn() }
	}

	return &projector{
		store:              opts.Store,
		flushInterval:      opts.FlushInterval,
		dirtyLimit:         opts.DirtyLimit,
		coldThreshold:      opts.ColdThreshold,
		subscriberPageSize: opts.SubscriberPageSize,
		now:                opts.Now,
		async:              opts.Async,
		hot:                make(map[metadb.ConversationKey]metadb.ChannelUpdateLog),
		dirty:              make(map[metadb.ConversationKey]struct{}),
	}
}

func (p *projector) Start() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return nil
	}

	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})
	p.running = true

	go p.run(p.stopCh, p.doneCh)
	return nil
}

func (p *projector) Stop() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return p.Flush(context.Background())
	}
	stopCh := p.stopCh
	doneCh := p.doneCh
	p.running = false
	p.stopCh = nil
	p.doneCh = nil
	p.mu.Unlock()

	close(stopCh)
	<-doneCh
	return p.Flush(context.Background())
}

func (p *projector) SubmitCommitted(ctx context.Context, msg channellog.Message) error {
	if p == nil {
		return nil
	}

	key := metadb.ConversationKey{ChannelID: msg.ChannelID, ChannelType: int64(msg.ChannelType)}
	entry := channelUpdateFromMessage(msg)

	needWakeup := false
	if p.store != nil {
		p.mu.RLock()
		_, hot := p.hot[key]
		p.mu.RUnlock()
		if !hot {
			existing, err := p.store.BatchGetChannelUpdateLogs(ctx, []metadb.ConversationKey{key})
			if err == nil {
				current, ok := existing[key]
				needWakeup = !ok || p.isCold(current.LastMsgAt)
			}
		}
	}

	p.mu.Lock()
	existing, hot := p.hot[key]
	if !hot || shouldReplaceHotEntry(existing, entry) {
		p.hot[key] = entry
	}
	p.dirty[key] = struct{}{}
	dirtyCount := len(p.dirty)
	p.mu.Unlock()

	if needWakeup {
		msgCopy := msg
		p.async(func() {
			_ = p.touchConversationActive(context.Background(), msgCopy)
		})
	}
	if p.store != nil && dirtyCount >= p.dirtyLimit {
		p.async(func() {
			_ = p.Flush(context.Background())
		})
	}
	return nil
}

func (p *projector) BatchGetHotChannelUpdates(_ context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error) {
	if p == nil || len(keys) == 0 {
		return map[metadb.ConversationKey]metadb.ChannelUpdateLog{}, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	entries := make(map[metadb.ConversationKey]metadb.ChannelUpdateLog, len(keys))
	for _, key := range keys {
		if entry, ok := p.hot[key]; ok {
			entries[key] = entry
		}
	}
	return entries, nil
}

func (p *projector) Flush(ctx context.Context) error {
	if p == nil || p.store == nil {
		return nil
	}

	p.mu.RLock()
	if len(p.dirty) == 0 {
		p.mu.RUnlock()
		return nil
	}
	entries := make([]metadb.ChannelUpdateLog, 0, len(p.dirty))
	for key := range p.dirty {
		if entry, ok := p.hot[key]; ok {
			entries = append(entries, entry)
		}
	}
	p.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ChannelType != entries[j].ChannelType {
			return entries[i].ChannelType < entries[j].ChannelType
		}
		return entries[i].ChannelID < entries[j].ChannelID
	})

	if len(entries) == 0 {
		return nil
	}
	if err := p.store.UpsertChannelUpdateLogs(ctx, entries); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range entries {
		key := metadb.ConversationKey{ChannelID: entry.ChannelID, ChannelType: entry.ChannelType}
		current, ok := p.hot[key]
		if !ok {
			delete(p.dirty, key)
			continue
		}
		if !hotEntryNewerThan(current, entry) {
			delete(p.dirty, key)
		}
	}
	return nil
}

func (p *projector) run(stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = p.Flush(context.Background())
		case <-stopCh:
			return
		}
	}
}

func (p *projector) touchConversationActive(ctx context.Context, msg channellog.Message) error {
	if p.store == nil {
		return nil
	}

	activeAt := time.Unix(int64(msg.Timestamp), 0).UnixNano()
	if msg.ChannelType == wkframe.ChannelTypePerson {
		left, right, err := runtimechannelid.DecodePersonChannel(msg.ChannelID)
		if err != nil {
			return nil
		}
		return p.store.TouchUserConversationActiveAt(ctx, []metadb.UserConversationActivePatch{
			{UID: left, ChannelID: msg.ChannelID, ChannelType: int64(msg.ChannelType), ActiveAt: activeAt},
			{UID: right, ChannelID: msg.ChannelID, ChannelType: int64(msg.ChannelType), ActiveAt: activeAt},
		})
	}

	cursor := ""
	for {
		uids, nextCursor, done, err := p.store.ListChannelSubscribers(ctx, msg.ChannelID, int64(msg.ChannelType), cursor, p.subscriberPageSize)
		if err != nil {
			return err
		}
		if len(uids) > 0 {
			patches := make([]metadb.UserConversationActivePatch, 0, len(uids))
			for _, uid := range uids {
				patches = append(patches, metadb.UserConversationActivePatch{
					UID:         uid,
					ChannelID:   msg.ChannelID,
					ChannelType: int64(msg.ChannelType),
					ActiveAt:    activeAt,
				})
			}
			if err := p.store.TouchUserConversationActiveAt(ctx, patches); err != nil {
				return err
			}
		}
		if done {
			return nil
		}
		cursor = nextCursor
	}
}

func (p *projector) isCold(lastMsgAt int64) bool {
	if lastMsgAt == 0 {
		return true
	}
	return lastMsgAt <= p.now().Add(-p.coldThreshold).UnixNano()
}

func channelUpdateFromMessage(msg channellog.Message) metadb.ChannelUpdateLog {
	updatedAt := time.Unix(int64(msg.Timestamp), 0).UnixNano()
	return metadb.ChannelUpdateLog{
		ChannelID:       msg.ChannelID,
		ChannelType:     int64(msg.ChannelType),
		UpdatedAt:       updatedAt,
		LastMsgSeq:      msg.MessageSeq,
		LastClientMsgNo: msg.ClientMsgNo,
		LastMsgAt:       updatedAt,
	}
}

func shouldReplaceHotEntry(current, next metadb.ChannelUpdateLog) bool {
	return hotEntryNewerThan(next, current)
}

func hotEntryNewerThan(left, right metadb.ChannelUpdateLog) bool {
	if left.LastMsgSeq != 0 && right.LastMsgSeq != left.LastMsgSeq {
		return left.LastMsgSeq > right.LastMsgSeq
	}
	if right.UpdatedAt != left.UpdatedAt {
		return left.UpdatedAt > right.UpdatedAt
	}
	return left.LastClientMsgNo > right.LastClientMsgNo
}
