package channelcluster

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/isr"
)

type fakeGroupHandle struct {
	state       isr.ReplicaState
	appendErr   error
	appendCalls int
	appendFn    func([]isr.Record) (isr.CommitResult, error)
}

func (f *fakeGroupHandle) Append(_ context.Context, records []isr.Record) (isr.CommitResult, error) {
	f.appendCalls++
	if f.appendErr != nil {
		return isr.CommitResult{}, f.appendErr
	}
	if f.appendFn != nil {
		return f.appendFn(records)
	}
	return isr.CommitResult{}, nil
}

func (f *fakeGroupHandle) Status() isr.ReplicaState {
	return f.state
}

type fakeRuntime struct {
	groups map[uint64]*fakeGroupHandle
}

func (f *fakeRuntime) Group(groupID uint64) (GroupHandle, bool) {
	if f == nil {
		return nil, false
	}
	group, ok := f.groups[groupID]
	return group, ok
}

type fakeMessageLog struct {
	records []LogRecord
}

func (f *fakeMessageLog) Read(_ uint64, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if limit <= 0 || fromOffset >= uint64(len(f.records)) {
		return nil, nil
	}

	total := 0
	out := make([]LogRecord, 0, limit)
	for i := fromOffset; i < uint64(len(f.records)) && len(out) < limit; i++ {
		record := f.records[i]
		size := len(record.Payload)
		if len(out) > 0 && total+size > maxBytes {
			break
		}
		out = append(out, record)
		total += size
	}
	return out, nil
}

type fakeStateStore struct {
	idempotency   map[IdempotencyKey]IdempotencyEntry
	restoreCalled bool
}

func (f *fakeStateStore) PutIdempotency(key IdempotencyKey, entry IdempotencyEntry) error {
	if f.idempotency == nil {
		f.idempotency = make(map[IdempotencyKey]IdempotencyEntry)
	}
	f.idempotency[key] = entry
	return nil
}

func (f *fakeStateStore) GetIdempotency(key IdempotencyKey) (IdempotencyEntry, bool, error) {
	entry, ok := f.idempotency[key]
	return entry, ok, nil
}

func (f *fakeStateStore) Snapshot(uint64) ([]byte, error) {
	return nil, nil
}

func (f *fakeStateStore) Restore([]byte) error {
	f.restoreCalled = true
	return nil
}

type fakeStateStoreFactory struct {
	stores map[ChannelKey]*fakeStateStore
}

func (f *fakeStateStoreFactory) ForChannel(key ChannelKey) (ChannelStateStore, error) {
	if f.stores == nil {
		f.stores = make(map[ChannelKey]*fakeStateStore)
	}
	store, ok := f.stores[key]
	if !ok {
		store = &fakeStateStore{}
		f.stores[key] = store
	}
	return store, nil
}

type fakeMessageIDGenerator struct {
	next uint64
}

func (f *fakeMessageIDGenerator) Next() uint64 {
	f.next++
	return f.next
}

func newTestCluster() *cluster {
	got, err := New(Config{
		Runtime:    &fakeRuntime{},
		Log:        &fakeMessageLog{},
		States:     &fakeStateStoreFactory{},
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		panic(err)
	}
	return got.(*cluster)
}

func testMeta(channelID string, channelType uint8, groupID, channelEpoch, leaderEpoch uint64) ChannelMeta {
	return ChannelMeta{
		GroupID:      groupID,
		ChannelID:    channelID,
		ChannelType:  channelType,
		ChannelEpoch: channelEpoch,
		LeaderEpoch:  leaderEpoch,
		Replicas:     []NodeID{1, 2, 3},
		ISR:          []NodeID{1, 2},
		Leader:       1,
		MinISR:       2,
		Status:       ChannelStatusActive,
		Features: ChannelFeatures{
			MessageSeqFormat: MessageSeqFormatLegacyU32,
		},
	}
}

func conflictingReplay(meta ChannelMeta) ChannelMeta {
	meta.Leader = 2
	return meta
}
