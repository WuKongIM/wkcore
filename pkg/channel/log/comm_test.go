package log

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

type realRuntime struct {
	groups map[isr.ChannelKey]ChannelHandle
}

func (r *realRuntime) Channel(channelKey isr.ChannelKey) (ChannelHandle, bool) {
	group, ok := r.groups[channelKey]
	return group, ok
}

func newRealStoreCluster(t testing.TB, db *DB, key ChannelKey, replica isr.Replica, channelEpoch, leaderEpoch uint64) *cluster {
	t.Helper()

	got, err := New(Config{
		Runtime: &realRuntime{
			groups: map[isr.ChannelKey]ChannelHandle{
				isrChannelKeyForChannel(key): replica,
			},
		},
		Log:        db,
		States:     db.StateStoreFactory(),
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	c := got.(*cluster)
	if err := c.ApplyMeta(realStoreChannelMeta(key, channelEpoch, leaderEpoch)); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	return c
}

func realStoreAppendRequest(key ChannelKey, payload string) AppendRequest {
	return AppendRequest{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		Message: Message{
			ChannelID:   key.ChannelID,
			ChannelType: key.ChannelType,
			FromUID:     "u1",
			ClientMsgNo: "msg-" + payload,
			Payload:     []byte(payload),
		},
	}
}

func realStoreChannelMeta(key ChannelKey, channelEpoch, leaderEpoch uint64) ChannelMeta {
	return ChannelMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  key.ChannelType,
		ChannelEpoch: channelEpoch,
		LeaderEpoch:  leaderEpoch,
		Replicas:     []NodeID{1},
		ISR:          []NodeID{1},
		Leader:       1,
		MinISR:       1,
		Status:       ChannelStatusActive,
		Features: ChannelFeatures{
			MessageSeqFormat: MessageSeqFormatLegacyU32,
		},
	}
}

type singleStateStoreFactory struct {
	store ChannelStateStore
}

func (f *singleStateStoreFactory) ForChannel(ChannelKey) (ChannelStateStore, error) {
	return f.store, nil
}

type atomicCheckpointOnlyStateStore struct {
	idempotency    map[IdempotencyKey]IdempotencyEntry
	directPutErr   error
	directPutCalls int
	atomicCommits  int
}

func (s *atomicCheckpointOnlyStateStore) PutIdempotency(key IdempotencyKey, entry IdempotencyEntry) error {
	s.directPutCalls++
	if s.directPutErr != nil {
		return s.directPutErr
	}
	if s.idempotency == nil {
		s.idempotency = make(map[IdempotencyKey]IdempotencyEntry)
	}
	s.idempotency[key] = entry
	return nil
}

func (s *atomicCheckpointOnlyStateStore) GetIdempotency(key IdempotencyKey) (IdempotencyEntry, bool, error) {
	entry, ok := s.idempotency[key]
	return entry, ok, nil
}

func (s *atomicCheckpointOnlyStateStore) Snapshot(uint64) ([]byte, error) {
	return nil, nil
}

func (s *atomicCheckpointOnlyStateStore) Restore([]byte) error {
	return nil
}

func (s *atomicCheckpointOnlyStateStore) CommitCommittedWithCheckpoint(_ isr.Checkpoint, batch []appliedMessage) error {
	s.atomicCommits++
	if s.idempotency == nil {
		s.idempotency = make(map[IdempotencyKey]IdempotencyEntry)
	}
	for _, msg := range batch {
		s.idempotency[msg.key] = msg.entry
	}
	return nil
}
