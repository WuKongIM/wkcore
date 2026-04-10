package log

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func TestAppendReturnsCommittedMessageSeqFromHW(t *testing.T) {
	env := newAppendEnv(t)

	result, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.MessageSeq != 1 {
		t.Fatalf("MessageSeq = %d, want 1", result.MessageSeq)
	}
	if result.MessageID != 1 {
		t.Fatalf("MessageID = %d, want 1", result.MessageID)
	}
	if result.Message.MessageID != 1 || result.Message.MessageSeq != 1 {
		t.Fatalf("Message = %+v, want committed identifiers", result.Message)
	}
	if result.Message.FromUID != "u1" {
		t.Fatalf("FromUID = %q, want %q", result.Message.FromUID, "u1")
	}
	if result.Message.ClientMsgNo != "m1" {
		t.Fatalf("ClientMsgNo = %q, want %q", result.Message.ClientMsgNo, "m1")
	}
	if !bytes.Equal(result.Message.Payload, []byte("payload")) {
		t.Fatalf("Payload = %q, want %q", result.Message.Payload, "payload")
	}
}

func TestAppendUsesBaseOffsetForSingleMessageSeqWhenCommitHWCoversBatch(t *testing.T) {
	env := newAppendEnv(t)
	env.group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		return isr.CommitResult{
			BaseOffset:   7,
			NextCommitHW: 9,
			RecordCount:  len(records),
		}, nil
	}

	result, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.MessageSeq != 8 {
		t.Fatalf("MessageSeq = %d, want 8", result.MessageSeq)
	}
	if result.Message.MessageSeq != 8 {
		t.Fatalf("Message.MessageSeq = %d, want 8", result.Message.MessageSeq)
	}
}

func TestAppendReturnsExistingEntryOnIdempotentRetry(t *testing.T) {
	env := newAppendEnv(t)

	first, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	second, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if first.MessageID != second.MessageID || first.MessageSeq != second.MessageSeq {
		t.Fatalf("results differ: first=%+v second=%+v", first, second)
	}
	if first.Message.MessageID != second.Message.MessageID || first.Message.MessageSeq != second.Message.MessageSeq {
		t.Fatalf("committed messages differ: first=%+v second=%+v", first.Message, second.Message)
	}
	if first.Message.FromUID != second.Message.FromUID || !bytes.Equal(first.Message.Payload, second.Message.Payload) {
		t.Fatalf("committed payloads differ: first=%+v second=%+v", first.Message, second.Message)
	}
	if env.group.appendCalls != 1 {
		t.Fatalf("appendCalls = %d, want 1", env.group.appendCalls)
	}
	if env.log.readCalls != 1 {
		t.Fatalf("readCalls = %d, want 1", env.log.readCalls)
	}
}

func TestAppendReturnsCommittedResultWithoutPostCommitIdempotencyRewrite(t *testing.T) {
	env := newAppendEnv(t)
	env.group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		base := uint64(len(env.log.records))
		for _, record := range records {
			env.log.records = append(env.log.records, LogRecord{
				Offset:  uint64(len(env.log.records)),
				Payload: append([]byte(nil), record.Payload...),
			})
		}
		env.group.state.HW = uint64(len(env.log.records))
		env.state.idempotency = map[IdempotencyKey]IdempotencyEntry{
			{
				ChannelID:   "c1",
				ChannelType: 1,
				FromUID:     "u1",
				ClientMsgNo: "m1",
			}: {
				MessageID:  1,
				MessageSeq: 1,
				Offset:     0,
			},
		}
		return isr.CommitResult{
			BaseOffset:   base,
			NextCommitHW: env.group.state.HW,
			RecordCount:  len(records),
		}, nil
	}

	result, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.MessageSeq != 1 {
		t.Fatalf("MessageSeq = %d, want 1", result.MessageSeq)
	}
	if env.state.getCalls != 1 {
		t.Fatalf("getCalls = %d, want 1", env.state.getCalls)
	}
	if env.state.putCalls != 0 {
		t.Fatalf("putCalls = %d, want 0", env.state.putCalls)
	}
}

func TestAppendDuplicateStillReturnsStoredMessageAfterCoordinatorCommit(t *testing.T) {
	env := newAppendEnv(t)
	env.group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		base := uint64(len(env.log.records))
		for _, record := range records {
			env.log.records = append(env.log.records, LogRecord{
				Offset:  uint64(len(env.log.records)),
				Payload: append([]byte(nil), record.Payload...),
			})
		}
		env.group.state.HW = uint64(len(env.log.records))
		env.state.idempotency = map[IdempotencyKey]IdempotencyEntry{
			{
				ChannelID:   "c1",
				ChannelType: 1,
				FromUID:     "u1",
				ClientMsgNo: "m1",
			}: {
				MessageID:  1,
				MessageSeq: 1,
				Offset:     0,
			},
		}
		return isr.CommitResult{
			BaseOffset:   base,
			NextCommitHW: env.group.state.HW,
			RecordCount:  len(records),
		}, nil
	}

	first, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	second, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if first.MessageID != second.MessageID || first.MessageSeq != second.MessageSeq {
		t.Fatalf("results differ: first=%+v second=%+v", first, second)
	}
	if env.group.appendCalls != 1 {
		t.Fatalf("appendCalls = %d, want 1", env.group.appendCalls)
	}
	if env.state.putCalls != 0 {
		t.Fatalf("putCalls = %d, want 0", env.state.putCalls)
	}
}

func TestAppendReturnsErrNotLeaderWhenGroupRoleIsFollower(t *testing.T) {
	env := newAppendEnv(t)
	env.group.state.Role = isr.RoleFollower

	_, err := env.cluster.Append(context.Background(), testAppendRequest())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestAppendReturnsErrStaleMetaWhenChannelMissing(t *testing.T) {
	c := newTestCluster()

	_, err := c.Append(context.Background(), testAppendRequest())
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestAppendReturnsErrIdempotencyConflictWhenPayloadChanges(t *testing.T) {
	env := newAppendEnv(t)
	_, err := env.cluster.Append(context.Background(), testAppendRequest())
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}

	req := testAppendRequest()
	req.Message.Payload = []byte("different")
	_, err = env.cluster.Append(context.Background(), req)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestAppendReturnsErrChannelDeleting(t *testing.T) {
	env := newAppendEnv(t)
	meta := env.meta
	meta.ChannelEpoch++
	meta.Status = ChannelStatusDeleting
	if err := env.cluster.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	_, err := env.cluster.Append(context.Background(), testAppendRequest())
	if !errors.Is(err, ErrChannelDeleting) {
		t.Fatalf("expected ErrChannelDeleting, got %v", err)
	}
}

func TestAppendReturnsErrProtocolUpgradeRequiredForLegacyClientOnU64Channel(t *testing.T) {
	env := newAppendEnv(t)
	meta := env.meta
	meta.ChannelEpoch++
	meta.Features.MessageSeqFormat = MessageSeqFormatU64
	if err := env.cluster.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	req := testAppendRequest()
	req.SupportsMessageSeqU64 = false

	_, err := env.cluster.Append(context.Background(), req)
	if !errors.Is(err, ErrProtocolUpgradeRequired) {
		t.Fatalf("expected ErrProtocolUpgradeRequired, got %v", err)
	}
}

func TestAppendReturnsErrMessageSeqExhaustedAtLegacyLimit(t *testing.T) {
	env := newAppendEnv(t)
	env.group.state.HW = maxLegacyMessageSeq

	_, err := env.cluster.Append(context.Background(), testAppendRequest())
	if !errors.Is(err, ErrMessageSeqExhausted) {
		t.Fatalf("expected ErrMessageSeqExhausted, got %v", err)
	}
}

type appendEnv struct {
	cluster *cluster
	group   *fakeChannelHandle
	log     *fakeMessageLog
	state   *fakeStateStore
	meta    ChannelMeta
}

func newAppendEnv(t *testing.T) *appendEnv {
	t.Helper()

	log := &fakeMessageLog{}
	stores := &fakeStateStoreFactory{}
	state, err := stores.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	if err != nil {
		t.Fatalf("stores.ForChannel() error = %v", err)
	}
	stateStore := state.(*fakeStateStore)
	group := &fakeChannelHandle{
		state: isr.ReplicaState{
			ChannelKey: isrChannelKeyForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1}),
			Role:       isr.RoleLeader,
			Epoch:      9,
			Leader:     1,
			HW:         0,
		},
	}
	group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		base := uint64(len(log.records))
		for _, record := range records {
			log.records = append(log.records, LogRecord{
				Offset:  uint64(len(log.records)),
				Payload: append([]byte(nil), record.Payload...),
			})
			view, err := decodeMessageView(record.Payload)
			if err != nil {
				return isr.CommitResult{}, err
			}
			if view.Message.ClientMsgNo == "" {
				continue
			}
			if err := stateStore.PutIdempotency(IdempotencyKey{
				ChannelID:   view.Message.ChannelID,
				ChannelType: view.Message.ChannelType,
				FromUID:     view.Message.FromUID,
				ClientMsgNo: view.Message.ClientMsgNo,
			}, IdempotencyEntry{
				MessageID:  view.Message.MessageID,
				MessageSeq: base + uint64(len(log.records)),
				Offset:     uint64(len(log.records) - 1),
			}); err != nil {
				return isr.CommitResult{}, err
			}
		}
		group.state.HW = uint64(len(log.records))
		return isr.CommitResult{
			BaseOffset:   base,
			NextCommitHW: group.state.HW,
			RecordCount:  len(records),
		}, nil
	}

	runtime := &fakeRuntime{
		groups: map[isr.ChannelKey]*fakeChannelHandle{
			isrChannelKeyForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1}): group,
		},
	}
	got, err := New(Config{
		Runtime:    runtime,
		Log:        log,
		States:     stores,
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	c := got.(*cluster)
	meta := testMeta("c1", 1, 3, 9)
	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	return &appendEnv{
		cluster: c,
		group:   group,
		log:     log,
		state:   stateStore,
		meta:    meta,
	}
}

func testAppendRequest() AppendRequest {
	return AppendRequest{
		ChannelID:   "c1",
		ChannelType: 1,
		Message: Message{
			ChannelID:   "c1",
			ChannelType: 1,
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("payload"),
		},
	}
}
