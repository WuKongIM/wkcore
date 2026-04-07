package channellog

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestSendReturnsCommittedMessageSeqFromHW(t *testing.T) {
	env := newSendEnv(t)

	result, err := env.cluster.Send(context.Background(), testSendRequest())
	if err != nil {
		t.Fatalf("Send() error = %v", err)
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

func TestSendReturnsExistingEntryOnIdempotentRetry(t *testing.T) {
	env := newSendEnv(t)

	first, err := env.cluster.Send(context.Background(), testSendRequest())
	if err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	second, err := env.cluster.Send(context.Background(), testSendRequest())
	if err != nil {
		t.Fatalf("second Send() error = %v", err)
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

func TestSendReturnsErrNotLeaderWhenGroupRoleIsFollower(t *testing.T) {
	env := newSendEnv(t)
	env.group.state.Role = isr.RoleFollower

	_, err := env.cluster.Send(context.Background(), testSendRequest())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestSendReturnsErrStaleMetaWhenChannelMissing(t *testing.T) {
	c := newTestCluster()

	_, err := c.Send(context.Background(), testSendRequest())
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestSendReturnsErrIdempotencyConflictWhenPayloadChanges(t *testing.T) {
	env := newSendEnv(t)
	_, err := env.cluster.Send(context.Background(), testSendRequest())
	if err != nil {
		t.Fatalf("first Send() error = %v", err)
	}

	req := testSendRequest()
	req.Message.Payload = []byte("different")
	_, err = env.cluster.Send(context.Background(), req)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestSendReturnsErrChannelDeleting(t *testing.T) {
	env := newSendEnv(t)
	meta := env.meta
	meta.ChannelEpoch++
	meta.Status = ChannelStatusDeleting
	if err := env.cluster.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	_, err := env.cluster.Send(context.Background(), testSendRequest())
	if !errors.Is(err, ErrChannelDeleting) {
		t.Fatalf("expected ErrChannelDeleting, got %v", err)
	}
}

func TestSendReturnsErrProtocolUpgradeRequiredForLegacyClientOnU64Channel(t *testing.T) {
	env := newSendEnv(t)
	meta := env.meta
	meta.ChannelEpoch++
	meta.Features.MessageSeqFormat = MessageSeqFormatU64
	if err := env.cluster.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	req := testSendRequest()
	req.SupportsMessageSeqU64 = false

	_, err := env.cluster.Send(context.Background(), req)
	if !errors.Is(err, ErrProtocolUpgradeRequired) {
		t.Fatalf("expected ErrProtocolUpgradeRequired, got %v", err)
	}
}

func TestSendReturnsErrMessageSeqExhaustedAtLegacyLimit(t *testing.T) {
	env := newSendEnv(t)
	env.group.state.HW = maxLegacyMessageSeq

	_, err := env.cluster.Send(context.Background(), testSendRequest())
	if !errors.Is(err, ErrMessageSeqExhausted) {
		t.Fatalf("expected ErrMessageSeqExhausted, got %v", err)
	}
}

type sendEnv struct {
	cluster *cluster
	group   *fakeGroupHandle
	log     *fakeMessageLog
	meta    ChannelMeta
}

func newSendEnv(t *testing.T) *sendEnv {
	t.Helper()

	log := &fakeMessageLog{}
	group := &fakeGroupHandle{
		state: isr.ReplicaState{
			GroupKey: channelGroupKey(ChannelKey{ChannelID: "c1", ChannelType: 1}),
			Role:     isr.RoleLeader,
			Epoch:    9,
			Leader:   1,
			HW:       0,
		},
	}
	group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		base := uint64(len(log.records))
		for _, record := range records {
			log.records = append(log.records, LogRecord{
				Offset:  uint64(len(log.records)),
				Payload: append([]byte(nil), record.Payload...),
			})
		}
		group.state.HW = uint64(len(log.records))
		return isr.CommitResult{
			BaseOffset:   base,
			NextCommitHW: group.state.HW,
			RecordCount:  len(records),
		}, nil
	}

	runtime := &fakeRuntime{
		groups: map[isr.GroupKey]*fakeGroupHandle{
			channelGroupKey(ChannelKey{ChannelID: "c1", ChannelType: 1}): group,
		},
	}
	stores := &fakeStateStoreFactory{}
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
	return &sendEnv{
		cluster: c,
		group:   group,
		log:     log,
		meta:    meta,
	}
}

func testSendRequest() SendRequest {
	return SendRequest{
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
