package node

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func testGroupKey(groupID uint64) isr.GroupKey {
	return isr.GroupKey("group-" + strconv.FormatUint(groupID, 10))
}

func TestManyGroupsToSamePeerReuseOneSession(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(21, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(22, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(testGroupKey(21), 2)
	env.runtime.enqueueReplication(testGroupKey(22), 2)
	env.runtime.runScheduler()

	if got := env.sessions.createdFor(2); got != 1 {
		t.Fatalf("expected one session for peer 2, got %d", got)
	}
}

func TestReplicationRequestPopulatesFetchRequestEnvelope(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(27, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	env.runtime.enqueueReplication(testGroupKey(27), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	if session.sendCount() != 1 {
		t.Fatalf("expected one fetch request send, got %d", session.sendCount())
	}
	if session.last.Kind != MessageKindFetchRequest {
		t.Fatalf("last kind = %v, want fetch request", session.last.Kind)
	}
	if session.last.FetchRequest == nil {
		t.Fatal("expected fetch request payload")
	}
	if session.last.FetchRequest.GroupKey != testGroupKey(27) {
		t.Fatalf("FetchRequest.GroupKey = %q, want %q", session.last.FetchRequest.GroupKey, testGroupKey(27))
	}
	if session.last.FetchRequest.ReplicaID != 1 {
		t.Fatalf("FetchRequest.ReplicaID = %d, want 1", session.last.FetchRequest.ReplicaID)
	}
	if session.last.FetchRequest.FetchOffset != 6 {
		t.Fatalf("FetchRequest.FetchOffset = %d, want 6", session.last.FetchRequest.FetchOffset)
	}
	if session.last.FetchRequest.OffsetEpoch != 4 {
		t.Fatalf("FetchRequest.OffsetEpoch = %d, want 4", session.last.FetchRequest.OffsetEpoch)
	}
	if session.last.FetchRequest.MaxBytes <= 0 {
		t.Fatalf("FetchRequest.MaxBytes = %d, want > 0", session.last.FetchRequest.MaxBytes)
	}
}

func TestReplicationRequestUsesReplicaOffsetEpochInsteadOfGroupMetaEpoch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(271, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 3
	replica.mu.Unlock()

	env.runtime.enqueueReplication(testGroupKey(271), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	if session.sendCount() != 1 {
		t.Fatalf("expected one fetch request send, got %d", session.sendCount())
	}
	if session.last.FetchRequest == nil {
		t.Fatal("expected fetch request payload")
	}
	if session.last.FetchRequest.OffsetEpoch != 3 {
		t.Fatalf("FetchRequest.OffsetEpoch = %d, want 3", session.last.FetchRequest.OffsetEpoch)
	}
}

func TestReplicationRequestUsesPeerSessionBatchingWhenAccepted(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(2711, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	session := env.sessions.session(2)
	session.setTryBatch(true)

	env.runtime.enqueueReplication(testGroupKey(2711), 2)
	env.runtime.runScheduler()

	if got := session.sendCount(); got != 0 {
		t.Fatalf("expected batched fetch request to avoid direct send, got %d sends", got)
	}
	if got := session.batchCount(); got != 1 {
		t.Fatalf("expected one batched fetch request, got %d", got)
	}
	if session.batched[0].Kind != MessageKindFetchRequest {
		t.Fatalf("batched kind = %v, want fetch request", session.batched[0].Kind)
	}
	if session.batched[0].FetchRequest == nil || session.batched[0].FetchRequest.FetchOffset != 6 {
		t.Fatalf("batched fetch request = %+v", session.batched[0].FetchRequest)
	}
}

func TestQueuedReplicationRecomputesReplicaProgressBetweenSends(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(272, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	session := env.sessions.session(2)
	session.afterSend = func() {
		replica.mu.Lock()
		defer replica.mu.Unlock()
		replica.state.LEO = 7
	}

	env.runtime.enqueueReplication(testGroupKey(272), 2)
	env.runtime.enqueueReplication(testGroupKey(272), 2)
	env.runtime.runScheduler()

	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected same group to keep one fetch request in flight until response, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected follow-up same-group replication to queue behind in-flight fetch, got %d", got)
	}
	if got := session.sent[0].FetchRequest.FetchOffset; got != 6 {
		t.Fatalf("first FetchOffset = %d, want 6", got)
	}

	env.runtime.releasePeerInflight(2)
	env.runtime.releaseGroupInflight(testGroupKey(272), 2)
	env.runtime.drainPeerQueue(2)

	if got := session.sendCount(); got != 2 {
		t.Fatalf("expected queued same-group fetch request to send after inflight release, got %d sends", got)
	}
	if got := session.sent[1].FetchRequest.FetchOffset; got != 7 {
		t.Fatalf("second FetchOffset after queued drain = %d, want 7", got)
	}
}

func TestQueuedReplicationRecomputesReplicaProgressAfterInflightQueueing(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(273, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	env.runtime.enqueueReplication(testGroupKey(273), 2)
	env.runtime.enqueueReplication(testGroupKey(273), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected one immediate fetch request send before draining queued inflight work, got %d", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected one queued peer request, got %d", got)
	}
	if got := session.sent[0].FetchRequest.FetchOffset; got != 6 {
		t.Fatalf("first FetchOffset = %d, want 6", got)
	}

	replica.mu.Lock()
	replica.state.LEO = 7
	replica.mu.Unlock()

	env.runtime.releasePeerInflight(2)
	env.runtime.releaseGroupInflight(testGroupKey(273), 2)
	env.runtime.drainPeerQueue(2)

	if got := session.sendCount(); got != 2 {
		t.Fatalf("expected drained queued fetch request to send once inflight is released, got %d sends", got)
	}
	if got := session.sent[1].FetchRequest.FetchOffset; got != 7 {
		t.Fatalf("second FetchOffset after queued drain = %d, want 7", got)
	}
}

func TestQueuedReplicationCoalescesSameGroupRequestsWhileInflight(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(2731, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	env.runtime.enqueueReplication(testGroupKey(2731), 2)
	env.runtime.enqueueReplication(testGroupKey(2731), 2)
	env.runtime.enqueueReplication(testGroupKey(2731), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected one immediate fetch request send, got %d", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected queued same-group fetches to coalesce to one request, got %d", got)
	}
}

func TestConcurrentPeerInflightStillSerializesSameGroupReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 2
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(274, 4, 1, []isr.NodeID{1, 2}))

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 6
	replica.state.Epoch = 4
	replica.state.OffsetEpoch = 4
	replica.mu.Unlock()

	env.runtime.enqueueReplication(testGroupKey(274), 2)
	env.runtime.enqueueReplication(testGroupKey(274), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected same group replication to keep one in-flight fetch even when peer limit is 2, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected follow-up replication for same group to remain queued behind in-flight fetch, got %d", got)
	}
	if got := session.sent[0].FetchRequest.FetchOffset; got != 6 {
		t.Fatalf("first FetchOffset = %d, want 6", got)
	}

	replica.mu.Lock()
	replica.state.LEO = 7
	replica.mu.Unlock()

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(274),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 7,
		},
	})

	if got := session.sendCount(); got != 2 {
		t.Fatalf("expected queued same-group replication to send after first fetch completes, got %d sends", got)
	}
	if got := session.sent[1].FetchRequest.FetchOffset; got != 7 {
		t.Fatalf("second FetchOffset after same-group drain = %d, want 7", got)
	}
}

func TestReplicationSendErrorRetriesOnceWithoutNewWork(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(28, 4, 1, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	session.enqueueSendErrors(context.DeadlineExceeded)

	env.runtime.enqueueReplication(testGroupKey(28), 2)
	env.runtime.runScheduler()

	if got := session.sendCount(); got != 2 {
		t.Fatalf("expected failed replication to be retried once automatically, got %d sends", got)
	}
}

func TestReplicationSendErrorKeepsRetryingUntilSuccessWithoutNewWork(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(29, 4, 1, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	session.enqueueSendErrors(context.DeadlineExceeded, context.DeadlineExceeded)

	env.runtime.enqueueReplication(testGroupKey(29), 2)
	env.runtime.runScheduler()

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := session.sendCount(); got >= 3 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("expected failed replication to keep retrying until success, got %d sends", session.sendCount())
}

func TestReplicationRetryIntervalUsesConfigOverride(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
		cfg.FollowerReplicationRetryInterval = time.Hour
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(30, 4, 1, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	session.enqueueSendErrors(context.DeadlineExceeded, context.DeadlineExceeded)

	env.runtime.enqueueReplication(testGroupKey(30), 2)
	env.runtime.runScheduler()

	time.Sleep(30 * time.Millisecond)
	if got := session.sendCount(); got != 2 {
		t.Fatalf("expected only the immediate retry before custom interval elapses, got %d sends", got)
	}
}

func TestScheduleFollowerReplicationCoalescesPendingDelayedRetry(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.FollowerReplicationRetryInterval = 15 * time.Millisecond
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(301, 4, 2, []isr.NodeID{1, 2}))

	env.runtime.runScheduler()

	groupKey := testGroupKey(301)
	env.runtime.scheduleFollowerReplication(groupKey, 2)
	env.runtime.scheduleFollowerReplication(groupKey, 2)
	env.runtime.scheduleFollowerReplication(groupKey, 2)

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := queuedReplicationPeers(env.runtime.groups[groupKey]); got > 0 {
			if got != 1 {
				t.Fatalf("expected one delayed retry to be queued, got %d", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("expected delayed retry to be queued")
}

func TestDelayedFollowerRetrySkipsStaleLeaderAfterMetaChange(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
		cfg.FollowerReplicationRetryInterval = 15 * time.Millisecond
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(302, 4, 2, []isr.NodeID{1, 2, 3}))

	oldLeader := env.sessions.session(2)
	newLeader := env.sessions.session(3)

	env.runtime.runScheduler()
	if got := oldLeader.sendCount(); got != 1 {
		t.Fatalf("expected initial fetch to old leader, got %d sends", got)
	}

	env.runtime.scheduleFollowerReplication(testGroupKey(302), 2)
	if err := env.runtime.ApplyMeta(testMetaLocal(302, 5, 3, []isr.NodeID{1, 2, 3})); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := newLeader.sendCount(); got >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := newLeader.sendCount(); got != 1 {
		t.Fatalf("expected immediate fetch to new leader, got %d sends", got)
	}

	time.Sleep(40 * time.Millisecond)
	if got := oldLeader.sendCount(); got != 1 {
		t.Fatalf("expected stale delayed retry to old leader to be skipped, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 0 {
		t.Fatalf("expected stale delayed retry to avoid queueing old leader fetches, got %d queued", got)
	}
}

func TestApplyMetaFailureLeavesCachedGroupMetaUnchanged(t *testing.T) {
	env := newSessionTestEnv(t)
	initial := testMetaLocal(31, 1, 2, []isr.NodeID{1, 2})
	mustEnsureLocal(t, env.runtime, initial)

	replica := env.factory.replicas[0]
	replica.becomeLeaderErr = context.DeadlineExceeded

	err := env.runtime.ApplyMeta(testMetaLocal(31, 2, 1, []isr.NodeID{1, 2}))
	if err == nil {
		t.Fatal("expected ApplyMeta to fail")
	}

	meta := env.runtime.groups[testGroupKey(31)].metaSnapshot()
	if meta.Epoch != initial.Epoch || meta.Leader != initial.Leader {
		t.Fatalf("cached meta changed on failure: %+v", meta)
	}
	if state := replica.Status(); state.Role != isr.RoleFollower || state.Epoch != initial.Epoch || state.Leader != initial.Leader {
		t.Fatalf("replica state changed on failure: %+v", state)
	}
}

func TestInboundEnvelopeDemuxRequiresMatchingGeneration(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(23, 1, 1, []isr.NodeID{1, 2}))

	env.transport.deliver(Envelope{GroupKey: testGroupKey(23), Generation: 99, Epoch: 1, Kind: MessageKindFetchResponse})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("unexpected apply fetch on generation mismatch")
	}
}

func TestFetchResponseDropsStaleEpoch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(24, 3, 1, []isr.NodeID{1, 2}))

	env.transport.deliver(Envelope{
		Peer:          2,
		GroupKey:      testGroupKey(24),
		Generation:    1,
		Epoch:         2,
		Kind:          MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{LeaderHW: 5, Records: []isr.Record{{Payload: []byte("stale"), SizeBytes: 5}}},
	})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("stale epoch response should be dropped")
	}
}

func TestFetchResponseDecodesPayloadIntoApplyFetch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(25, 4, 1, []isr.NodeID{1, 2}))

	truncateTo := uint64(7)
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(25),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW:   11,
			TruncateTo: &truncateTo,
			Records:    []isr.Record{{Payload: []byte("ok"), SizeBytes: 2}},
		},
	})

	if env.factory.replicas[0].applyFetchCalls != 1 {
		t.Fatalf("expected fetch response to be applied")
	}
	got := env.factory.replicas[0].lastApplyFetch
	if got.LeaderHW != 11 {
		t.Fatalf("expected LeaderHW 11, got %d", got.LeaderHW)
	}
	if got.TruncateTo == nil || *got.TruncateTo != 7 {
		t.Fatalf("expected TruncateTo 7, got %+v", got.TruncateTo)
	}
	if len(got.Records) != 1 || string(got.Records[0].Payload) != "ok" {
		t.Fatalf("unexpected records: %+v", got.Records)
	}
}

func TestFetchResponseSendsProgressAckAfterApplyFetch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(250, 4, 2, []isr.NodeID{1, 2}))

	env.factory.replicas[0].state.LEO = 9
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(250),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 9,
			Records:  []isr.Record{{Payload: []byte("ok"), SizeBytes: 2}},
		},
	})

	session := env.sessions.session(2)
	session.mu.Lock()
	sent := append([]Envelope(nil), session.sent...)
	session.mu.Unlock()
	if len(sent) < 2 {
		t.Fatalf("expected progress ack and follow-up fetch, got %d sends", len(sent))
	}
	if sent[0].Kind != MessageKindProgressAck {
		t.Fatalf("first outbound kind = %v, want progress ack", sent[0].Kind)
	}
	if sent[0].ProgressAck == nil {
		t.Fatal("expected progress ack payload")
	}
	if sent[0].ProgressAck.MatchOffset != 9 {
		t.Fatalf("progress ack match offset = %d, want 9", sent[0].ProgressAck.MatchOffset)
	}
	if sent[1].Kind != MessageKindFetchRequest {
		t.Fatalf("second outbound kind = %v, want fetch request", sent[1].Kind)
	}
}

func TestNonEmptyFetchResponseQueuesImmediateFollowerRefetch(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.FollowerReplicationRetryInterval = time.Hour
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(251, 4, 2, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	env.runtime.runScheduler()
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected initial follower fetch request, got %d sends", got)
	}

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(251),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 1,
			Records:  []isr.Record{{Payload: []byte("ok"), SizeBytes: 2}},
		},
	})

	if got := session.sendCount(); got != 3 {
		t.Fatalf("expected progress ack plus immediate follower re-fetch after apply, got %d sends", got)
	}
	if session.last.Kind != MessageKindFetchRequest {
		t.Fatalf("last kind = %v, want fetch request", session.last.Kind)
	}
	if session.last.GroupKey != testGroupKey(251) {
		t.Fatalf("last group = %q, want %q", session.last.GroupKey, testGroupKey(251))
	}
}

func TestProgressAckEnvelopeRequiresMatchingGeneration(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(253, 4, 1, []isr.NodeID{1, 2}))

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(253),
		Generation: 99,
		Epoch:      4,
		Kind:       MessageKindProgressAck,
		ProgressAck: &ProgressAckEnvelope{
			GroupKey:    testGroupKey(253),
			Epoch:       4,
			Generation:  99,
			ReplicaID:   2,
			MatchOffset: 7,
		},
	})

	if env.factory.replicas[0].applyProgressAckCalls != 0 {
		t.Fatalf("unexpected progress ack apply on generation mismatch")
	}
}

func TestLeaderAppliesProgressAckWithoutWaitingForNextFetch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(254, 4, 1, []isr.NodeID{1, 2}))

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(254),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindProgressAck,
		ProgressAck: &ProgressAckEnvelope{
			GroupKey:    testGroupKey(254),
			Epoch:       4,
			Generation:  1,
			ReplicaID:   2,
			MatchOffset: 7,
		},
	})

	replica := env.factory.replicas[0]
	if replica.applyProgressAckCalls != 1 {
		t.Fatalf("applyProgressAckCalls = %d, want 1", replica.applyProgressAckCalls)
	}
	if replica.lastProgressAck.MatchOffset != 7 {
		t.Fatalf("lastProgressAck.MatchOffset = %d, want 7", replica.lastProgressAck.MatchOffset)
	}
	if replica.fetchCalls != 0 {
		t.Fatalf("progress ack should not require follow-up fetch, got %d fetch calls", replica.fetchCalls)
	}
}

func TestFetchFailureEnvelopeReleasesInflightAndRetriesReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
		cfg.FollowerReplicationRetryInterval = time.Hour
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(255, 4, 2, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	env.runtime.runScheduler()
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected initial follower fetch request, got %d sends", got)
	}

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(255),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchFailure,
	})

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := session.sendCount(); got >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("expected fetch failure to trigger immediate retry, got %d sends", session.sendCount())
}

func TestEmptyFetchResponseUsesRetryIntervalBeforeFollowerRefetch(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.AutoRunScheduler = true
		cfg.FollowerReplicationRetryInterval = 20 * time.Millisecond
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(252, 4, 2, []isr.NodeID{1, 2}))

	session := env.sessions.session(2)
	env.runtime.runScheduler()
	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected initial follower fetch request, got %d sends", got)
	}

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(252),
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 1,
		},
	})

	if got := session.sendCount(); got != 1 {
		t.Fatalf("expected empty fetch response to avoid immediate follower re-fetch, got %d sends", got)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := session.sendCount(); got >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("expected empty fetch response to trigger delayed follower re-fetch, got %d sends", session.sendCount())
}

func TestServeFetchReturnsReplicaFetchResult(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(26, 5, 1, []isr.NodeID{1, 2}))

	truncateTo := uint64(9)
	env.factory.replicas[0].fetchResult = isr.FetchResult{
		Epoch:      5,
		HW:         11,
		TruncateTo: &truncateTo,
		Records: []isr.Record{
			{Payload: []byte("a"), SizeBytes: 1},
			{Payload: []byte("bc"), SizeBytes: 2},
		},
	}

	resp, err := env.runtime.ServeFetch(context.Background(), FetchRequestEnvelope{
		GroupKey:    testGroupKey(26),
		Epoch:       5,
		Generation:  1,
		ReplicaID:   2,
		FetchOffset: 7,
		OffsetEpoch: 4,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("ServeFetch() error = %v", err)
	}
	if env.factory.replicas[0].fetchCalls != 1 {
		t.Fatalf("expected replica.Fetch to be called once")
	}
	gotReq := env.factory.replicas[0].lastFetch
	if gotReq.GroupKey != testGroupKey(26) || gotReq.FetchOffset != 7 || gotReq.OffsetEpoch != 4 || gotReq.MaxBytes != 1024 {
		t.Fatalf("unexpected fetch request: %+v", gotReq)
	}
	if resp.GroupKey != testGroupKey(26) || resp.Epoch != 5 || resp.Generation != 1 {
		t.Fatalf("unexpected response envelope metadata: %+v", resp)
	}
	if resp.TruncateTo == nil || *resp.TruncateTo != truncateTo {
		t.Fatalf("unexpected TruncateTo: %+v", resp.TruncateTo)
	}
	if resp.LeaderHW != 11 {
		t.Fatalf("expected LeaderHW 11, got %d", resp.LeaderHW)
	}
	if len(resp.Records) != 2 || string(resp.Records[1].Payload) != "bc" {
		t.Fatalf("unexpected response records: %+v", resp.Records)
	}
}

type sessionTestEnv struct {
	runtime     *runtime
	generations *sessionGenerationStore
	factory     *sessionReplicaFactory
	transport   *sessionTransport
	sessions    *sessionPeerSessionManager
}

func newSessionTestEnv(t *testing.T) *sessionTestEnv {
	t.Helper()

	generations := newSessionGenerationStore()
	factory := newSessionReplicaFactory()
	transport := &sessionTransport{}
	sessions := newSessionPeerSessionManager()

	rt, err := New(Config{
		LocalNode:       1,
		ReplicaFactory:  factory,
		GenerationStore: generations,
		Transport:       transport,
		PeerSessions:    sessions,
		Tombstones: TombstonePolicy{
			TombstoneTTL: 30 * time.Second,
		},
		Now: func() time.Time { return time.Unix(1700000000, 0) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return &sessionTestEnv{
		runtime:     rt.(*runtime),
		generations: generations,
		factory:     factory,
		transport:   transport,
		sessions:    sessions,
	}
}

func testMetaLocal(groupID, epoch uint64, leader isr.NodeID, replicas []isr.NodeID) isr.GroupMeta {
	return isr.GroupMeta{
		GroupKey: testGroupKey(groupID),
		Epoch:    epoch,
		Leader:   leader,
		Replicas: append([]isr.NodeID(nil), replicas...),
		ISR:      append([]isr.NodeID(nil), replicas...),
		MinISR:   1,
	}
}

func queuedReplicationPeers(g *group) int {
	count := 0
	for {
		if _, ok := g.popReplicationPeer(); !ok {
			return count
		}
		count++
	}
}

func mustEnsureLocal(t *testing.T, rt *runtime, meta isr.GroupMeta) {
	t.Helper()
	if err := rt.EnsureGroup(meta); err != nil {
		t.Fatalf("EnsureGroup(%q) error = %v", meta.GroupKey, err)
	}
}

type sessionGenerationStore struct {
	mu     sync.Mutex
	values map[isr.GroupKey]uint64
}

func newSessionGenerationStore() *sessionGenerationStore {
	return &sessionGenerationStore{values: make(map[isr.GroupKey]uint64)}
}

func (s *sessionGenerationStore) Load(groupKey isr.GroupKey) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[groupKey], nil
}

func (s *sessionGenerationStore) Store(groupKey isr.GroupKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[groupKey] = generation
	return nil
}

type sessionReplicaFactory struct {
	mu       sync.Mutex
	replicas []*sessionReplica
}

func newSessionReplicaFactory() *sessionReplicaFactory {
	return &sessionReplicaFactory{}
}

func (f *sessionReplicaFactory) New(cfg GroupConfig) (isr.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	replica := &sessionReplica{
		state: isr.ReplicaState{
			GroupKey:    cfg.GroupKey,
			Role:        isr.RoleLeader,
			Epoch:       cfg.Meta.Epoch,
			OffsetEpoch: cfg.Meta.Epoch,
			Leader:      cfg.Meta.Leader,
		},
	}
	f.replicas = append(f.replicas, replica)
	return replica, nil
}

type sessionReplica struct {
	mu                    sync.Mutex
	state                 isr.ReplicaState
	applyFetchCalls       int
	lastApplyFetch        isr.ApplyFetchRequest
	applyFetchErr         error
	applyProgressAckCalls int
	lastProgressAck       isr.ProgressAckRequest
	applyProgressAckErr   error
	fetchCalls            int
	lastFetch             isr.FetchRequest
	fetchResult           isr.FetchResult
	fetchErr              error
	applyMetaErr          error
	becomeLeaderErr       error
	becomeFollowErr       error
}

func (r *sessionReplica) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applyMetaErr != nil {
		return r.applyMetaErr
	}
	r.state.GroupKey = meta.GroupKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	return nil
}

func (r *sessionReplica) BecomeLeader(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.becomeLeaderErr != nil {
		return r.becomeLeaderErr
	}
	r.state.GroupKey = meta.GroupKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	r.state.Role = isr.RoleLeader
	return nil
}

func (r *sessionReplica) BecomeFollower(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.becomeFollowErr != nil {
		return r.becomeFollowErr
	}
	r.state.GroupKey = meta.GroupKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	r.state.Role = isr.RoleFollower
	return nil
}
func (r *sessionReplica) Tombstone() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Role = isr.RoleTombstoned
	return nil
}
func (r *sessionReplica) InstallSnapshot(ctx context.Context, snap isr.Snapshot) error {
	return nil
}
func (r *sessionReplica) Append(ctx context.Context, batch []isr.Record) (isr.CommitResult, error) {
	return isr.CommitResult{}, nil
}
func (r *sessionReplica) Fetch(ctx context.Context, req isr.FetchRequest) (isr.FetchResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchCalls++
	r.lastFetch = req
	return r.fetchResult, r.fetchErr
}
func (r *sessionReplica) ApplyFetch(ctx context.Context, req isr.ApplyFetchRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyFetchCalls++
	r.lastApplyFetch = req
	return r.applyFetchErr
}
func (r *sessionReplica) ApplyProgressAck(ctx context.Context, req isr.ProgressAckRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyProgressAckCalls++
	r.lastProgressAck = req
	return r.applyProgressAckErr
}
func (r *sessionReplica) Status() isr.ReplicaState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

type sessionTransport struct {
	mu      sync.Mutex
	handler func(Envelope)
}

func (t *sessionTransport) Send(peer isr.NodeID, env Envelope) error {
	return nil
}

func (t *sessionTransport) RegisterHandler(fn func(Envelope)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = fn
}

func (t *sessionTransport) deliver(env Envelope) {
	t.mu.Lock()
	handler := t.handler
	t.mu.Unlock()
	if handler != nil {
		handler(env)
	}
}

type sessionPeerSessionManager struct {
	mu      sync.Mutex
	created map[isr.NodeID]int
	cache   map[isr.NodeID]*trackingPeerSession
}

func newSessionPeerSessionManager() *sessionPeerSessionManager {
	return &sessionPeerSessionManager{
		created: make(map[isr.NodeID]int),
		cache:   make(map[isr.NodeID]*trackingPeerSession),
	}
}

func (m *sessionPeerSessionManager) Session(peer isr.NodeID) PeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created[peer]++
	if session, ok := m.cache[peer]; ok {
		return session
	}
	session := &trackingPeerSession{}
	m.cache[peer] = session
	return session
}

func (m *sessionPeerSessionManager) createdFor(peer isr.NodeID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.created[peer]
}

func (m *sessionPeerSessionManager) session(peer isr.NodeID) *trackingPeerSession {
	m.mu.Lock()
	session, ok := m.cache[peer]
	m.mu.Unlock()
	if ok {
		return session
	}
	return m.Session(peer).(*trackingPeerSession)
}

type trackingPeerSession struct {
	mu           sync.Mutex
	sends        int
	last         Envelope
	sent         []Envelope
	batched      []Envelope
	sendErr      error
	sendErrs     []error
	backpressure BackpressureState
	afterSend    func()
	tryBatch     bool
}

func (s *trackingPeerSession) Send(env Envelope) error {
	s.mu.Lock()
	s.sends++
	s.last = env
	s.sent = append(s.sent, env)
	afterSend := s.afterSend
	if len(s.sendErrs) > 0 {
		err := s.sendErrs[0]
		s.sendErrs = append([]error(nil), s.sendErrs[1:]...)
		s.mu.Unlock()
		if afterSend != nil {
			afterSend()
		}
		return err
	}
	err := s.sendErr
	s.mu.Unlock()
	if afterSend != nil {
		afterSend()
	}
	return err
}

func (s *trackingPeerSession) TryBatch(env Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tryBatch {
		return false
	}
	s.batched = append(s.batched, env)
	return true
}

func (s *trackingPeerSession) Flush() error {
	return nil
}

func (s *trackingPeerSession) Backpressure() BackpressureState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backpressure
}

func (s *trackingPeerSession) Close() error {
	return nil
}

func (s *trackingPeerSession) sendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sends
}

func (s *trackingPeerSession) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batched)
}

func (s *trackingPeerSession) setBackpressure(state BackpressureState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backpressure = state
}

func (s *trackingPeerSession) setTryBatch(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tryBatch = v
}

func (s *trackingPeerSession) setSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *trackingPeerSession) enqueueSendErrors(errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErrs = append(s.sendErrs, errs...)
}
