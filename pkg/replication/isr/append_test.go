package isr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type staleAppendLEOLogStore struct {
	*fakeLogStore
}

func (f *staleAppendLEOLogStore) LEO() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.records) > 0 {
		return 0
	}
	return f.leo
}

func TestAppendRejectsReplicaThatIsNotLeader(t *testing.T) {
	r := newFollowerReplica(t)

	_, err := r.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestAppendWaitsUntilMinISRReplicasAcknowledgeViaFetch(t *testing.T) {
	env := newThreeReplicaCluster(t)
	done := make(chan CommitResult, 1)

	go func() {
		res, err := env.leader.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
		if err != nil {
			t.Errorf("Append() error = %v", err)
			return
		}
		done <- res
	}()
	waitForLogAppend(t, env.leader.log.(*fakeLogStore), 1)

	env.replicateOnce(t, env.follower2)
	if got := env.leader.progress[2]; got != 1 {
		t.Fatalf("progress[2] = %d", got)
	}
	if got := env.leader.state.HW; got != 0 {
		t.Fatalf("HW after follower2 = %d", got)
	}
	select {
	case <-done:
		t.Fatal("append returned before MinISR was satisfied")
	default:
	}

	env.replicateOnce(t, env.follower3)
	if got := env.leader.progress[3]; got != 1 {
		t.Fatalf("progress[3] = %d", got)
	}
	if got := env.leader.state.HW; got != 1 {
		t.Fatalf("HW after follower3 = %d", got)
	}
	res := <-done
	if res.NextCommitHW != 1 {
		t.Fatalf("NextCommitHW = %d", res.NextCommitHW)
	}
}

func TestLeaderLeaseExpiryFencesAppend(t *testing.T) {
	env := newTestEnv(t)
	meta := activeMeta(7, 1)
	meta.LeaseUntil = env.clock.Now().Add(time.Second)
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	env.clock.Advance(2 * time.Second)
	syncCount := env.log.syncCount

	_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if env.replica.state.Role != RoleFencedLeader {
		t.Fatalf("role = %v", env.replica.state.Role)
	}
	if env.log.syncCount != syncCount {
		t.Fatalf("syncCount = %d, want %d", env.log.syncCount, syncCount)
	}

	_, err = env.replica.Append(context.Background(), []Record{{Payload: []byte("y"), SizeBytes: 1}})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired on fenced leader, got %v", err)
	}
}

func TestAppendFailsFastWhenMetaISRBelowMinISR(t *testing.T) {
	env := newTestEnv(t)
	meta := activeMetaWithMinISR(7, 1, 3)
	meta.ISR = []NodeID{1, 3}

	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrInsufficientISR) {
		t.Fatalf("expected ErrInsufficientISR, got %v", err)
	}
}

func TestAppendCommitsUsingReturnedBaseOffsetWhenLEOReadLags(t *testing.T) {
	env := newTestEnv(t)

	logStore := &staleAppendLEOLogStore{
		fakeLogStore: &fakeLogStore{},
	}
	got, err := NewReplica(ReplicaConfig{
		LocalNode:         env.localNode,
		LogStore:          logStore,
		CheckpointStore:   env.checkpoints,
		EpochHistoryStore: env.history,
		SnapshotApplier:   env.snapshots,
		Now:               env.clock.Now,
	})
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}

	r, ok := got.(*replica)
	if !ok {
		t.Fatalf("NewReplica() type = %T", got)
	}
	meta := activeMetaWithMinISR(7, 1, 1)
	r.mustApplyMeta(t, meta)
	if err := r.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, err := r.Append(ctx, []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.BaseOffset != 0 || result.NextCommitHW != 1 {
		t.Fatalf("result = %+v", result)
	}
	if r.state.LEO != 1 || r.state.HW != 1 {
		t.Fatalf("state = %+v", r.state)
	}
}

func TestAppendDoesNotHoldReplicaLockAcrossLogSync(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.log.syncStarted = make(chan struct{}, 1)
	env.log.syncContinue = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := env.replica.Append(ctx, []Record{{Payload: []byte("x"), SizeBytes: 1}})
		done <- err
	}()

	<-env.log.syncStarted

	statusReady := make(chan struct{}, 1)
	go func() {
		_ = env.replica.Status()
		statusReady <- struct{}{}
	}()

	select {
	case <-statusReady:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked while Append log.Sync was in progress")
	}

	close(env.log.syncContinue)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation after sync, got %v", err)
	}
}

func TestAppendAllowsNewRequestsToQueueWhileLogSyncIsInProgress(t *testing.T) {
	env := newTestEnv(t)
	env.replica = newReplicaFromEnv(t, env)
	meta := activeMetaWithMinISR(7, 1, 1)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}
	env.log.syncStarted = make(chan struct{}, 1)
	env.log.syncContinue = make(chan struct{})

	firstDone := make(chan error, 1)
	go func() {
		_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("first"), SizeBytes: 5}})
		firstDone <- err
	}()

	<-env.log.syncStarted

	secondDone := make(chan error, 1)
	go func() {
		_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("second"), SizeBytes: 6}})
		secondDone <- err
	}()

	time.Sleep(10 * time.Millisecond)

	pendingReady := make(chan int, 1)
	go func() {
		env.replica.appendMu.Lock()
		pendingReady <- len(env.replica.appendPending)
		env.replica.appendMu.Unlock()
	}()

	select {
	case pending := <-pendingReady:
		if pending == 0 {
			t.Fatalf("appendPending = %d, want queued request while sync in progress", pending)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("appendPending inspection blocked while log.Sync was in progress")
	}

	close(env.log.syncContinue)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
}

func TestFetchSkipsUnsyncedLeaderRecordsWhileAppendSyncInProgress(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.log.syncStarted = make(chan struct{}, 1)
	env.log.syncContinue = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appendDone := make(chan error, 1)
	go func() {
		_, err := env.replica.Append(ctx, []Record{{Payload: []byte("x"), SizeBytes: 1}})
		appendDone <- err
	}()

	<-env.log.syncStarted

	type fetchResult struct {
		result FetchResult
		err    error
	}
	fetchDone := make(chan fetchResult, 1)
	go func() {
		result, err := env.replica.Fetch(context.Background(), FetchRequest{
			GroupKey:    "group-10",
			Epoch:       7,
			ReplicaID:   2,
			FetchOffset: env.replica.state.LEO,
			OffsetEpoch: 7,
			MaxBytes:    1024,
		})
		fetchDone <- fetchResult{result: result, err: err}
	}()

	select {
	case got := <-fetchDone:
		if got.err != nil {
			t.Fatalf("Fetch() error = %v", got.err)
		}
		if len(got.result.Records) != 0 {
			t.Fatalf("expected fetch to hide unsynced records, got %d", len(got.result.Records))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Fetch blocked while Append log.Sync was in progress")
	}

	close(env.log.syncContinue)
	cancel()
	if err := <-appendDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation after sync, got %v", err)
	}
}

func TestConcurrentAppendsShareSingleDurableAppend(t *testing.T) {
	env := newTestEnv(t)
	env.replica = newReplicaFromEnvWithGroupCommit(t, env, 20*time.Millisecond, 2, 1<<20)

	meta := activeMetaWithMinISR(7, 1, 1)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	type appendResult struct {
		result CommitResult
		err    error
	}
	firstDone := make(chan appendResult, 1)
	secondDone := make(chan appendResult, 1)

	go func() {
		result, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("first"), SizeBytes: 5}})
		firstDone <- appendResult{result: result, err: err}
	}()

	select {
	case got := <-firstDone:
		t.Fatalf("first append completed before group commit window: %+v %v", got.result, got.err)
	case <-time.After(2 * time.Millisecond):
	}

	go func() {
		result, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("second"), SizeBytes: 6}})
		secondDone <- appendResult{result: result, err: err}
	}()

	first := <-firstDone
	second := <-secondDone
	if first.err != nil {
		t.Fatalf("first Append() error = %v", first.err)
	}
	if second.err != nil {
		t.Fatalf("second Append() error = %v", second.err)
	}
	if first.result.BaseOffset != 0 {
		t.Fatalf("first BaseOffset = %d, want 0", first.result.BaseOffset)
	}
	if second.result.BaseOffset != 1 {
		t.Fatalf("second BaseOffset = %d, want 1", second.result.BaseOffset)
	}
	if env.log.appendCount != 1 {
		t.Fatalf("appendCount = %d, want 1", env.log.appendCount)
	}
	if env.log.syncCount != 1 {
		t.Fatalf("syncCount = %d, want 1", env.log.syncCount)
	}
}

func TestBatchedAppendRetainsCommitWaitSemantics(t *testing.T) {
	cluster := newThreeReplicaClusterWithGroupCommit(t, 20*time.Millisecond, 2, 1<<20)

	type appendResult struct {
		result CommitResult
		err    error
	}
	firstDone := make(chan appendResult, 1)
	secondDone := make(chan appendResult, 1)

	go func() {
		result, err := cluster.leader.Append(context.Background(), []Record{{Payload: []byte("first"), SizeBytes: 5}})
		firstDone <- appendResult{result: result, err: err}
	}()

	select {
	case got := <-firstDone:
		t.Fatalf("first append completed before second request joined batch: %+v %v", got.result, got.err)
	case <-time.After(2 * time.Millisecond):
	}

	go func() {
		result, err := cluster.leader.Append(context.Background(), []Record{{Payload: []byte("second"), SizeBytes: 6}})
		secondDone <- appendResult{result: result, err: err}
	}()

	waitForLogAppend(t, cluster.leader.log.(*fakeLogStore), 2)
	if cluster.leader.log.(*fakeLogStore).appendCount != 1 {
		t.Fatalf("appendCount = %d, want 1", cluster.leader.log.(*fakeLogStore).appendCount)
	}

	cluster.replicateOnce(t, cluster.follower2)
	select {
	case <-firstDone:
		t.Fatal("first append returned before MinISR was satisfied")
	default:
	}
	select {
	case <-secondDone:
		t.Fatal("second append returned before MinISR was satisfied")
	default:
	}

	cluster.replicateOnce(t, cluster.follower3)

	first := <-firstDone
	second := <-secondDone
	if first.err != nil {
		t.Fatalf("first Append() error = %v", first.err)
	}
	if second.err != nil {
		t.Fatalf("second Append() error = %v", second.err)
	}
	if first.result.BaseOffset != 0 || second.result.BaseOffset != 1 {
		t.Fatalf("batched offsets = %d,%d", first.result.BaseOffset, second.result.BaseOffset)
	}
	if first.result.NextCommitHW != 2 || second.result.NextCommitHW != 2 {
		t.Fatalf("batched NextCommitHW = %d,%d", first.result.NextCommitHW, second.result.NextCommitHW)
	}
}

func TestCanceledPendingAppendIsRemovedBeforeBatchFlush(t *testing.T) {
	env := newTestEnv(t)
	env.replica = newReplicaFromEnvWithGroupCommit(t, env, 20*time.Millisecond, 2, 1<<20)

	meta := activeMetaWithMinISR(7, 1, 1)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	type appendResult struct {
		result CommitResult
		err    error
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan appendResult, 1)
	secondDone := make(chan appendResult, 1)

	go func() {
		result, err := env.replica.Append(firstCtx, []Record{{Payload: []byte("first"), SizeBytes: 5}})
		firstDone <- appendResult{result: result, err: err}
	}()

	select {
	case got := <-firstDone:
		t.Fatalf("first append completed before cancellation: %+v %v", got.result, got.err)
	case <-time.After(2 * time.Millisecond):
	}

	cancelFirst()

	go func() {
		result, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("second"), SizeBytes: 6}})
		secondDone <- appendResult{result: result, err: err}
	}()

	first := <-firstDone
	second := <-secondDone
	if !errors.Is(first.err, context.Canceled) {
		t.Fatalf("first Append() error = %v, want context.Canceled", first.err)
	}
	if second.err != nil {
		t.Fatalf("second Append() error = %v", second.err)
	}
	if second.result.BaseOffset != 0 {
		t.Fatalf("second BaseOffset = %d, want 0", second.result.BaseOffset)
	}
	if env.log.appendCount != 1 {
		t.Fatalf("appendCount = %d, want 1", env.log.appendCount)
	}
	if len(env.log.records) != 1 {
		t.Fatalf("record count = %d, want 1", len(env.log.records))
	}
}

func TestNewReplicaAppliesDefaultGroupCommitConfig(t *testing.T) {
	r := newTestReplica(t)

	maxWait, maxRecords, maxBytes := replicaGroupCommitConfig(t, r)
	if maxWait != time.Millisecond {
		t.Fatalf("maxWait = %v, want %v", maxWait, time.Millisecond)
	}
	if maxRecords != 64 {
		t.Fatalf("maxRecords = %d, want 64", maxRecords)
	}
	if maxBytes != 64*1024 {
		t.Fatalf("maxBytes = %d, want %d", maxBytes, 64*1024)
	}
}

func newReplicaFromEnvWithGroupCommit(t testing.TB, env *testEnv, maxWait time.Duration, maxRecords, maxBytes int) *replica {
	t.Helper()

	cfg := env.config()
	setReplicaConfigFieldIfPresent(&cfg, "AppendGroupCommitMaxWait", maxWait)
	setReplicaConfigFieldIfPresent(&cfg, "AppendGroupCommitMaxRecords", maxRecords)
	setReplicaConfigFieldIfPresent(&cfg, "AppendGroupCommitMaxBytes", maxBytes)

	got, err := NewReplica(cfg)
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}
	r, ok := got.(*replica)
	if !ok {
		t.Fatalf("NewReplica() type = %T", got)
	}
	return r
}

func newThreeReplicaClusterWithGroupCommit(t testing.TB, maxWait time.Duration, maxRecords, maxBytes int) *threeReplicaCluster {
	t.Helper()

	meta := activeMetaWithMinISR(7, 1, 3)

	leaderEnv := newTestEnv(t)
	leaderEnv.replica = newReplicaFromEnvWithGroupCommit(t, leaderEnv, maxWait, maxRecords, maxBytes)
	leaderEnv.replica.mustApplyMeta(t, meta)
	if err := leaderEnv.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("leader BecomeLeader() error = %v", err)
	}

	follower2Env := newTestEnv(t)
	follower2Env.localNode = 2
	follower2Env.replica = newReplicaFromEnvWithGroupCommit(t, follower2Env, maxWait, maxRecords, maxBytes)
	follower2Env.replica.mustApplyMeta(t, meta)
	if err := follower2Env.replica.BecomeFollower(meta); err != nil {
		t.Fatalf("follower2 BecomeFollower() error = %v", err)
	}

	follower3Env := newTestEnv(t)
	follower3Env.localNode = 3
	follower3Env.replica = newReplicaFromEnvWithGroupCommit(t, follower3Env, maxWait, maxRecords, maxBytes)
	follower3Env.replica.mustApplyMeta(t, meta)
	if err := follower3Env.replica.BecomeFollower(meta); err != nil {
		t.Fatalf("follower3 BecomeFollower() error = %v", err)
	}

	return &threeReplicaCluster{
		leader:    leaderEnv.replica,
		follower2: follower2Env.replica,
		follower3: follower3Env.replica,
	}
}

func setReplicaConfigFieldIfPresent(cfg *ReplicaConfig, field string, value any) {
	rv := reflect.ValueOf(cfg).Elem()
	fv := rv.FieldByName(field)
	if !fv.IsValid() || !fv.CanSet() {
		return
	}
	valueOf := reflect.ValueOf(value)
	if valueOf.Type().AssignableTo(fv.Type()) {
		fv.Set(valueOf)
		return
	}
	if valueOf.Type().ConvertibleTo(fv.Type()) {
		fv.Set(valueOf.Convert(fv.Type()))
	}
}

func replicaGroupCommitConfig(t testing.TB, r *replica) (time.Duration, int, int) {
	t.Helper()

	rv := reflect.ValueOf(r).Elem()
	cfg := rv.FieldByName("appendGroupCommit")
	if !cfg.IsValid() {
		t.Fatalf("replica missing appendGroupCommit config field")
	}

	maxWaitField := cfg.FieldByName("maxWait")
	maxRecordsField := cfg.FieldByName("maxRecords")
	maxBytesField := cfg.FieldByName("maxBytes")
	if !maxWaitField.IsValid() || !maxRecordsField.IsValid() || !maxBytesField.IsValid() {
		t.Fatalf("replica appendGroupCommit fields are incomplete")
	}

	return time.Duration(maxWaitField.Int()), int(maxRecordsField.Int()), int(maxBytesField.Int())
}
