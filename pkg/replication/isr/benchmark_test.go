package isr

import (
	"context"
	"testing"
)

type benchmarkRoundTripConfig struct {
	batchSize      int
	payloadBytes   int
	reserveRecords int
}

type benchmarkRoundTripHarness struct {
	t       testing.TB
	cfg     benchmarkRoundTripConfig
	cluster *threeReplicaCluster
	batch   []Record
}

func newBenchmarkRoundTripHarness(t testing.TB, cfg benchmarkRoundTripConfig) *benchmarkRoundTripHarness {
	t.Helper()

	batchSize := cfg.batchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	return &benchmarkRoundTripHarness{
		t:       t,
		cfg:     cfg,
		cluster: newThreeReplicaCluster(t),
		batch:   makeBenchmarkRecords(batchSize, cfg.payloadBytes),
	}
}

func (h *benchmarkRoundTripHarness) rebuild() {
	h.t.Helper()
	h.cluster = newThreeReplicaCluster(h.t)
	reserveRecords := h.cfg.reserveRecords
	if reserveRecords < len(h.batch) {
		reserveRecords = len(h.batch)
	}
	// Reserve enough capacity for a whole reset window so each window keeps a
	// similar append/replication shape instead of repeatedly paying slice growth.
	h.cluster.leader.log.(*fakeLogStore).records = make([]Record, 0, reserveRecords)
	h.cluster.follower2.log.(*fakeLogStore).records = make([]Record, 0, reserveRecords)
	h.cluster.follower3.log.(*fakeLogStore).records = make([]Record, 0, reserveRecords)
}

func (h *benchmarkRoundTripHarness) runOnce(ctx context.Context) (CommitResult, error) {
	done := make(chan struct {
		res CommitResult
		err error
	}, 1)

	startLEO := h.cluster.leader.log.(*fakeLogStore).LEO()
	wantLEO := startLEO + uint64(len(h.batch))

	go func() {
		res, err := h.cluster.leader.Append(ctx, h.batch)
		done <- struct {
			res CommitResult
			err error
		}{res: res, err: err}
	}()

	waitForLogAppend(h.t, h.cluster.leader.log.(*fakeLogStore), wantLEO)
	batchSize := len(h.batch)
	replicaMaxBytes := batchSize * h.cfg.payloadBytes
	if replicaMaxBytes <= 0 {
		replicaMaxBytes = 1
	}
	h.replicateFollower(ctx, h.cluster.follower2, replicaMaxBytes)
	h.replicateFollower(ctx, h.cluster.follower3, replicaMaxBytes)

	got := <-done
	return got.res, got.err
}

func (h *benchmarkRoundTripHarness) replicateFollower(ctx context.Context, follower *replica, maxBytes int) {
	req := FetchRequest{
		GroupKey:    h.cluster.leader.state.GroupKey,
		Epoch:       h.cluster.leader.state.Epoch,
		ReplicaID:   follower.localNode,
		FetchOffset: follower.state.LEO,
		OffsetEpoch: follower.state.Epoch,
		MaxBytes:    maxBytes,
	}
	result, err := h.cluster.leader.Fetch(ctx, req)
	if err != nil {
		h.t.Fatalf("Fetch() error = %v", err)
	}
	if err := follower.ApplyFetch(ctx, ApplyFetchRequest{
		GroupKey:   req.GroupKey,
		Epoch:      result.Epoch,
		Leader:     h.cluster.leader.localNode,
		TruncateTo: result.TruncateTo,
		Records:    result.Records,
		LeaderHW:   result.HW,
	}); err != nil {
		h.t.Fatalf("ApplyFetch() error = %v", err)
	}
	_, err = h.cluster.leader.Fetch(ctx, FetchRequest{
		GroupKey:    req.GroupKey,
		Epoch:       result.Epoch,
		ReplicaID:   follower.localNode,
		FetchOffset: follower.state.LEO,
		OffsetEpoch: follower.state.Epoch,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		h.t.Fatalf("ack Fetch() error = %v", err)
	}
}

type benchmarkFetchConfig struct {
	backlogRecords int
	maxBytes       int
	payloadBytes   int
}

type benchmarkFetchHarness struct {
	t         testing.TB
	cfg       benchmarkFetchConfig
	leaderEnv *testEnv
	leader    *replica
	req       FetchRequest
}

func newBenchmarkFetchHarness(t testing.TB, cfg benchmarkFetchConfig) *benchmarkFetchHarness {
	t.Helper()

	h := &benchmarkFetchHarness{
		t:   t,
		cfg: cfg,
	}
	h.rebuild()
	return h
}

func (h *benchmarkFetchHarness) rebuild() {
	h.t.Helper()

	h.leaderEnv = newTestEnv(h.t)
	h.leaderEnv.log.records = makeBenchmarkRecords(h.cfg.backlogRecords, h.cfg.payloadBytes)
	h.leaderEnv.log.leo = uint64(len(h.leaderEnv.log.records))
	h.leaderEnv.checkpoints.loadErr = nil
	h.leaderEnv.checkpoints.checkpoint = Checkpoint{
		Epoch:          3,
		LogStartOffset: 0,
		HW:             uint64(len(h.leaderEnv.log.records)),
	}
	h.leaderEnv.history.loadErr = nil
	h.leaderEnv.history.points = []EpochPoint{{Epoch: 3, StartOffset: 0}}
	h.leaderEnv.replica = newReplicaFromEnv(h.t, h.leaderEnv)
	h.leader = h.leaderEnv.replica

	meta := activeMeta(7, 1)
	h.leader.mustApplyMeta(h.t, meta)
	if err := h.leader.BecomeLeader(meta); err != nil {
		h.t.Fatalf("BecomeLeader() error = %v", err)
	}

	h.req = FetchRequest{
		GroupKey:    h.leader.state.GroupKey,
		Epoch:       h.leader.state.Epoch,
		ReplicaID:   2,
		FetchOffset: 0,
		OffsetEpoch: h.leader.state.Epoch,
		MaxBytes:    h.cfg.maxBytes,
	}
}

func (h *benchmarkFetchHarness) fetchOnce(ctx context.Context) (FetchResult, error) {
	return h.leader.Fetch(ctx, h.req)
}

type benchmarkApplyMode uint8

const (
	benchmarkApplyModeAppendOnly benchmarkApplyMode = iota
	benchmarkApplyModeTruncateAppend
)

type benchmarkApplyFetchConfig struct {
	mode         benchmarkApplyMode
	payloadBytes int
}

type benchmarkApplyFetchHarness struct {
	t           testing.TB
	cfg         benchmarkApplyFetchConfig
	follower    *replica
	followerEnv *testEnv
	req         ApplyFetchRequest
}

func newBenchmarkApplyFetchHarness(t testing.TB, cfg benchmarkApplyFetchConfig) *benchmarkApplyFetchHarness {
	t.Helper()

	h := &benchmarkApplyFetchHarness{
		t:   t,
		cfg: cfg,
	}
	h.rebuild()
	return h
}

func (h *benchmarkApplyFetchHarness) applyOnce(ctx context.Context) error {
	return h.follower.ApplyFetch(ctx, h.req)
}

func (h *benchmarkApplyFetchHarness) rebuild() {
	h.t.Helper()

	h.followerEnv = newFollowerEnv(h.t)
	h.follower = h.followerEnv.replica
	h.req = ApplyFetchRequest{
		GroupKey: h.follower.state.GroupKey,
		Epoch:    h.follower.state.Epoch,
		Leader:   1,
		Records:  makeBenchmarkRecords(1, h.cfg.payloadBytes),
		LeaderHW: 1,
	}

	switch h.cfg.mode {
	case benchmarkApplyModeTruncateAppend:
		h.followerEnv.log.records = makeBenchmarkRecords(2, h.cfg.payloadBytes)
		h.followerEnv.log.leo = 2
		h.followerEnv.checkpoints.checkpoint.HW = 1
		h.follower.state.HW = 1
		h.follower.state.LEO = 2
		truncateTo := uint64(1)
		h.req.TruncateTo = &truncateTo
		h.req.LeaderHW = 2
	case benchmarkApplyModeAppendOnly:
		fallthrough
	default:
		h.req.TruncateTo = nil
	}
}

func makeBenchmarkRecords(count, payloadBytes int) []Record {
	if count <= 0 {
		return nil
	}
	records := make([]Record, count)
	payload := make([]byte, payloadBytes)
	for i := range records {
		records[i] = Record{
			Payload:   append([]byte(nil), payload...),
			SizeBytes: payloadBytes,
		}
	}
	return records
}

func TestBenchmarkRoundTripHelperCommitsAppend(t *testing.T) {
	harness := newBenchmarkRoundTripHarness(t, benchmarkRoundTripConfig{
		payloadBytes: 128,
	})

	result, err := harness.runOnce(context.Background())
	if err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}
	if result.NextCommitHW != 1 {
		t.Fatalf("NextCommitHW = %d", result.NextCommitHW)
	}
	if got := harness.cluster.follower2.state.LEO; got != 1 {
		t.Fatalf("follower2 LEO = %d", got)
	}
	if got := harness.cluster.follower3.state.LEO; got != 1 {
		t.Fatalf("follower3 LEO = %d", got)
	}
}

func TestBenchmarkApplyFetchRebuildResetsFollowerState(t *testing.T) {
	harness := newBenchmarkApplyFetchHarness(t, benchmarkApplyFetchConfig{
		payloadBytes: 128,
	})

	if err := harness.applyOnce(context.Background()); err != nil {
		t.Fatalf("applyOnce() error = %v", err)
	}
	if got := harness.follower.state.LEO; got == 0 {
		t.Fatalf("follower LEO = %d, want > 0 before rebuild", got)
	}

	harness.rebuild()
	if got := harness.follower.state.LEO; got != 0 {
		t.Fatalf("follower LEO = %d, want 0 after rebuild", got)
	}
}

func TestBenchmarkFetchFixtureUsesBacklogAndOffsetZero(t *testing.T) {
	harness := newBenchmarkFetchHarness(t, benchmarkFetchConfig{
		backlogRecords: 32,
		maxBytes:       4096,
		payloadBytes:   128,
	})

	if got := harness.req.FetchOffset; got != 0 {
		t.Fatalf("FetchOffset = %d, want 0", got)
	}
	if got := harness.req.MaxBytes; got != 4096 {
		t.Fatalf("MaxBytes = %d, want 4096", got)
	}
	if got := harness.leader.state.LEO; got != 32 {
		t.Fatalf("leader LEO = %d, want 32", got)
	}
}

func TestBenchmarkTruncateApplyFixtureStartsWithDirtyTail(t *testing.T) {
	harness := newBenchmarkApplyFetchHarness(t, benchmarkApplyFetchConfig{
		mode:         benchmarkApplyModeTruncateAppend,
		payloadBytes: 128,
	})

	if got := harness.follower.state.LEO; got <= harness.follower.state.HW {
		t.Fatalf("LEO = %d, HW = %d, want dirty tail", got, harness.follower.state.HW)
	}
	if harness.req.TruncateTo == nil {
		t.Fatal("TruncateTo = nil, want truncate path")
	}
}

func TestBenchmarkRecoveryFixturesMatchSpec(t *testing.T) {
	cases := []struct {
		name    string
		state   string
		wantLEO uint64
		wantHW  uint64
		wantErr error
	}{
		{name: "state=empty", state: "empty", wantErr: ErrEmptyState},
		{name: "state=clean_checkpoint", state: "clean_checkpoint", wantLEO: 64, wantHW: 64},
		{name: "state=dirty_tail", state: "dirty_tail", wantLEO: 96, wantHW: 64},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fixture := newBenchmarkRecoveryFixture(t, tc.state)
			if got := fixture.log.LEO(); got != tc.wantLEO {
				t.Fatalf("log LEO = %d, want %d", got, tc.wantLEO)
			}
			if got := fixture.checkpoints.checkpoint.HW; got != tc.wantHW {
				t.Fatalf("checkpoint HW = %d, want %d", got, tc.wantHW)
			}
			if got := fixture.checkpoints.loadErr; got != tc.wantErr {
				t.Fatalf("checkpoint loadErr = %v, want %v", got, tc.wantErr)
			}
			switch tc.state {
			case "empty":
				if got := fixture.history.loadErr; got != ErrEmptyState {
					t.Fatalf("history loadErr = %v, want %v", got, ErrEmptyState)
				}
			case "clean_checkpoint", "dirty_tail":
				if got := fixture.history.loadErr; got != nil {
					t.Fatalf("history loadErr = %v, want nil", got)
				}
				wantPoints := []EpochPoint{{Epoch: 3, StartOffset: 0}}
				if got := fixture.history.points; len(got) != 1 || got[0] != wantPoints[0] {
					t.Fatalf("history points = %+v, want %+v", got, wantPoints)
				}
			}
		})
	}
}

type benchmarkRecoveryConfig struct {
	state string
}

type benchmarkRecoveryHarness struct {
	t   testing.TB
	cfg benchmarkRecoveryConfig
	env *testEnv
}

func newBenchmarkRecoveryHarness(t testing.TB, cfg benchmarkRecoveryConfig) *benchmarkRecoveryHarness {
	t.Helper()

	h := &benchmarkRecoveryHarness{
		t:   t,
		cfg: cfg,
	}
	h.rebuild()
	return h
}

func newBenchmarkRecoveryFixture(t testing.TB, state string) *testEnv {
	t.Helper()

	env := newTestEnv(t)
	switch state {
	case "empty":
		// Default empty fixture.
	case "clean_checkpoint":
		env.log.records = makeBenchmarkRecords(64, 128)
		env.log.leo = 64
		env.checkpoints.loadErr = nil
		env.checkpoints.checkpoint = Checkpoint{
			Epoch:          3,
			LogStartOffset: 0,
			HW:             64,
		}
		env.history.loadErr = nil
		env.history.points = []EpochPoint{{Epoch: 3, StartOffset: 0}}
	case "dirty_tail":
		env.log.records = makeBenchmarkRecords(96, 128)
		env.log.leo = 96
		env.checkpoints.loadErr = nil
		env.checkpoints.checkpoint = Checkpoint{
			Epoch:          3,
			LogStartOffset: 0,
			HW:             64,
		}
		env.history.loadErr = nil
		env.history.points = []EpochPoint{{Epoch: 3, StartOffset: 0}}
	default:
		t.Fatalf("unknown benchmark recovery state %q", state)
	}
	return env
}

func (h *benchmarkRecoveryHarness) rebuild() {
	h.t.Helper()

	h.env = newBenchmarkRecoveryFixture(h.t, h.cfg.state)
}

func (h *benchmarkRecoveryHarness) recoverOnce() error {
	_, err := NewReplica(h.env.config())
	return err
}

// Reset intervals are benchmark tuning knobs: they keep the fixture bounded
// and the workload shape comparable inside each window, but they are not
// correctness requirements.
const (
	benchmarkReplicaAppendResetAfterOps          = 256
	benchmarkReplicaFetchResetAfterOps           = 0
	benchmarkReplicaApplyFetchAppendOnlyResetOps = 256
	benchmarkReplicaApplyFetchTruncateResetOps   = 1
	benchmarkRoundTripResetAfterOps              = 128
)

func runBenchmarkWithResetPolicy(b *testing.B, resetAfterOps int, rebuild func(), op func() error) {
	for i := 0; i < b.N; i++ {
		if resetAfterOps > 0 && i > 0 && i%resetAfterOps == 0 {
			b.StopTimer()
			rebuild()
			b.StartTimer()
		}
		if err := op(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicaAppend(b *testing.B) {
	cases := []struct {
		name       string
		batchSize  int
		payloadLen int
	}{
		{name: "batch=1/payload=128", batchSize: 1, payloadLen: 128},
		{name: "batch=16/payload=128", batchSize: 16, payloadLen: 128},
		{name: "batch=16/payload=1024", batchSize: 16, payloadLen: 1024},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			harness := newBenchmarkRoundTripHarness(b, benchmarkRoundTripConfig{
				batchSize:      tc.batchSize,
				payloadBytes:   tc.payloadLen,
				reserveRecords: tc.batchSize * benchmarkReplicaAppendResetAfterOps,
			})
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			runBenchmarkWithResetPolicy(b, benchmarkReplicaAppendResetAfterOps, func() {
				harness.rebuild()
			}, func() error {
				_, err := harness.runOnce(ctx)
				return err
			})
		})
	}
}

func BenchmarkReplicaFetch(b *testing.B) {
	cases := []struct {
		name          string
		maxBytes      int
		backlog       int
		payloadLength int
	}{
		{name: "max_bytes=4096/backlog=32", maxBytes: 4096, backlog: 32, payloadLength: 128},
		{name: "max_bytes=65536/backlog=256", maxBytes: 65536, backlog: 256, payloadLength: 128},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			harness := newBenchmarkFetchHarness(b, benchmarkFetchConfig{
				backlogRecords: tc.backlog,
				maxBytes:       tc.maxBytes,
				payloadBytes:   tc.payloadLength,
			})
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			runBenchmarkWithResetPolicy(b, benchmarkReplicaFetchResetAfterOps, func() {
				harness.rebuild()
			}, func() error {
				_, err := harness.fetchOnce(ctx)
				return err
			})
		})
	}
}

func BenchmarkReplicaApplyFetch(b *testing.B) {
	cases := []struct {
		name         string
		mode         benchmarkApplyMode
		resetAfterOp int
	}{
		{name: "mode=append_only", mode: benchmarkApplyModeAppendOnly, resetAfterOp: benchmarkReplicaApplyFetchAppendOnlyResetOps},
		{name: "mode=truncate_append", mode: benchmarkApplyModeTruncateAppend, resetAfterOp: benchmarkReplicaApplyFetchTruncateResetOps},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			harness := newBenchmarkApplyFetchHarness(b, benchmarkApplyFetchConfig{
				mode:         tc.mode,
				payloadBytes: 128,
			})
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			runBenchmarkWithResetPolicy(b, tc.resetAfterOp, func() {
				harness.rebuild()
			}, func() error {
				return harness.applyOnce(ctx)
			})
		})
	}
}

func BenchmarkNewReplicaRecovery(b *testing.B) {
	cases := []struct {
		name         string
		state        string
		resetAfterOp int
	}{
		{name: "state=empty", state: "empty", resetAfterOp: 0},
		{name: "state=clean_checkpoint", state: "clean_checkpoint", resetAfterOp: 0},
		{name: "state=dirty_tail", state: "dirty_tail", resetAfterOp: 1},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			harness := newBenchmarkRecoveryHarness(b, benchmarkRecoveryConfig{state: tc.state})
			b.ReportAllocs()
			b.ResetTimer()
			runBenchmarkWithResetPolicy(b, tc.resetAfterOp, func() {
				harness.rebuild()
			}, func() error {
				return harness.recoverOnce()
			})
		})
	}
}

func BenchmarkThreeReplicaReplicationRoundTrip(b *testing.B) {
	cases := []struct {
		name         string
		batchSize    int
		payloadBytes int
	}{
		{name: "batch=1/payload=128", batchSize: 1, payloadBytes: 128},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			harness := newBenchmarkRoundTripHarness(b, benchmarkRoundTripConfig{
				batchSize:      tc.batchSize,
				payloadBytes:   tc.payloadBytes,
				reserveRecords: tc.batchSize * benchmarkRoundTripResetAfterOps,
			})
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			runBenchmarkWithResetPolicy(b, benchmarkRoundTripResetAfterOps, func() {
				harness.rebuild()
			}, func() error {
				_, err := harness.runOnce(ctx)
				return err
			})
		})
	}
}
