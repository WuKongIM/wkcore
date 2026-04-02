package isr

import (
	"context"
	"testing"
)

type benchmarkRoundTripConfig struct {
	payloadBytes int
}

type benchmarkRoundTripHarness struct {
	t       testing.TB
	cfg     benchmarkRoundTripConfig
	cluster *threeReplicaCluster
}

func newBenchmarkRoundTripHarness(t testing.TB, cfg benchmarkRoundTripConfig) *benchmarkRoundTripHarness {
	t.Helper()

	return &benchmarkRoundTripHarness{
		t:       t,
		cfg:     cfg,
		cluster: newThreeReplicaCluster(t),
	}
}

func (h *benchmarkRoundTripHarness) runOnce(ctx context.Context) (CommitResult, error) {
	batch := makeBenchmarkRecords(1, h.cfg.payloadBytes)
	done := make(chan struct {
		res CommitResult
		err error
	}, 1)

	go func() {
		res, err := h.cluster.leader.Append(ctx, batch)
		done <- struct {
			res CommitResult
			err error
		}{res: res, err: err}
	}()

	waitForLogAppend(h.t, h.cluster.leader.log.(*fakeLogStore), 1)
	h.cluster.replicateOnce(h.t, h.cluster.follower2)
	h.cluster.replicateOnce(h.t, h.cluster.follower3)

	got := <-done
	return got.res, got.err
}

type benchmarkApplyFetchConfig struct {
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
		GroupID:  h.follower.state.GroupID,
		Epoch:    h.follower.state.Epoch,
		Leader:   1,
		Records:  makeBenchmarkRecords(1, h.cfg.payloadBytes),
		LeaderHW: 1,
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
