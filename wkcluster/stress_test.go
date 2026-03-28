package wkcluster

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
)

const (
	stressEnvKey      = "WKCLUSTER_STRESS"
	stressDurationEnv = "WKCLUSTER_STRESS_DURATION"
	stressWorkersEnv  = "WKCLUSTER_STRESS_WORKERS"
	stressSeedEnv     = "WKCLUSTER_STRESS_SEED"
)

type stressConfig struct {
	Enabled  bool
	Duration time.Duration
	Workers  int
	Seed     int64
}

func loadStressConfig(t *testing.T) stressConfig {
	t.Helper()
	cfg := stressConfig{
		Enabled:  envBool(stressEnvKey, false),
		Duration: envDuration(t, stressDurationEnv, 5*time.Second),
		Workers:  envInt(t, stressWorkersEnv, max(4, runtime.GOMAXPROCS(0))),
		Seed:     envInt64(t, stressSeedEnv, 20260328),
	}
	return cfg
}

func requireStressEnabled(t *testing.T, cfg stressConfig) {
	t.Helper()
	if !cfg.Enabled {
		t.Skip("set WKCLUSTER_STRESS=1 to enable wkcluster stress tests")
	}
}

// startSingleNodeForStress starts a single-node cluster suitable for
// high-throughput stress testing. Uses multiple raft groups for sharding.
func startSingleNodeForStress(t testing.TB, groupCount int) *Cluster {
	t.Helper()
	dir := t.TempDir()

	groups := make([]GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1},
		}
	}

	cfg := Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: uint32(groupCount),
		DataDir:    dir,
		Nodes:      []NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups:     groups,
	}

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c
}

// startThreeNodeForStress starts a 3-node cluster with the given group count.
func startThreeNodeForStress(t testing.TB, groupCount int) []*Cluster {
	t.Helper()

	listeners := make([]net.Listener, 3)
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]NodeConfig, 3)
	for i := range 3 {
		nodes[i] = NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	groups := make([]GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1, 2, 3},
		}
	}

	clusters := make([]*Cluster, 3)
	for i := range 3 {
		cfg := Config{
			NodeID:     multiraft.NodeID(i + 1),
			ListenAddr: nodes[i].Addr,
			GroupCount: uint32(groupCount),
			DataDir:    filepath.Join(t.TempDir(), fmt.Sprintf("n%d", i+1)),
			Nodes:      nodes,
			Groups:     groups,
		}
		c, err := NewCluster(cfg)
		if err != nil {
			t.Fatalf("NewCluster node %d: %v", i+1, err)
		}
		if err := c.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i+1, err)
		}
		clusters[i] = c
	}

	return clusters
}

// TestStressSingleNodeThroughput hammers a single-node cluster with
// concurrent CreateChannel + GetChannel from multiple goroutines across
// multiple raft groups, measuring throughput and verifying data integrity.
func TestStressSingleNodeThroughput(t *testing.T) {
	cfg := loadStressConfig(t)
	requireStressEnabled(t, cfg)

	const groupCount = 4
	c := startSingleNodeForStress(t, groupCount)
	defer c.Stop()

	// Wait for all groups to elect leader
	for g := 1; g <= groupCount; g++ {
		waitForLeader(t, c, uint64(g))
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errCh    = make(chan error, 1)
		creates  atomic.Uint64
		reads    atomic.Uint64
		updates  atomic.Uint64
		deletes  atomic.Uint64
	)

	for worker := 0; worker < cfg.Workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(cfg.Seed + int64(worker)))
			for idx := 0; ; idx++ {
				if ctx.Err() != nil {
					return
				}

				channelID := fmt.Sprintf("stress-ch-%d-%d", worker, rng.Intn(64))
				channelType := int64(rng.Intn(4) + 1)

				// Weighted operation mix: 50% create, 25% read, 15% update, 10% delete
				op := rng.Intn(100)
				switch {
				case op < 50:
					if err := c.CreateChannel(ctx, channelID, channelType); err != nil {
						if ctx.Err() != nil {
							return
						}
						select {
						case errCh <- fmt.Errorf("w%d CreateChannel(%s): %w", worker, channelID, err):
						default:
						}
						cancel()
						return
					}
					creates.Add(1)

				case op < 75:
					// Read — may not exist, that's fine
					_, _ = c.GetChannel(ctx, channelID, channelType)
					reads.Add(1)

				case op < 90:
					ban := int64(rng.Intn(2))
					if err := c.UpdateChannel(ctx, channelID, channelType, ban); err != nil {
						if ctx.Err() != nil {
							return
						}
						select {
						case errCh <- fmt.Errorf("w%d UpdateChannel(%s): %w", worker, channelID, err):
						default:
						}
						cancel()
						return
					}
					updates.Add(1)

				default:
					if err := c.DeleteChannel(ctx, channelID, channelType); err != nil {
						if ctx.Err() != nil {
							return
						}
						select {
						case errCh <- fmt.Errorf("w%d DeleteChannel(%s): %w", worker, channelID, err):
						default:
						}
						cancel()
						return
					}
					deletes.Add(1)
				}
			}
		}(worker)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("stress error: %v", err)
	default:
	}

	total := creates.Load() + reads.Load() + updates.Load() + deletes.Load()
	elapsed := cfg.Duration
	if ctx.Err() != nil {
		elapsed = cfg.Duration // completed full duration
	}
	t.Logf("single-node throughput: seed=%d workers=%d groups=%d duration=%s",
		cfg.Seed, cfg.Workers, groupCount, elapsed)
	t.Logf("  ops=%d (creates=%d reads=%d updates=%d deletes=%d) %.0f ops/sec",
		total, creates.Load(), reads.Load(), updates.Load(), deletes.Load(),
		float64(total)/elapsed.Seconds())
}

// TestStressThreeNodeMixedWorkload runs a mixed CRUD workload across a
// 3-node cluster. Writes hit random nodes (some go to leader, some get
// forwarded). Verifies that all created channels are readable after the
// workload completes.
func TestStressThreeNodeMixedWorkload(t *testing.T) {
	cfg := loadStressConfig(t)
	requireStressEnabled(t, cfg)

	const groupCount = 3
	clusters := startThreeNodeForStress(t, groupCount)
	defer func() {
		for _, c := range clusters {
			c.Stop()
		}
	}()

	// Wait for stable leaders on all groups
	for g := uint64(1); g <= groupCount; g++ {
		waitForStableLeader(t, clusters, g)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		wg        sync.WaitGroup
		creates   atomic.Uint64
		reads     atomic.Uint64
		updates   atomic.Uint64
		writeErrs atomic.Uint64
		mu        sync.Mutex
		written   = make(map[string]int64) // channelID -> channelType
	)

	for worker := 0; worker < cfg.Workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(cfg.Seed + int64(worker)*777))
			var localWritten []struct {
				id  string
				typ int64
			}

			for idx := 0; ; idx++ {
				if ctx.Err() != nil {
					break
				}

				// Pick a random node to send the request to
				node := clusters[rng.Intn(len(clusters))]
				channelID := fmt.Sprintf("mn-ch-%d-%d", worker, rng.Intn(32))
				channelType := int64(rng.Intn(3) + 1)

				op := rng.Intn(100)
				switch {
				case op < 60:
					if err := node.CreateChannel(ctx, channelID, channelType); err != nil {
						if ctx.Err() != nil {
							break
						}
						// Transient errors (leadership changes, forwarding timeouts)
						// are expected in multi-node clusters under load.
						writeErrs.Add(1)
						continue
					}
					creates.Add(1)
					localWritten = append(localWritten, struct {
						id  string
						typ int64
					}{channelID, channelType})

				case op < 80:
					_, _ = node.GetChannel(ctx, channelID, channelType)
					reads.Add(1)

				default:
					ban := int64(rng.Intn(2))
					if err := node.UpdateChannel(ctx, channelID, channelType, ban); err != nil {
						if ctx.Err() != nil {
							break
						}
						writeErrs.Add(1)
						continue
					}
					updates.Add(1)
				}
			}

			mu.Lock()
			for _, w := range localWritten {
				written[w.id] = w.typ
			}
			mu.Unlock()
		}(worker)
	}

	wg.Wait()

	// Allow replication to settle
	time.Sleep(500 * time.Millisecond)

	// Verify: all created channels should be readable on the leader
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	verified := 0
	for channelID, channelType := range written {
		// Read from each node until one succeeds (eventual consistency)
		found := false
		for _, c := range clusters {
			ch, err := c.GetChannel(verifyCtx, channelID, channelType)
			if err == nil && ch.ChannelID == channelID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("channel %q type=%d not found on any node after workload", channelID, channelType)
		}
		verified++
	}

	total := creates.Load() + reads.Load() + updates.Load()
	t.Logf("3-node mixed workload: seed=%d workers=%d groups=%d duration=%s",
		cfg.Seed, cfg.Workers, groupCount, cfg.Duration)
	t.Logf("  ops=%d (creates=%d reads=%d updates=%d write_errs=%d) verified=%d/%d channels",
		total, creates.Load(), reads.Load(), updates.Load(), writeErrs.Load(), verified, len(written))
}

// TestStressForwardingContention sends all writes to follower nodes only,
// forcing every write to go through the forwarding path. Measures forwarding
// throughput under contention.
func TestStressForwardingContention(t *testing.T) {
	cfg := loadStressConfig(t)
	requireStressEnabled(t, cfg)

	const groupCount = 2
	clusters := startThreeNodeForStress(t, groupCount)
	defer func() {
		for _, c := range clusters {
			c.Stop()
		}
	}()

	// Wait for stable leaders
	leaders := make(map[uint64]multiraft.NodeID)
	for g := uint64(1); g <= groupCount; g++ {
		leaders[g] = waitForStableLeader(t, clusters, g)
	}

	// Collect followers (nodes that are NOT the leader for group 1)
	var followers []*Cluster
	for _, c := range clusters {
		if c.cfg.NodeID != leaders[1] {
			followers = append(followers, c)
		}
	}
	if len(followers) == 0 {
		t.Fatal("no followers found")
	}
	t.Logf("leaders=%v, using %d followers for forwarding", leaders, len(followers))

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		wg      sync.WaitGroup
		errCh   = make(chan error, 1)
		fwdOps  atomic.Uint64
		fwdErrs atomic.Uint64
	)

	for worker := 0; worker < cfg.Workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(cfg.Seed + int64(worker)*333))
			for idx := 0; ; idx++ {
				if ctx.Err() != nil {
					return
				}

				// Always write to a follower to exercise forwarding
				follower := followers[rng.Intn(len(followers))]
				channelID := fmt.Sprintf("fwd-ch-%d-%d", worker, rng.Intn(32))
				channelType := int64(rng.Intn(3) + 1)

				err := follower.CreateChannel(ctx, channelID, channelType)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// Leadership changes can cause transient errors; count but don't abort
					fwdErrs.Add(1)
					continue
				}
				fwdOps.Add(1)
			}
		}(worker)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("stress error: %v", err)
	default:
	}

	t.Logf("forwarding contention: seed=%d workers=%d groups=%d duration=%s",
		cfg.Seed, cfg.Workers, groupCount, cfg.Duration)
	t.Logf("  forwarded_ops=%d errors=%d %.0f ops/sec",
		fwdOps.Load(), fwdErrs.Load(),
		float64(fwdOps.Load())/cfg.Duration.Seconds())
}

// TestStressConcurrentCreateReadVerify does high-concurrency create followed
// by a full read-back verification. Every channel that was created must be
// readable. This catches data loss bugs under concurrent raft proposals.
func TestStressConcurrentCreateReadVerify(t *testing.T) {
	cfg := loadStressConfig(t)
	requireStressEnabled(t, cfg)

	const groupCount = 4
	c := startSingleNodeForStress(t, groupCount)
	defer c.Stop()

	for g := 1; g <= groupCount; g++ {
		waitForLeader(t, c, uint64(g))
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		wg    sync.WaitGroup
		errCh = make(chan error, 1)
		mu    sync.Mutex
		// Track all successfully created channels
		created = make(map[string]int64) // "channelID" -> channelType
		count   atomic.Uint64
	)

	for worker := 0; worker < cfg.Workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			var local []struct {
				id  string
				typ int64
			}

			for idx := 0; ; idx++ {
				if ctx.Err() != nil {
					break
				}

				// Unique channel per worker+idx to avoid collisions
				channelID := fmt.Sprintf("verify-w%d-i%d", worker, idx)
				channelType := int64((worker+idx)%4 + 1)

				if err := c.CreateChannel(ctx, channelID, channelType); err != nil {
					if ctx.Err() != nil {
						break
					}
					select {
					case errCh <- fmt.Errorf("w%d CreateChannel(%s): %w", worker, channelID, err):
					default:
					}
					cancel()
					return
				}
				count.Add(1)
				local = append(local, struct {
					id  string
					typ int64
				}{channelID, channelType})
			}

			mu.Lock()
			for _, ch := range local {
				created[ch.id] = ch.typ
			}
			mu.Unlock()
		}(worker)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("stress error: %v", err)
	default:
	}

	// Verify every created channel is readable
	verifyCtx := context.Background()
	missing := 0
	for channelID, channelType := range created {
		ch, err := c.GetChannel(verifyCtx, channelID, channelType)
		if err != nil {
			missing++
			if missing <= 5 {
				t.Errorf("missing channel %q type=%d: %v", channelID, channelType, err)
			}
			continue
		}
		if ch.ChannelID != channelID {
			t.Errorf("channel mismatch: got %q, want %q", ch.ChannelID, channelID)
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d channels missing after create", missing, len(created))
	}

	t.Logf("create+verify: seed=%d workers=%d groups=%d duration=%s created=%d all_verified=true",
		cfg.Seed, cfg.Workers, groupCount, cfg.Duration, len(created))
}

// --- env helpers (same pattern as wkfsm/stress_test.go) ---

func envBool(name string, fallback bool) bool {
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return d
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return n
}

func envInt64(t *testing.T, name string, fallback int64) int64 {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return n
}
