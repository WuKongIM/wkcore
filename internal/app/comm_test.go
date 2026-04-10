package app

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/stretchr/testify/require"
)

func envBool(name string, fallback bool) bool {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
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

	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return parsed
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()

	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return parsed
}

func envInt64(t *testing.T, name string, fallback int64) int64 {
	t.Helper()

	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return parsed
}

func newThreeNodeConversationSyncHarness(t *testing.T) *threeNodeAppHarness {
	t.Helper()

	clusterAddrs := reserveTestTCPAddrs(t, 3)
	gatewayAddrs := reserveTestTCPAddrs(t, 3)
	apiAddrs := reserveTestTCPAddrs(t, 3)
	clusterNodes := make([]NodeConfigRef, 0, 3)
	for i := 0; i < 3; i++ {
		clusterNodes = append(clusterNodes, NodeConfigRef{
			ID:   uint64(i + 1),
			Addr: clusterAddrs[uint64(i+1)],
		})
	}

	root := t.TempDir()
	apps := make(map[uint64]*App, 3)
	for i := 0; i < 3; i++ {
		nodeID := uint64(i + 1)
		cfg := validConfig()
		cfg.Node.ID = nodeID
		cfg.Node.Name = fmt.Sprintf("node-%d", nodeID)
		cfg.Node.DataDir = fmt.Sprintf("%s/node-%d", root, nodeID)
		cfg.Storage = StorageConfig{}
		cfg.Cluster.ListenAddr = clusterAddrs[nodeID]
		cfg.Cluster.Nodes = append([]NodeConfigRef(nil), clusterNodes...)
		cfg.Cluster.SlotCount = 1
		cfg.Cluster.ControllerReplicaN = 3
		cfg.Cluster.SlotReplicaN = 3
		cfg.Cluster.Slots = nil
		cfg.Cluster.TickInterval = 10 * time.Millisecond
		cfg.Cluster.ElectionTick = 10
		cfg.Cluster.HeartbeatTick = 1
		cfg.Cluster.ForwardTimeout = 2 * time.Second
		cfg.Cluster.DialTimeout = 2 * time.Second
		cfg.Cluster.PoolSize = 1
		cfg.API.ListenAddr = apiAddrs[nodeID]
		cfg.Conversation.SyncEnabled = true
		cfg.Gateway.Listeners = []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto", gatewayAddrs[nodeID]),
		}

		app, err := New(cfg)
		require.NoError(t, err)
		apps[nodeID] = app
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(apps))
	for _, app := range []*App{apps[1], apps[2], apps[3]} {
		wg.Add(1)
		go func(app *App) {
			defer wg.Done()
			errCh <- app.Start()
		}(app)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, app := range []*App{apps[3], apps[2], apps[1]} {
			require.NoError(t, app.Stop())
		}
	})
	return &threeNodeAppHarness{apps: apps}
}
