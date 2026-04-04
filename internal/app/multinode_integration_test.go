package app

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

const multinodeAppReadTimeout = 20 * time.Second

func TestThreeNodeAppGatewaySendUsesDurableCommit(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]

	key := channellog.ChannelKey{
		ChannelID:   "three-node-gateway-user",
		ChannelType: wkframe.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 15,
		LeaderEpoch:  6,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

	for _, app := range harness.appsWithLeaderFirst(leaderID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), key)
		require.NoError(t, err)
	}

	conn, err := net.Dial("tcp", leader.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sendAppWKProtoFrame(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "sender",
		DeviceID:        "sender-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	connack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*wkframe.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, connack.ReasonCode)

	sendAppWKProtoFrame(t, conn, &wkframe.SendPacket{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		ClientSeq:   1,
		ClientMsgNo: "three-node-app-1",
		Payload:     []byte("hello durable gateway"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*wkframe.SendackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)

	for _, app := range harness.orderedApps() {
		msg := waitForAppCommittedMessage(t, app.ChannelLogDB().ForChannel(key), sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello durable gateway"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}
}

type threeNodeAppHarness struct {
	apps map[uint64]*App
}

func newThreeNodeAppHarness(t *testing.T) *threeNodeAppHarness {
	t.Helper()

	clusterAddrs := reserveTestTCPAddrs(t, 3)
	gatewayAddrs := reserveTestTCPAddrs(t, 3)
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
		cfg.Node.DataDir = filepath.Join(root, fmt.Sprintf("node-%d", nodeID))
		cfg.Storage = StorageConfig{}
		cfg.Cluster.ListenAddr = clusterAddrs[nodeID]
		cfg.Cluster.Nodes = append([]NodeConfigRef(nil), clusterNodes...)
		cfg.Cluster.Groups = []GroupConfig{{
			ID:    1,
			Peers: []uint64{1, 2, 3},
		}}
		cfg.Cluster.GroupCount = 1
		cfg.Cluster.TickInterval = 10 * time.Millisecond
		cfg.Cluster.ElectionTick = 10
		cfg.Cluster.HeartbeatTick = 1
		cfg.Cluster.ForwardTimeout = 2 * time.Second
		cfg.Cluster.DialTimeout = 2 * time.Second
		cfg.Cluster.PoolSize = 1
		cfg.Gateway.Listeners = []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto", gatewayAddrs[nodeID]),
		}

		app, err := New(cfg)
		require.NoError(t, err)
		apps[nodeID] = app
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(apps))
	for _, app := range apps {
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

	harness := &threeNodeAppHarness{apps: apps}
	t.Cleanup(func() {
		for i := 3; i >= 1; i-- {
			if app := apps[uint64(i)]; app != nil {
				require.NoError(t, app.Stop())
			}
		}
	})
	return harness
}

func (h *threeNodeAppHarness) orderedApps() []*App {
	return []*App{h.apps[1], h.apps[2], h.apps[3]}
}

func (h *threeNodeAppHarness) appsWithLeaderFirst(leaderID uint64) []*App {
	apps := make([]*App, 0, len(h.apps))
	if leader := h.apps[leaderID]; leader != nil {
		apps = append(apps, leader)
	}
	for _, app := range h.orderedApps() {
		if app == nil || app.cfg.Node.ID == leaderID {
			continue
		}
		apps = append(apps, app)
	}
	return apps
}

func (h *threeNodeAppHarness) waitForStableLeader(t *testing.T, groupID uint64) uint64 {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var stable multiraft.NodeID
	stableCount := 0
	for time.Now().Before(deadline) {
		leader, ok := h.consensusLeader(multiraft.GroupID(groupID))
		if ok && leader != 0 {
			if leader == stable {
				stableCount++
			} else {
				stable = leader
				stableCount = 1
			}
			if stableCount >= 5 {
				return uint64(stable)
			}
		} else {
			stable = 0
			stableCount = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for stable leader for group %d", groupID)
	return 0
}

func (h *threeNodeAppHarness) consensusLeader(groupID multiraft.GroupID) (multiraft.NodeID, bool) {
	var leader multiraft.NodeID
	for _, app := range h.orderedApps() {
		current, err := app.Cluster().LeaderOf(groupID)
		if err != nil {
			return 0, false
		}
		if leader == 0 {
			leader = current
			continue
		}
		if current != leader {
			return 0, false
		}
	}
	return leader, true
}

func reserveTestTCPAddrs(t *testing.T, count int) map[uint64]string {
	t.Helper()

	addrs := make(map[uint64]string, count)
	for i := 0; i < count; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addrs[uint64(i+1)] = ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	return addrs
}

func readAppWKProtoFrameWithin(t *testing.T, conn net.Conn, timeout time.Duration) wkframe.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(timeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkframe.LatestVersion)
	require.NoError(t, err)
	return frame
}

func waitForAppCommittedMessage(t *testing.T, store *channellog.Store, seq uint64, timeout time.Duration) channellog.ChannelMessage {
	t.Helper()

	var msg channellog.ChannelMessage
	require.Eventually(t, func() bool {
		loaded, err := store.LoadMsg(seq)
		if err != nil {
			return false
		}
		msg = loaded
		return true
	}, timeout, 10*time.Millisecond)
	return msg
}
