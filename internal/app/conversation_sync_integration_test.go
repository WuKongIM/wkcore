package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/stretchr/testify/require"
)

func TestAppConversationSyncReturnsLegacyConversationAfterSend(t *testing.T) {
	cfg := testConfig(t)
	cfg.API.ListenAddr = "127.0.0.1:0"
	cfg.Conversation.SyncEnabled = true

	app, err := New(cfg)
	require.NoError(t, err)
	channelID := deliveryusecase.EncodePersonChannel("u1", "u2")
	seedChannelRuntimeMeta(t, app, channelID, frame.ChannelTypePerson)

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	_, err = app.Message().Send(context.Background(), message.SendCommand{
		FromUID:     "u1",
		ChannelID:   "u2",
		ChannelType: frame.ChannelTypePerson,
		ClientMsgNo: "conversation-sync-1",
		Payload:     []byte("hello sync"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversation/sync", bytes.NewBufferString(`{"uid":"u2","limit":10}`))
		req.Header.Set("Content-Type", "application/json")

		app.API().Engine().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}

		var got []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			return false
		}
		if len(got) != 1 {
			return false
		}
		return got[0]["channel_id"] == "u1" &&
			got[0]["last_client_msg_no"] == "conversation-sync-1" &&
			got[0]["last_msg_seq"] == float64(1) &&
			got[0]["unread"] == float64(1)
	}, 3*time.Second, 20*time.Millisecond)
}

func TestConversationSyncLoadsFactsFromRemoteOwnerWhenAPINodeIsNotReplica(t *testing.T) {
	harness := newThreeNodeConversationSyncHarness(t)
	groupLeaderID := harness.waitForStableLeader(t, 1)
	groupLeader := harness.apps[groupLeaderID]
	apiNode := harness.apps[1]

	senderUID := "remote-owner-sender"
	recipientUID := "remote-owner-recipient"
	channelID := deliveryusecase.EncodePersonChannel(senderUID, recipientUID)
	key := channellog.ChannelKey{
		ChannelID:   channelID,
		ChannelType: frame.ChannelTypePerson,
	}

	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 15,
		LeaderEpoch:  6,
		Replicas:     []uint64{2, 3},
		ISR:          []uint64{2, 3},
		Leader:       2,
		MinISR:       1,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, groupLeader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, nodeID := range []uint64{2, 3} {
		_, err := harness.apps[nodeID].channelMetaSync.RefreshChannelMeta(context.Background(), key)
		require.NoError(t, err)
	}

	_, err := harness.apps[2].Message().Send(context.Background(), message.SendCommand{
		FromUID:     senderUID,
		ChannelID:   recipientUID,
		ChannelType: frame.ChannelTypePerson,
		ClientMsgNo: "conversation-sync-remote-1",
		Payload:     []byte("hello remote sync"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversation/sync", bytes.NewBufferString(fmt.Sprintf(`{"uid":"%s","limit":10,"msg_count":1}`, recipientUID)))
		req.Header.Set("Content-Type", "application/json")

		apiNode.API().Engine().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}

		var got []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			return false
		}
		if len(got) != 1 {
			return false
		}
		recents, ok := got[0]["recents"].([]any)
		if !ok || len(recents) != 1 {
			return false
		}
		recent0, ok := recents[0].(map[string]any)
		if !ok {
			return false
		}
		return got[0]["channel_id"] == senderUID &&
			got[0]["last_client_msg_no"] == "conversation-sync-remote-1" &&
			got[0]["last_msg_seq"] == float64(1) &&
			recent0["client_msg_no"] == "conversation-sync-remote-1"
	}, 5*time.Second, 20*time.Millisecond)
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
