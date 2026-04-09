package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

const multinodeAppReadTimeout = 20 * time.Second

func TestAppThreeNodeClusterStartsWithoutStaticGroupPeers(t *testing.T) {
	harness := newThreeNodeManagedAppHarness(t)

	require.Eventually(t, func() bool {
		for _, app := range harness.orderedApps() {
			assignments, err := app.Cluster().ListGroupAssignments(context.Background())
			if err != nil || len(assignments) != 1 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	require.NotZero(t, harness.waitForStableLeader(t, 1))
}

func TestAppMajorityAvailableAfterSingleReplicaNodeFailure(t *testing.T) {
	harness := newThreeNodeManagedAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	harness.stopNode(t, leaderID)
	survivorLeaderID := harness.waitForLeaderChange(t, 1, leaderID)
	survivorLeader := harness.apps[survivorLeaderID]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channelID := fmt.Sprintf("majority-channel-%d", time.Now().UnixNano())
	require.NoError(t, survivorLeader.Store().CreateChannel(ctx, channelID, int64(wkframe.ChannelTypeGroup)))
	for _, app := range harness.runningApps() {
		app := app
		require.Eventually(t, func() bool {
			channel, err := app.Store().GetChannel(context.Background(), channelID, int64(wkframe.ChannelTypeGroup))
			return err == nil && channel.ChannelID == channelID
		}, 5*time.Second, 20*time.Millisecond)
	}
}

func TestAppManagedGroupStartupAllowsSubsetAssignmentsPerNode(t *testing.T) {
	harness := newThreeNodeManagedAppHarnessWithLayout(t, 4, 2)

	require.Eventually(t, func() bool {
		for _, app := range harness.orderedApps() {
			assignments, err := app.Cluster().ListGroupAssignments(context.Background())
			if err != nil || len(assignments) != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}

func TestThreeNodeAppGatewaySendUsesDurableCommit(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	recipientUID := "three-node-gateway-user"
	channelID := deliveryusecase.EncodePersonChannel("sender", recipientUID)

	key := channellog.ChannelKey{
		ChannelID:   channelID,
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
		ChannelID:   recipientUID,
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

func TestThreeNodeAppDurableSendReturnsBeforeRemoteAck(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	senderNodeID := ownerID
	recipientNodeID := ownerID%3 + 1
	owner := harness.apps[ownerID]
	senderNode := harness.apps[senderNodeID]
	recipientNode := harness.apps[recipientNodeID]
	recipientUID := "remote-recipient"
	channelID := deliveryusecase.EncodePersonChannel("sender-remote", recipientUID)

	key := channellog.ChannelKey{
		ChannelID:   channelID,
		ChannelType: wkframe.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 21,
		LeaderEpoch:  8,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), key)
		require.NoError(t, err)
	}

	senderConn, err := net.Dial("tcp", senderNode.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = senderConn.Close() })
	recipientConn, err := net.Dial("tcp", recipientNode.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = recipientConn.Close() })

	sendAppWKProtoFrame(t, senderConn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "sender-remote",
		DeviceID:        "sender-remote-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	connack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*wkframe.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, connack.ReasonCode)

	sendAppWKProtoFrame(t, recipientConn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             recipientUID,
		DeviceID:        "recipient-remote-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	recipientConnack, ok := readAppWKProtoFrameWithin(t, recipientConn, multinodeAppReadTimeout).(*wkframe.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, recipientConnack.ReasonCode)

	sendAppWKProtoFrame(t, senderConn, &wkframe.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: key.ChannelType,
		ClientSeq:   1,
		ClientMsgNo: "three-node-async-1",
		Payload:     []byte("hello realtime"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*wkframe.SendackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, sendack.ReasonCode)

	recv, ok := readAppWKProtoFrameWithin(t, recipientConn, multinodeAppReadTimeout).(*wkframe.RecvPacket)
	require.True(t, ok)
	require.Equal(t, "sender-remote", recv.FromUID)
	require.Equal(t, sendack.MessageID, recv.MessageID)
	require.Equal(t, sendack.MessageSeq, recv.MessageSeq)

	var sessionID uint64
	require.Eventually(t, func() bool {
		conns := recipientNode.messageApp.OnlineRegistry().ConnectionsByUID(recipientUID)
		if len(conns) == 0 {
			return false
		}
		sessionID = conns[0].SessionID
		return owner.deliveryRuntime.HasAckBinding(sessionID, uint64(sendack.MessageID))
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		_, recipientHasBinding := recipientNode.deliveryAcks.Lookup(sessionID, uint64(sendack.MessageID))
		return recipientHasBinding
	}, 5*time.Second, 20*time.Millisecond)

	sendAppWKProtoFrame(t, recipientConn, &wkframe.RecvackPacket{
		MessageID:  recv.MessageID,
		MessageSeq: recv.MessageSeq,
	})
	require.Eventually(t, func() bool {
		_, recipientHasBinding := recipientNode.deliveryAcks.Lookup(sessionID, uint64(sendack.MessageID))
		return !owner.deliveryRuntime.HasAckBinding(sessionID, uint64(sendack.MessageID)) && !recipientHasBinding
	}, 5*time.Second, 20*time.Millisecond)
}

func TestThreeNodeAppGroupChannelRealtimeDeliveryUsesStoredSubscribers(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	owner := harness.apps[ownerID]
	senderNode := harness.apps[ownerID]
	recipientNodeA := harness.apps[ownerID%3+1]
	recipientNodeB := harness.apps[(ownerID+1)%3+1]

	key := channellog.ChannelKey{
		ChannelID:   "group-realtime",
		ChannelType: wkframe.ChannelTypeGroup,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 31,
		LeaderEpoch:  9,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), key.ChannelID, int64(key.ChannelType), []string{"group-user-a", "group-user-b"}))

	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), key)
		require.NoError(t, err)
	}

	senderConn := connectMultinodeWKProtoClient(t, senderNode, "group-sender", "group-sender-device")
	recipientConnA := connectMultinodeWKProtoClient(t, recipientNodeA, "group-user-a", "group-device-a")
	recipientConnB := connectMultinodeWKProtoClient(t, recipientNodeB, "group-user-b", "group-device-b")

	sendAppWKProtoFrame(t, senderConn, &wkframe.SendPacket{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		ClientSeq:   1,
		ClientMsgNo: "three-node-group-1",
		Payload:     []byte("hello group members"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*wkframe.SendackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, sendack.ReasonCode)

	recvA, ok := readAppWKProtoFrameWithin(t, recipientConnA, multinodeAppReadTimeout).(*wkframe.RecvPacket)
	require.True(t, ok)
	require.Equal(t, key.ChannelID, recvA.ChannelID)
	require.Equal(t, key.ChannelType, recvA.ChannelType)
	require.Equal(t, "group-sender", recvA.FromUID)

	recvB, ok := readAppWKProtoFrameWithin(t, recipientConnB, multinodeAppReadTimeout).(*wkframe.RecvPacket)
	require.True(t, ok)
	require.Equal(t, key.ChannelID, recvB.ChannelID)
	require.Equal(t, key.ChannelType, recvB.ChannelType)
	require.Equal(t, "group-sender", recvB.FromUID)
}

func TestThreeNodeAppHotGroupDoesNotBlockNormalGroupDelivery(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	owner := harness.apps[ownerID]
	recipientNode := harness.apps[ownerID%3+1]

	hotKey := channellog.ChannelKey{ChannelID: "group-hot", ChannelType: wkframe.ChannelTypeGroup}
	normalKey := channellog.ChannelKey{ChannelID: "group-normal", ChannelType: wkframe.ChannelTypeGroup}
	for _, key := range []channellog.ChannelKey{hotKey, normalKey} {
		require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), metadb.ChannelRuntimeMeta{
			ChannelID:    key.ChannelID,
			ChannelType:  int64(key.ChannelType),
			ChannelEpoch: 41,
			LeaderEpoch:  10,
			Replicas:     []uint64{1, 2, 3},
			ISR:          []uint64{1, 2, 3},
			Leader:       owner.cfg.Node.ID,
			MinISR:       3,
			Status:       uint8(channellog.ChannelStatusActive),
			Features:     uint64(channellog.MessageSeqFormatLegacyU32),
			LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		}))
	}
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), hotKey.ChannelID, int64(hotKey.ChannelType), []string{"hot-user"}))
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), normalKey.ChannelID, int64(normalKey.ChannelType), []string{"normal-user"}))

	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), hotKey)
		require.NoError(t, err)
		_, err = app.channelMetaSync.RefreshChannelMeta(context.Background(), normalKey)
		require.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		require.NoError(t, owner.deliveryRuntime.Submit(context.Background(), deliveryruntime.CommittedEnvelope{
			ChannelID:   hotKey.ChannelID,
			ChannelType: hotKey.ChannelType,
			MessageID:   uint64(i + 1),
			MessageSeq:  uint64(i + 1),
			FromUID:     "hot-sender",
			Payload:     []byte("hot"),
		}))
	}
	require.Eventually(t, func() bool {
		return owner.deliveryRuntime.ActorLane(hotKey.ChannelID, hotKey.ChannelType) == deliveryruntime.LaneDedicated
	}, 5*time.Second, 20*time.Millisecond)

	senderConn := connectMultinodeWKProtoClient(t, owner, "normal-sender", "normal-sender-device")
	normalRecipientConn := connectMultinodeWKProtoClient(t, recipientNode, "normal-user", "normal-user-device")

	sendAppWKProtoFrame(t, senderConn, &wkframe.SendPacket{
		ChannelID:   normalKey.ChannelID,
		ChannelType: normalKey.ChannelType,
		ClientSeq:   100,
		ClientMsgNo: "normal-1",
		Payload:     []byte("normal"),
	})
	_, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*wkframe.SendackPacket)
	require.True(t, ok)

	recvNormal, ok := readAppWKProtoFrameWithin(t, normalRecipientConn, multinodeAppReadTimeout).(*wkframe.RecvPacket)
	require.True(t, ok)
	require.Equal(t, normalKey.ChannelID, recvNormal.ChannelID)
}

func TestThreeNodeAppUserTokenEndpointPersistsThroughClusterForwarding(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)

	targetNodeID := leaderID%3 + 1
	require.NotEqual(t, leaderID, targetNodeID)
	target := harness.apps[targetNodeID]

	req, err := http.NewRequest(
		http.MethodPost,
		"http://"+target.API().Addr()+"/user/token",
		bytes.NewBufferString(`{"uid":"multi-token-user","token":"token-cluster","device_flag":1,"device_level":1}`),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.JSONEq(t, `{"status":200}`, string(body))

	for _, app := range harness.orderedApps() {
		app := app
		require.Eventually(t, func() bool {
			gotUser, err := app.DB().ForSlot(1).GetUser(context.Background(), "multi-token-user")
			return err == nil && gotUser == (metadb.User{UID: "multi-token-user"})
		}, 5*time.Second, 20*time.Millisecond)
	}

	for _, app := range harness.orderedApps() {
		app := app
		require.Eventually(t, func() bool {
			gotDevice, err := app.DB().ForSlot(1).GetDevice(context.Background(), "multi-token-user", 1)
			return err == nil && gotDevice == (metadb.Device{
				UID:         "multi-token-user",
				DeviceFlag:  1,
				Token:       "token-cluster",
				DeviceLevel: 1,
			})
		}, 5*time.Second, 20*time.Millisecond)
	}
}

func TestThreeNodeAppSendAckSurvivesLeaderCrash(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	recipientUID := "crash-recipient"
	channelID := deliveryusecase.EncodePersonChannel("crash-sender", recipientUID)

	key := channellog.ChannelKey{
		ChannelID:   channelID,
		ChannelType: wkframe.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 41,
		LeaderEpoch:  12,
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

	conn := connectMultinodeWKProtoClient(t, leader, "crash-sender", "crash-sender-device")
	sendAppWKProtoFrame(t, conn, &wkframe.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: key.ChannelType,
		ClientSeq:   1,
		ClientMsgNo: "sendack-crash-1",
		Payload:     []byte("survive leader crash"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*wkframe.SendackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, sendack.ReasonCode)
	require.NoError(t, conn.Close())

	harness.stopNode(t, leaderID)
	newLeaderID := harness.waitForLeaderChange(t, 1, leaderID)
	require.NotZero(t, newLeaderID)

	for _, app := range harness.runningApps() {
		msg := waitForAppCommittedMessage(t, app.ChannelLogDB().ForChannel(key), sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("survive leader crash"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}

	harness.restartNode(t, leaderID)
	harness.waitForStableLeader(t, 1)

	msg := waitForAppCommittedMessage(t, harness.apps[leaderID].ChannelLogDB().ForChannel(key), sendack.MessageSeq, 5*time.Second)
	require.Equal(t, []byte("survive leader crash"), msg.Payload)
	require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
}

func TestThreeNodeAppRollingRestartPreservesWriteAvailability(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	owner := harness.apps[ownerID]
	recipientUID := "rolling-recipient"
	channelID := deliveryusecase.EncodePersonChannel("rolling-sender", recipientUID)

	key := channellog.ChannelKey{
		ChannelID:   channelID,
		ChannelType: wkframe.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 51,
		LeaderEpoch:  13,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), key)
		require.NoError(t, err)
	}

	type sentMessage struct {
		seq     uint64
		payload []byte
	}
	sent := make([]sentMessage, 0, 4)

	sendViaOwner := func(clientSeq uint64, clientMsgNo string, payload []byte) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := harness.apps[ownerID].Message().Send(ctx, messageusecase.SendCommand{
			FromUID:     "rolling-sender",
			ChannelID:   recipientUID,
			ChannelType: key.ChannelType,
			ClientSeq:   clientSeq,
			ClientMsgNo: clientMsgNo,
			Payload:     payload,
		})
		require.NoError(t, err)
		require.Equal(t, wkframe.ReasonSuccess, result.Reason)
		sent = append(sent, sentMessage{seq: result.MessageSeq, payload: payload})
	}

	restartOrder := make([]uint64, 0, 3)
	for _, nodeID := range []uint64{1, 2, 3} {
		if nodeID == ownerID {
			continue
		}
		restartOrder = append(restartOrder, nodeID)
	}
	restartOrder = append(restartOrder, ownerID)

	sendViaOwner(1, "rolling-1", []byte(fmt.Sprintf("before restart node-%d", restartOrder[0])))
	harness.stopNode(t, restartOrder[0])
	harness.restartNode(t, restartOrder[0])
	harness.waitForStableLeader(t, 1)

	sendViaOwner(2, "rolling-2", []byte(fmt.Sprintf("before restart node-%d", restartOrder[1])))
	harness.stopNode(t, restartOrder[1])
	harness.restartNode(t, restartOrder[1])
	harness.waitForStableLeader(t, 1)

	sendViaOwner(3, "rolling-3", []byte("before restart owner"))
	harness.stopNode(t, ownerID)
	harness.restartNode(t, ownerID)
	harness.waitForStableLeader(t, 1)

	sendViaOwner(4, "rolling-4", []byte("after rolling restart"))

	for _, item := range sent {
		for _, app := range harness.orderedApps() {
			msg := waitForAppCommittedMessage(t, app.ChannelLogDB().ForChannel(key), item.seq, 5*time.Second)
			require.Equal(t, item.payload, msg.Payload)
			require.Equal(t, item.seq, msg.MessageSeq)
		}
	}
}

func TestThreeNodeAppHarnessRestartNodePreservesDataDir(t *testing.T) {
	harness := newThreeNodeAppHarness(t)

	oldDataDir := harness.apps[2].cfg.Node.DataDir
	oldGatewayAddr := harness.apps[2].Gateway().ListenerAddr("tcp-wkproto")

	harness.stopNode(t, 2)
	restarted := harness.restartNode(t, 2)

	require.Equal(t, oldDataDir, restarted.cfg.Node.DataDir)
	require.Equal(t, oldGatewayAddr, restarted.Gateway().ListenerAddr("tcp-wkproto"))

	harness.waitForStableLeader(t, 1)
}

func TestThreeNodeAppHarnessUsesExplicitDataPlaneConcurrency(t *testing.T) {
	harness := newThreeNodeAppHarness(t)

	for _, app := range harness.orderedApps() {
		require.Equal(t, 1, app.cfg.Cluster.PoolSize)
		require.Equal(t, 8, app.cfg.Cluster.DataPlanePoolSize)
		require.Equal(t, 16, app.cfg.Cluster.DataPlaneMaxFetchInflight)
		require.Equal(t, 16, app.cfg.Cluster.DataPlaneMaxPendingFetch)
	}
}

type threeNodeAppHarness struct {
	apps  map[uint64]*App
	specs map[uint64]appNodeSpec
}

type appNodeSpec struct {
	cfg Config
}

func newThreeNodeAppHarness(t *testing.T) *threeNodeAppHarness {
	return newThreeNodeAppHarnessWithOptions(t, 1, 3, nil)
}

func newThreeNodeManagedAppHarness(t *testing.T) *threeNodeAppHarness {
	return newThreeNodeManagedAppHarnessWithLayout(t, 1, 3)
}

func newThreeNodeManagedAppHarnessWithLayout(t *testing.T, groupCount uint32, groupReplicaN int) *threeNodeAppHarness {
	return newThreeNodeAppHarnessWithOptions(t, groupCount, groupReplicaN, nil)
}

func newThreeNodeAppHarnessWithConfigMutator(t *testing.T, mutate func(*Config)) *threeNodeAppHarness {
	return newThreeNodeAppHarnessWithOptions(t, 1, 3, mutate)
}

func newThreeNodeAppHarnessWithOptions(t *testing.T, groupCount uint32, groupReplicaN int, mutate func(*Config)) *threeNodeAppHarness {
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
	specs := make(map[uint64]appNodeSpec, 3)
	for i := 0; i < 3; i++ {
		nodeID := uint64(i + 1)
		cfg := validConfig()
		cfg.Node.ID = nodeID
		cfg.Node.Name = fmt.Sprintf("node-%d", nodeID)
		cfg.Node.DataDir = filepath.Join(root, fmt.Sprintf("node-%d", nodeID))
		cfg.Storage = StorageConfig{}
		cfg.Cluster.ListenAddr = clusterAddrs[nodeID]
		cfg.Cluster.Nodes = append([]NodeConfigRef(nil), clusterNodes...)
		cfg.Cluster.GroupCount = groupCount
		cfg.Cluster.ControllerReplicaN = 3
		cfg.Cluster.GroupReplicaN = groupReplicaN
		cfg.Cluster.Groups = nil
		cfg.Cluster.TickInterval = 10 * time.Millisecond
		cfg.Cluster.ElectionTick = 10
		cfg.Cluster.HeartbeatTick = 1
		cfg.Cluster.ForwardTimeout = 2 * time.Second
		cfg.Cluster.DialTimeout = 2 * time.Second
		cfg.Cluster.PoolSize = 1
		cfg.Cluster.DataPlanePoolSize = 8
		cfg.Cluster.DataPlaneMaxFetchInflight = 16
		cfg.Cluster.DataPlaneMaxPendingFetch = 16
		cfg.API.ListenAddr = apiAddrs[nodeID]
		cfg.Gateway.Listeners = []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto", gatewayAddrs[nodeID]),
		}
		if mutate != nil {
			mutate(&cfg)
		}

		specs[nodeID] = appNodeSpec{cfg: cfg}

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

	harness := &threeNodeAppHarness{apps: apps, specs: specs}
	t.Cleanup(func() {
		for i := 3; i >= 1; i-- {
			if app := apps[uint64(i)]; app != nil {
				require.NoError(t, app.Stop())
			}
		}
	})
	return harness
}

func (h *threeNodeAppHarness) runningApps() []*App {
	apps := make([]*App, 0, len(h.apps))
	for _, nodeID := range []uint64{1, 2, 3} {
		if app := h.apps[nodeID]; app != nil {
			apps = append(apps, app)
		}
	}
	return apps
}

func (h *threeNodeAppHarness) orderedApps() []*App {
	return h.runningApps()
}

func (h *threeNodeAppHarness) appsWithLeaderFirst(leaderID uint64) []*App {
	apps := make([]*App, 0, len(h.apps))
	if leader := h.apps[leaderID]; leader != nil {
		apps = append(apps, leader)
	}
	for _, app := range h.runningApps() {
		if app.cfg.Node.ID == leaderID {
			continue
		}
		apps = append(apps, app)
	}
	return apps
}

func (h *threeNodeAppHarness) stopNode(t *testing.T, nodeID uint64) {
	t.Helper()

	app := h.apps[nodeID]
	require.NotNil(t, app, "node %d is not running", nodeID)
	require.NoError(t, app.Stop())
	h.apps[nodeID] = nil
}

func (h *threeNodeAppHarness) restartNode(t *testing.T, nodeID uint64) *App {
	t.Helper()

	require.Nil(t, h.apps[nodeID], "node %d is already running", nodeID)

	spec, ok := h.specs[nodeID]
	require.True(t, ok, "missing spec for node %d", nodeID)

	app, err := New(spec.cfg)
	require.NoError(t, err)
	require.NoError(t, app.Start())
	h.apps[nodeID] = app
	return app
}

func (h *threeNodeAppHarness) waitForLeaderChange(t *testing.T, groupID uint64, oldLeader uint64) uint64 {
	t.Helper()

	require.Eventually(t, func() bool {
		leader, ok := h.consensusLeader(multiraft.GroupID(groupID))
		return ok && leader != 0 && uint64(leader) != oldLeader
	}, 10*time.Second, 50*time.Millisecond)
	return h.waitForStableLeader(t, groupID)
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
	for _, app := range h.runningApps() {
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

func connectMultinodeWKProtoClient(t *testing.T, app *App, uid, deviceID string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", app.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sendAppWKProtoFrame(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             uid,
		DeviceID:        deviceID,
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	connack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*wkframe.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, wkframe.ReasonSuccess, connack.ReasonCode)
	return conn
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

func waitForAppCommittedMessage(t *testing.T, store *channellog.Store, seq uint64, timeout time.Duration) channellog.Message {
	t.Helper()

	var msg channellog.Message
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
