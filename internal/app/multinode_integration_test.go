//go:build integration
// +build integration

package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/stretchr/testify/require"
)

func TestAppThreeNodeClusterStartsWithoutStaticGroupPeers(t *testing.T) {

	harness := newThreeNodeManagedAppHarness(t)

	require.Eventually(t, func() bool {
		for _, app := range harness.orderedApps() {
			assignments, err := app.Cluster().ListSlotAssignments(context.Background())
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
	require.NoError(t, survivorLeader.Store().CreateChannel(ctx, channelID, int64(frame.ChannelTypeGroup)))
	for _, app := range harness.runningApps() {
		app := app
		require.Eventually(t, func() bool {
			channel, err := app.Store().GetChannel(context.Background(), channelID, int64(frame.ChannelTypeGroup))
			return err == nil && channel.ChannelID == channelID
		}, 5*time.Second, 20*time.Millisecond)
	}
}

func TestAppManagedSlotStartupAllowsSubsetAssignmentsPerNode(t *testing.T) {
	harness := newThreeNodeManagedAppHarnessWithLayout(t, 4, 2)

	require.Eventually(t, func() bool {
		for _, app := range harness.orderedApps() {
			assignments, err := app.Cluster().ListSlotAssignments(context.Background())
			if err != nil || len(assignments) != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}

func TestThreeNodeAppGatewaySendUsesDurableCommitWithMinISR2(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	recipientUID := "three-node-gateway-user"
	channelID := deliveryusecase.EncodePersonChannel("sender", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 15,
		LeaderEpoch:  6,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

	for _, app := range harness.appsWithLeaderFirst(leaderID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	conn := connectAppWKProtoClient(t, leader, "sender")

	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-app-1",
		Payload:     []byte("hello durable gateway"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)
	require.NoError(t, conn.Close())

	harness.stopNode(t, leaderID)
	newLeaderID := harness.waitForLeaderChange(t, 1, leaderID)
	require.NotZero(t, newLeaderID)

	for _, app := range harness.runningApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello durable gateway"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}

	harness.restartNode(t, leaderID)
	harness.waitForStableLeader(t, 1)

	msg := waitForAppCommittedMessage(t, harness.apps[leaderID], id, sendack.MessageSeq, 5*time.Second)
	require.Equal(t, []byte("hello durable gateway"), msg.Payload)
	require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
}

func TestThreeNodeAppGatewaySendUsesLongPollQuorumCommitWithMinISR2(t *testing.T) {
	harness := newThreeNodeAppHarnessWithConfigMutator(t, func(cfg *Config) {
		cfg.Cluster.ReplicationMode = "long_poll"
	})
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	recipientUID := "three-node-long-poll-user"
	channelID := deliveryusecase.EncodePersonChannel("sender-long-poll", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 115,
		LeaderEpoch:  16,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

	for _, app := range harness.appsWithLeaderFirst(leaderID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	conn := connectAppWKProtoClient(t, leader, "sender-long-poll")

	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-long-poll-1",
		Payload:     []byte("hello long poll gateway"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)
	require.NoError(t, conn.Close())

	harness.stopNode(t, leaderID)
	newLeaderID := harness.waitForLeaderChange(t, 1, leaderID)
	require.NotZero(t, newLeaderID)

	for _, app := range harness.runningApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello long poll gateway"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}

	harness.restartNode(t, leaderID)
	harness.waitForStableLeader(t, 1)

	msg := waitForAppCommittedMessage(t, harness.apps[leaderID], id, sendack.MessageSeq, 5*time.Second)
	require.Equal(t, []byte("hello long poll gateway"), msg.Payload)
	require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
}

func TestThreeNodeAppMessageSendUsesLongPollLocalCommitWithoutAdvancingQuorum(t *testing.T) {
	harness := newThreeNodeAppHarnessWithConfigMutator(t, func(cfg *Config) {
		cfg.Cluster.ReplicationMode = "long_poll"
	})
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	recipientUID := "three-node-local-commit-user"
	channelID := deliveryusecase.EncodePersonChannel("sender-local-commit", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 116,
		LeaderEpoch:  17,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

	for _, app := range harness.appsWithLeaderFirst(leaderID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	offlineFollowerID := leaderID%3 + 1
	require.NotEqual(t, leaderID, offlineFollowerID)
	harness.stopNode(t, offlineFollowerID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := leader.Message().Send(ctx, messageusecase.SendCommand{
		FromUID:     "sender-local-commit",
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-local-commit-1",
		Payload:     []byte("hello local commit long poll"),
		CommitMode:  channel.CommitModeLocal,
	})
	require.NoError(t, err)
	require.Equal(t, frame.ReasonSuccess, result.Reason)
	require.NotZero(t, result.MessageSeq)

	require.Never(t, func() bool {
		status, statusErr := leader.channelLog.Status(id)
		if statusErr != nil {
			return false
		}
		return status.CommittedSeq >= result.MessageSeq
	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestThreeNodeAppGatewaySendFromFollowerForwardsDurableAppendToLeader(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	followerID := leaderID%3 + 1
	require.NotEqual(t, leaderID, followerID)
	follower := harness.apps[followerID]
	recipientUID := "three-node-follower-user"
	channelID := deliveryusecase.EncodePersonChannel("sender-follower", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 16,
		LeaderEpoch:  7,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	_, err := leader.channelMetaSync.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)
	_, err = follower.channelMetaSync.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)

	conn := connectAppWKProtoClient(t, follower, "sender-follower")

	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-follower-1",
		Payload:     []byte("hello durable follower"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)

	for _, app := range harness.orderedApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello durable follower"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}
}

func TestThreeNodeAppGatewaySendFromFollowerBootstrapsLeaderOnDemand(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	followerID := leaderID%3 + 1
	require.NotEqual(t, leaderID, followerID)
	follower := harness.apps[followerID]
	recipientUID := "three-node-ondemand-user"
	channelID := deliveryusecase.EncodePersonChannel("sender-ondemand", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 17,
		LeaderEpoch:  8,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	_, err := follower.channelMetaSync.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)

	conn := connectAppWKProtoClient(t, follower, "sender-ondemand")

	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-ondemand-1",
		Payload:     []byte("hello durable on demand"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)

	for _, app := range harness.orderedApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello durable on demand"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}
}

func TestThreeNodeAppGatewaySendFromFollowerAfterLeaseExpiryRecoversViaMetaSync(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]
	followerID := leaderID%3 + 1
	require.NotEqual(t, leaderID, followerID)
	follower := harness.apps[followerID]
	recipientUID := "three-node-expired-lease-user"
	channelID := deliveryusecase.EncodePersonChannel("sender-expired-lease", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	initialLeaseUntilMS := time.Now().Add(200 * time.Millisecond).UnixMilli()
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 18,
		LeaderEpoch:  9,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: initialLeaseUntilMS,
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

	require.Eventually(t, func() bool {
		got, err := leader.Store().GetChannelRuntimeMeta(context.Background(), id.ID, int64(id.Type))
		return err == nil && got.LeaseUntilMS > initialLeaseUntilMS
	}, 5*time.Second, 50*time.Millisecond)

	conn := connectAppWKProtoClient(t, follower, "sender-expired-lease")

	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-expired-lease-1",
		Payload:     []byte("hello after lease expiry"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NotZero(t, sendack.MessageSeq)
	require.NotZero(t, sendack.MessageID)

	for _, app := range harness.orderedApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("hello after lease expiry"), msg.Payload)
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

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 21,
		LeaderEpoch:  8,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	senderConn, err := net.Dial("tcp", senderNode.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = senderConn.Close() })
	recipientConn, err := net.Dial("tcp", recipientNode.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = recipientConn.Close() })

	sendAppWKProtoFrame(t, senderConn, &frame.ConnectPacket{
		Version:         frame.LatestVersion,
		UID:             "sender-remote",
		DeviceID:        "sender-remote-device",
		DeviceFlag:      frame.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	connack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*frame.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, connack.ReasonCode)

	sendAppWKProtoFrame(t, recipientConn, &frame.ConnectPacket{
		Version:         frame.LatestVersion,
		UID:             recipientUID,
		DeviceID:        "recipient-remote-device",
		DeviceFlag:      frame.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	recipientConnack, ok := readAppWKProtoFrameWithin(t, recipientConn, multinodeAppReadTimeout).(*frame.ConnackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, recipientConnack.ReasonCode)

	sendAppWKProtoFrame(t, senderConn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-async-1",
		Payload:     []byte("hello realtime"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)

	recv, ok := readAppWKProtoFrameWithin(t, recipientConn, multinodeAppReadTimeout).(*frame.RecvPacket)
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

	sendAppWKProtoFrame(t, recipientConn, &frame.RecvackPacket{
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

	id := channel.ChannelID{
		ID:   "slot-realtime",
		Type: frame.ChannelTypeGroup,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 31,
		LeaderEpoch:  9,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), id.ID, int64(id.Type), []string{"slot-user-a", "slot-user-b"}))

	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	senderConn := connectMultinodeWKProtoClient(t, senderNode, "slot-sender", "slot-sender-device")
	recipientConnA := connectMultinodeWKProtoClient(t, recipientNodeA, "slot-user-a", "slot-device-a")
	recipientConnB := connectMultinodeWKProtoClient(t, recipientNodeB, "slot-user-b", "slot-device-b")

	sendAppWKProtoFrame(t, senderConn, &frame.SendPacket{
		ChannelID:   id.ID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "three-node-slot-1",
		Payload:     []byte("hello slot members"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)

	recvA, ok := readAppWKProtoFrameWithin(t, recipientConnA, multinodeAppReadTimeout).(*frame.RecvPacket)
	require.True(t, ok)
	require.Equal(t, id.ID, recvA.ChannelID)
	require.Equal(t, id.Type, recvA.ChannelType)
	require.Equal(t, "slot-sender", recvA.FromUID)

	recvB, ok := readAppWKProtoFrameWithin(t, recipientConnB, multinodeAppReadTimeout).(*frame.RecvPacket)
	require.True(t, ok)
	require.Equal(t, id.ID, recvB.ChannelID)
	require.Equal(t, id.Type, recvB.ChannelType)
	require.Equal(t, "slot-sender", recvB.FromUID)
}

func TestThreeNodeAppHotGroupDoesNotBlockNormalGroupDelivery(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	owner := harness.apps[ownerID]
	recipientNode := harness.apps[ownerID%3+1]

	hotID := channel.ChannelID{ID: "slot-hot", Type: frame.ChannelTypeGroup}
	normalID := channel.ChannelID{ID: "slot-normal", Type: frame.ChannelTypeGroup}
	for _, id := range []channel.ChannelID{hotID, normalID} {
		require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), metadb.ChannelRuntimeMeta{
			ChannelID:    id.ID,
			ChannelType:  int64(id.Type),
			ChannelEpoch: 41,
			LeaderEpoch:  10,
			Replicas:     []uint64{1, 2, 3},
			ISR:          []uint64{1, 2, 3},
			Leader:       owner.cfg.Node.ID,
			MinISR:       3,
			Status:       uint8(channel.StatusActive),
			Features:     uint64(channel.MessageSeqFormatLegacyU32),
			LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		}))
	}
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), hotID.ID, int64(hotID.Type), []string{"hot-user"}))
	require.NoError(t, owner.Store().AddChannelSubscribers(context.Background(), normalID.ID, int64(normalID.Type), []string{"normal-user"}))

	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), hotID)
		require.NoError(t, err)
		_, err = app.channelMetaSync.RefreshChannelMeta(context.Background(), normalID)
		require.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		require.NoError(t, owner.deliveryRuntime.Submit(context.Background(), deliveryruntime.CommittedEnvelope{
			Message: channel.Message{
				ChannelID:   hotID.ID,
				ChannelType: hotID.Type,
				MessageID:   uint64(i + 1),
				MessageSeq:  uint64(i + 1),
				FromUID:     "hot-sender",
				Payload:     []byte("hot"),
			},
		}))
	}
	require.Eventually(t, func() bool {
		return owner.deliveryRuntime.ActorLane(hotID.ID, hotID.Type) == deliveryruntime.LaneDedicated
	}, 5*time.Second, 20*time.Millisecond)

	senderConn := connectMultinodeWKProtoClient(t, owner, "normal-sender", "normal-sender-device")
	normalRecipientConn := connectMultinodeWKProtoClient(t, recipientNode, "normal-user", "normal-user-device")

	sendAppWKProtoFrame(t, senderConn, &frame.SendPacket{
		ChannelID:   normalID.ID,
		ChannelType: normalID.Type,
		ClientSeq:   100,
		ClientMsgNo: "normal-1",
		Payload:     []byte("normal"),
	})
	_, ok := readAppWKProtoFrameWithin(t, senderConn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)

	recvNormal, ok := readAppWKProtoFrameWithin(t, normalRecipientConn, multinodeAppReadTimeout).(*frame.RecvPacket)
	require.True(t, ok)
	require.Equal(t, normalID.ID, recvNormal.ChannelID)
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

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 41,
		LeaderEpoch:  12,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       leader.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, app := range harness.appsWithLeaderFirst(leaderID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
		require.NoError(t, err)
	}

	conn := connectMultinodeWKProtoClient(t, leader, "crash-sender", "crash-sender-device")
	sendAppWKProtoFrame(t, conn, &frame.SendPacket{
		ChannelID:   recipientUID,
		ChannelType: id.Type,
		ClientSeq:   1,
		ClientMsgNo: "sendack-crash-1",
		Payload:     []byte("survive leader crash"),
	})

	sendack, ok := readAppWKProtoFrameWithin(t, conn, multinodeAppReadTimeout).(*frame.SendackPacket)
	require.True(t, ok)
	require.Equal(t, frame.ReasonSuccess, sendack.ReasonCode)
	require.NoError(t, conn.Close())

	harness.stopNode(t, leaderID)
	newLeaderID := harness.waitForLeaderChange(t, 1, leaderID)
	require.NotZero(t, newLeaderID)

	for _, app := range harness.runningApps() {
		msg := waitForAppCommittedMessage(t, app, id, sendack.MessageSeq, 5*time.Second)
		require.Equal(t, []byte("survive leader crash"), msg.Payload)
		require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
	}

	harness.restartNode(t, leaderID)
	harness.waitForStableLeader(t, 1)

	msg := waitForAppCommittedMessage(t, harness.apps[leaderID], id, sendack.MessageSeq, 5*time.Second)
	require.Equal(t, []byte("survive leader crash"), msg.Payload)
	require.Equal(t, sendack.MessageSeq, msg.MessageSeq)
}

func TestThreeNodeAppRollingRestartPreservesWriteAvailability(t *testing.T) {
	harness := newThreeNodeAppHarness(t)
	ownerID := harness.waitForStableLeader(t, 1)
	owner := harness.apps[ownerID]
	recipientUID := "rolling-recipient"
	channelID := deliveryusecase.EncodePersonChannel("rolling-sender", recipientUID)

	id := channel.ChannelID{
		ID:   channelID,
		Type: frame.ChannelTypePerson,
	}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 51,
		LeaderEpoch:  13,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2, 3},
		Leader:       owner.cfg.Node.ID,
		MinISR:       3,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, owner.Store().UpsertChannelRuntimeMeta(context.Background(), meta))
	for _, app := range harness.appsWithLeaderFirst(ownerID) {
		_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
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
			ChannelType: id.Type,
			ClientSeq:   clientSeq,
			ClientMsgNo: clientMsgNo,
			Payload:     payload,
		})
		require.NoError(t, err)
		require.Equal(t, frame.ReasonSuccess, result.Reason)
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
			msg := waitForAppCommittedMessage(t, app, id, item.seq, 5*time.Second)
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
