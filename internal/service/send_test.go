package service

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/service/testkit"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

var fixedSendNow = time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

func fixedNowFn() time.Time {
	return fixedSendNow
}

func TestHandleSendDeliversLocalPersonMessage(t *testing.T) {
	sender := testkit.NewRecordingSession(1, "tcp")
	recipientA := testkit.NewRecordingSession(2, "tcp")
	recipientB := testkit.NewRecordingSession(3, "tcp")

	svc := New(Options{Now: fixedNowFn})
	registerRecipient(t, svc, "u2", recipientA, recipientB)

	sender.SetValue(gateway.SessionValueUID, "u1")
	ctx := &gateway.Context{Session: sender, Listener: "tcp"}
	pkt := &wkpacket.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   9,
		ClientMsgNo: "m1",
	}

	require.NoError(t, svc.OnFrame(ctx, pkt))
	require.Len(t, sender.WrittenFrames(), 1)
	require.Len(t, recipientA.WrittenFrames(), 1)
	require.Len(t, recipientB.WrittenFrames(), 1)

	ack := requireSendackPacket(t, sender.WrittenFrames()[0])
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, pkt.ClientSeq, ack.ClientSeq)
	require.Equal(t, pkt.ClientMsgNo, ack.ClientMsgNo)
	require.NotZero(t, ack.MessageID)
	require.NotZero(t, ack.MessageSeq)

	recvA := requireRecvPacket(t, recipientA.WrittenFrames()[0])
	recvB := requireRecvPacket(t, recipientB.WrittenFrames()[0])

	require.Equal(t, "u1", recvA.FromUID)
	require.Equal(t, "u1", recvB.FromUID)
	require.Equal(t, "u1", recvA.ChannelID)
	require.Equal(t, "u1", recvB.ChannelID)
	require.Equal(t, wkpacket.ChannelTypePerson, recvA.ChannelType)
	require.Equal(t, wkpacket.ChannelTypePerson, recvB.ChannelType)
	require.NotZero(t, recvA.MessageID)
	require.Equal(t, recvA.MessageID, recvB.MessageID)
	require.Equal(t, recvA.MessageSeq, recvB.MessageSeq)
	require.Equal(t, ack.MessageID, recvA.MessageID)
	require.Equal(t, ack.MessageSeq, recvA.MessageSeq)
	require.Equal(t, pkt.Payload, recvA.Payload)
	require.Equal(t, pkt.Payload, recvB.Payload)
}

func TestHandleSendWritesUserNotOnNodeAckWhenRecipientIsOffline(t *testing.T) {
	sender := testkit.NewRecordingSession(1, "tcp")
	sender.SetValue(gateway.SessionValueUID, "u1")

	svc := New(Options{Now: fixedNowFn})
	ctx := &gateway.Context{Session: sender, Listener: "tcp"}
	pkt := &wkpacket.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		ClientSeq:   11,
		ClientMsgNo: "m2",
	}

	require.NoError(t, svc.OnFrame(ctx, pkt))
	require.Len(t, sender.WrittenFrames(), 1)

	ack := requireSendackPacket(t, sender.WrittenFrames()[0])
	require.Equal(t, wkpacket.ReasonUserNotOnNode, ack.ReasonCode)
	require.Zero(t, ack.MessageID)
	require.Zero(t, ack.MessageSeq)
	require.Equal(t, pkt.ClientSeq, ack.ClientSeq)
	require.Equal(t, pkt.ClientMsgNo, ack.ClientMsgNo)
}

func TestHandleSendRejectsUnsupportedChannelType(t *testing.T) {
	sender := testkit.NewRecordingSession(1, "tcp")
	recipient := testkit.NewRecordingSession(2, "tcp")
	sender.SetValue(gateway.SessionValueUID, "u1")

	svc := New(Options{Now: fixedNowFn})
	registerRecipient(t, svc, "group-1", recipient)

	ctx := &gateway.Context{Session: sender, Listener: "tcp"}
	pkt := &wkpacket.SendPacket{
		ChannelID:   "group-1",
		ChannelType: wkpacket.ChannelTypeGroup,
		ClientSeq:   12,
		ClientMsgNo: "m3",
	}

	require.NoError(t, svc.OnFrame(ctx, pkt))
	require.Len(t, sender.WrittenFrames(), 1)
	require.Empty(t, recipient.WrittenFrames())

	ack := requireSendackPacket(t, sender.WrittenFrames()[0])
	require.Equal(t, wkpacket.ReasonNotSupportChannelType, ack.ReasonCode)
	require.Zero(t, ack.MessageID)
	require.Zero(t, ack.MessageSeq)
}

func TestHandleSendRequiresAuthenticatedSender(t *testing.T) {
	sender := testkit.NewRecordingSession(1, "tcp")
	recipient := testkit.NewRecordingSession(2, "tcp")

	svc := New(Options{Now: fixedNowFn})
	registerRecipient(t, svc, "u2", recipient)

	ctx := &gateway.Context{Session: sender, Listener: "tcp"}
	pkt := &wkpacket.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
	}

	err := svc.OnFrame(ctx, pkt)
	require.ErrorIs(t, err, ErrUnauthenticatedSession)
	require.Empty(t, sender.WrittenFrames())
	require.Empty(t, recipient.WrittenFrames())
}

func registerRecipient(t *testing.T, svc *Service, uid string, sessions ...*testkit.RecordingSession) {
	t.Helper()

	for _, sess := range sessions {
		sess.SetValue(gateway.SessionValueUID, uid)
		sess.SetValue(gateway.SessionValueDeviceFlag, wkpacket.APP)
		sess.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)
		require.NoError(t, svc.OnSessionOpen(&gateway.Context{
			Session:  sess,
			Listener: sess.Listener(),
		}))
	}
}

func requireSendackPacket(t *testing.T, frame wkpacket.Frame) *wkpacket.SendackPacket {
	t.Helper()

	ack, ok := frame.(*wkpacket.SendackPacket)
	require.True(t, ok, "expected *wkpacket.SendackPacket, got %T", frame)
	return ack
}

func requireRecvPacket(t *testing.T, frame wkpacket.Frame) *wkpacket.RecvPacket {
	t.Helper()

	recv, ok := frame.(*wkpacket.RecvPacket)
	require.True(t, ok, "expected *wkpacket.RecvPacket, got %T", frame)
	return recv
}
