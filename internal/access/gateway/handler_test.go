package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	gatewaysession "github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestHandlerOnSessionOpenRegistersAuthenticatedSession(t *testing.T) {
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	handler := New(Options{Now: func() time.Time { return fixedNow }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, handler.OnSessionOpen(ctx))

	conns := handler.online.ConnectionsByUID("u1")
	require.Len(t, conns, 1)
	require.Equal(t, uint64(1), conns[0].SessionID)
	require.Equal(t, fixedNow, conns[0].ConnectedAt)
}

func TestHandlerOnSessionCloseUnregistersSession(t *testing.T) {
	handler := New(Options{Now: func() time.Time { return time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC) }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, handler.OnSessionOpen(ctx))
	require.NoError(t, handler.OnSessionClose(ctx))

	require.Empty(t, handler.online.ConnectionsByUID("u1"))
	_, ok := handler.online.Connection(1)
	require.False(t, ok)
}

func TestHandlerOnSessionOpenRejectsUnauthenticatedContext(t *testing.T) {
	handler := New(Options{})

	err := handler.OnSessionOpen(&coregateway.Context{
		Session: gatewaysession.New(gatewaysession.Config{
			ID:       1,
			Listener: "tcp",
		}),
	})

	require.ErrorIs(t, err, ErrUnauthenticatedSession)
}

func TestHandlerOnSessionErrorDoesNotMutateRegistry(t *testing.T) {
	handler := New(Options{Now: func() time.Time { return time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC) }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, handler.OnSessionOpen(ctx))
	before := handler.online.ConnectionsByUID("u1")

	handler.OnSessionError(ctx, errors.New("boom"))

	after := handler.online.ConnectionsByUID("u1")
	require.Equal(t, before, after)
}

func TestHandlerOnFrameSendMapsCommandAndWritesSendack(t *testing.T) {
	sender := newOptionRecordingSession(1, "tcp")
	sender.SetValue(coregateway.SessionValueUID, "u1")
	handler := New(Options{
		Messages: &fakeMessageUsecase{
			sendResult: message.SendResult{
				MessageID:  99,
				MessageSeq: 7,
				Reason:     wkpacket.ReasonSuccess,
			},
		},
	})

	ctx := &coregateway.Context{Session: sender, Listener: "tcp", ReplyToken: "reply-1"}
	pkt := &wkpacket.SendPacket{
		Framer:      wkpacket.Framer{RedDot: true},
		Setting:     1,
		MsgKey:      "key-1",
		Expire:      10,
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Topic:       "chat",
		Payload:     []byte("hi"),
		ClientSeq:   13,
		ClientMsgNo: "m4",
		StreamNo:    "stream-1",
	}

	require.NoError(t, handler.OnFrame(ctx, pkt))
	require.Len(t, sender.Writes(), 1)

	msgs := handler.messages.(*fakeMessageUsecase)
	require.Len(t, msgs.sendCommands, 1)
	require.Equal(t, "u1", msgs.sendCommands[0].SenderUID)
	require.Equal(t, "u2", msgs.sendCommands[0].ChannelID)
	require.Equal(t, wkpacket.ChannelTypePerson, msgs.sendCommands[0].ChannelType)
	require.Equal(t, uint64(13), msgs.sendCommands[0].ClientSeq)
	require.Equal(t, "m4", msgs.sendCommands[0].ClientMsgNo)
	require.Equal(t, "stream-1", msgs.sendCommands[0].StreamNo)
	require.Equal(t, "chat", msgs.sendCommands[0].Topic)
	require.Equal(t, []byte("hi"), msgs.sendCommands[0].Payload)
	require.True(t, msgs.sendCommands[0].Framer.RedDot)

	write := sender.Writes()[0]
	ack := requireSendackPacket(t, write.frame)
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, int64(99), ack.MessageID)
	require.Equal(t, uint32(7), ack.MessageSeq)
	require.Equal(t, uint64(13), ack.ClientSeq)
	require.Equal(t, "m4", ack.ClientMsgNo)
	require.Equal(t, "reply-1", write.meta.ReplyToken)
}

func TestHandlerOnFrameRecvackRoutesToMessageUsecase(t *testing.T) {
	msgs := &fakeMessageUsecase{}
	handler := New(Options{Messages: msgs})

	err := handler.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.RecvackPacket{
		Framer:     wkpacket.Framer{RedDot: true},
		MessageID:  88,
		MessageSeq: 9,
	})

	require.NoError(t, err)
	require.Equal(t, 1, msgs.recvAckCalls)
	require.Len(t, msgs.recvAckCommands, 1)
	require.Equal(t, "u1", msgs.recvAckCommands[0].UID)
	require.Equal(t, int64(88), msgs.recvAckCommands[0].MessageID)
	require.Equal(t, uint32(9), msgs.recvAckCommands[0].MessageSeq)
	require.True(t, msgs.recvAckCommands[0].Framer.RedDot)
}

func TestHandlerOnFramePingIsNoop(t *testing.T) {
	handler := New(Options{Messages: &fakeMessageUsecase{}})

	err := handler.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.PingPacket{})

	require.NoError(t, err)
}

func TestNewSharesOnlineRegistryWithInjectedMessageApp(t *testing.T) {
	msgApp := message.New(message.Options{
		IdentityStore: &fakeIdentityStore{},
		ChannelStore:  &fakeChannelStore{},
		ClusterPort:   &fakeClusterPort{},
		Now:           func() time.Time { return fixedGatewayNow },
	})
	handler := New(Options{
		Messages: msgApp,
		Now:      func() time.Time { return fixedGatewayNow },
	})

	sender := newOptionRecordingSession(1, "tcp")
	sender.SetValue(coregateway.SessionValueUID, "u1")
	sender.SetValue(coregateway.SessionValueDeviceFlag, wkpacket.APP)
	sender.SetValue(coregateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)

	recipient := newOptionRecordingSession(2, "tcp")
	recipient.SetValue(coregateway.SessionValueUID, "u2")
	recipient.SetValue(coregateway.SessionValueDeviceFlag, wkpacket.APP)
	recipient.SetValue(coregateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)

	require.NoError(t, handler.OnSessionOpen(&coregateway.Context{Session: sender, Listener: "tcp"}))
	require.NoError(t, handler.OnSessionOpen(&coregateway.Context{Session: recipient, Listener: "tcp"}))

	err := handler.OnFrame(&coregateway.Context{Session: sender, Listener: "tcp"}, &wkpacket.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   1,
		ClientMsgNo: "m1",
	})
	require.NoError(t, err)

	require.Len(t, sender.Writes(), 1)
	ack := requireSendackPacket(t, sender.Writes()[0].frame)
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)

	require.Len(t, recipient.Writes(), 1)
	recv := requireRecvPacket(t, recipient.Writes()[0].frame)
	require.Equal(t, "u1", recv.FromUID)
	require.Equal(t, []byte("hi"), recv.Payload)
}

func newAuthedContext(t *testing.T, sessionID uint64, uid string) *coregateway.Context {
	t.Helper()

	sess := gatewaysession.New(gatewaysession.Config{
		ID:       sessionID,
		Listener: "tcp",
	})
	sess.SetValue(coregateway.SessionValueUID, uid)
	sess.SetValue(coregateway.SessionValueDeviceFlag, wkpacket.APP)
	sess.SetValue(coregateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)

	return &coregateway.Context{
		Session:  sess,
		Listener: "tcp",
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

type fakeMessageUsecase struct {
	sendCommands    []message.SendCommand
	sendResult      message.SendResult
	sendErr         error
	recvAckCalls    int
	recvAckCommands []message.RecvAckCommand
	recvAckErr      error
}

func (f *fakeMessageUsecase) Send(cmd message.SendCommand) (message.SendResult, error) {
	f.sendCommands = append(f.sendCommands, cmd)
	return f.sendResult, f.sendErr
}

func (f *fakeMessageUsecase) RecvAck(cmd message.RecvAckCommand) error {
	f.recvAckCalls++
	f.recvAckCommands = append(f.recvAckCommands, cmd)
	return f.recvAckErr
}

type outboundWrite struct {
	frame wkpacket.Frame
	meta  gatewaysession.OutboundMeta
}

type optionRecordingSession struct {
	gatewaysession.Session
	mu     sync.Mutex
	writes []outboundWrite
}

func newOptionRecordingSession(id uint64, listener string) *optionRecordingSession {
	recorder := &optionRecordingSession{}
	recorder.Session = gatewaysession.New(gatewaysession.Config{
		ID:       id,
		Listener: listener,
		WriteFrameFn: func(frame wkpacket.Frame, meta gatewaysession.OutboundMeta) error {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			recorder.writes = append(recorder.writes, outboundWrite{frame: frame, meta: meta})
			return nil
		},
	})
	return recorder
}

func (s *optionRecordingSession) Writes() []outboundWrite {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]outboundWrite, len(s.writes))
	copy(out, s.writes)
	return out
}

var fixedGatewayNow = time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

type fakeIdentityStore struct{}

func (*fakeIdentityStore) GetUser(context.Context, string) (wkdb.User, error) {
	return wkdb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (wkdb.Channel, error) {
	return wkdb.Channel{}, nil
}

type fakeClusterPort struct{}

var _ online.Registry = (*online.MemoryRegistry)(nil)
