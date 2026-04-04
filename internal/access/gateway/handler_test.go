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
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
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
				MessageSeq: uint64(^uint32(0)) + 7,
				Reason:     wkframe.ReasonSuccess,
			},
		},
	})

	ctx := &coregateway.Context{
		Session:        sender,
		Listener:       "tcp",
		ReplyToken:     "reply-1",
		RequestContext: context.Background(),
	}
	pkt := &wkframe.SendPacket{
		Framer:      wkframe.Framer{RedDot: true},
		Setting:     1,
		MsgKey:      "key-1",
		Expire:      10,
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
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
	require.Equal(t, wkframe.ChannelTypePerson, msgs.sendCommands[0].ChannelType)
	require.Equal(t, uint64(13), msgs.sendCommands[0].ClientSeq)
	require.Equal(t, "m4", msgs.sendCommands[0].ClientMsgNo)
	require.Equal(t, "stream-1", msgs.sendCommands[0].StreamNo)
	require.Equal(t, "chat", msgs.sendCommands[0].Topic)
	require.Equal(t, []byte("hi"), msgs.sendCommands[0].Payload)
	require.True(t, msgs.sendCommands[0].Framer.RedDot)

	write := sender.Writes()[0]
	ack := requireSendackPacket(t, write.frame)
	require.Equal(t, wkframe.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, int64(99), ack.MessageID)
	require.Equal(t, uint64(^uint32(0))+7, ack.MessageSeq)
	require.Equal(t, uint64(13), ack.ClientSeq)
	require.Equal(t, "m4", ack.ClientMsgNo)
	require.Equal(t, "reply-1", write.meta.ReplyToken)
}

func TestHandlerOnFrameSendPropagatesRequestContext(t *testing.T) {
	type ctxKey string

	sender := newOptionRecordingSession(1, "tcp")
	sender.SetValue(coregateway.SessionValueUID, "u1")
	msgs := &fakeMessageUsecase{}
	handler := New(Options{Messages: msgs})

	reqCtx := context.WithValue(context.Background(), ctxKey("request"), "gateway-send")
	ctx := &coregateway.Context{
		Session:        sender,
		Listener:       "tcp",
		ReplyToken:     "reply-ctx",
		RequestContext: reqCtx,
	}
	pkt := &wkframe.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientSeq:   33,
		ClientMsgNo: "ctx-1",
	}

	require.NoError(t, handler.OnFrame(ctx, pkt))
	require.Len(t, msgs.sendContexts, 1)
	require.Equal(t, "gateway-send", msgs.sendContexts[0].Value(ctxKey("request")))
	_, ok := msgs.sendContexts[0].Deadline()
	require.True(t, ok)
}

func TestHandlerOnFrameSendMapsCanceledRequestContextToSendack(t *testing.T) {
	sender := newOptionRecordingSession(1, "tcp")
	sender.SetValue(coregateway.SessionValueUID, "u1")
	msgs := &fakeMessageUsecase{
		sendFn: func(ctx context.Context, _ message.SendCommand) (message.SendResult, error) {
			return message.SendResult{}, ctx.Err()
		},
	}
	handler := New(Options{Messages: msgs})

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := &coregateway.Context{
		Session:        sender,
		Listener:       "tcp",
		ReplyToken:     "reply-canceled",
		RequestContext: reqCtx,
	}

	err := handler.OnFrame(ctx, &wkframe.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientSeq:   34,
		ClientMsgNo: "ctx-canceled",
	})

	require.NoError(t, err)
	require.Len(t, sender.Writes(), 1)
	ack := requireSendackPacket(t, sender.Writes()[0].frame)
	require.Equal(t, wkframe.ReasonSystemError, ack.ReasonCode)
	require.Equal(t, uint64(34), ack.ClientSeq)
	require.Equal(t, "ctx-canceled", ack.ClientMsgNo)
	require.Len(t, msgs.sendContexts, 1)
	require.ErrorIs(t, msgs.sendContexts[0].Err(), context.Canceled)
}

func TestHandlerOnFrameSendMapsChannelclusterErrorsToSendack(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason wkframe.ReasonCode
	}{
		{
			name:   "channel deleting",
			err:    channellog.ErrChannelDeleting,
			reason: wkframe.ReasonChannelDeleting,
		},
		{
			name:   "protocol upgrade required",
			err:    channellog.ErrProtocolUpgradeRequired,
			reason: wkframe.ReasonProtocolUpgradeRequired,
		},
		{
			name:   "idempotency conflict",
			err:    channellog.ErrIdempotencyConflict,
			reason: wkframe.ReasonIdempotencyConflict,
		},
		{
			name:   "message seq exhausted",
			err:    channellog.ErrMessageSeqExhausted,
			reason: wkframe.ReasonMessageSeqExhausted,
		},
		{
			name:   "stale meta",
			err:    channellog.ErrStaleMeta,
			reason: wkframe.ReasonNodeNotMatch,
		},
		{
			name:   "not leader",
			err:    channellog.ErrNotLeader,
			reason: wkframe.ReasonNodeNotMatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := newOptionRecordingSession(1, "tcp")
			sender.SetValue(coregateway.SessionValueUID, "u1")
			handler := New(Options{
				Messages: &fakeMessageUsecase{sendErr: tt.err},
			})

			ctx := &coregateway.Context{
				Session:        sender,
				Listener:       "tcp",
				ReplyToken:     "reply-2",
				RequestContext: context.Background(),
			}
			pkt := &wkframe.SendPacket{
				ChannelID:   "u2",
				ChannelType: wkframe.ChannelTypePerson,
				ClientSeq:   13,
				ClientMsgNo: "m4",
			}

			require.NoError(t, handler.OnFrame(ctx, pkt))
			require.Len(t, sender.Writes(), 1)

			ack := requireSendackPacket(t, sender.Writes()[0].frame)
			require.Equal(t, tt.reason, ack.ReasonCode)
			require.Zero(t, ack.MessageID)
			require.Zero(t, ack.MessageSeq)
			require.Equal(t, uint64(13), ack.ClientSeq)
			require.Equal(t, "m4", ack.ClientMsgNo)
		})
	}
}

func TestHandlerOnFrameRecvackRoutesToMessageUsecase(t *testing.T) {
	msgs := &fakeMessageUsecase{}
	handler := New(Options{Messages: msgs})

	err := handler.OnFrame(newAuthedContext(t, 1, "u1"), &wkframe.RecvackPacket{
		Framer:     wkframe.Framer{RedDot: true},
		MessageID:  88,
		MessageSeq: 9,
	})

	require.NoError(t, err)
	require.Equal(t, 1, msgs.recvAckCalls)
	require.Len(t, msgs.recvAckCommands, 1)
	require.Equal(t, "u1", msgs.recvAckCommands[0].UID)
	require.Equal(t, int64(88), msgs.recvAckCommands[0].MessageID)
	require.Equal(t, uint64(9), msgs.recvAckCommands[0].MessageSeq)
	require.True(t, msgs.recvAckCommands[0].Framer.RedDot)
}

func TestHandlerOnFramePingIsNoop(t *testing.T) {
	handler := New(Options{Messages: &fakeMessageUsecase{}})

	err := handler.OnFrame(newAuthedContext(t, 1, "u1"), &wkframe.PingPacket{})

	require.NoError(t, err)
}

func TestNewSharesOnlineRegistryWithInjectedMessageApp(t *testing.T) {
	msgApp := message.New(message.Options{
		IdentityStore: &fakeIdentityStore{},
		ChannelStore:  &fakeChannelStore{},
		Now:           func() time.Time { return fixedGatewayNow },
	})
	handler := New(Options{
		Messages: msgApp,
		Now:      func() time.Time { return fixedGatewayNow },
	})

	sender := newOptionRecordingSession(1, "tcp")
	sender.SetValue(coregateway.SessionValueUID, "u1")
	sender.SetValue(coregateway.SessionValueDeviceFlag, wkframe.APP)
	sender.SetValue(coregateway.SessionValueDeviceLevel, wkframe.DeviceLevelMaster)

	recipient := newOptionRecordingSession(2, "tcp")
	recipient.SetValue(coregateway.SessionValueUID, "u2")
	recipient.SetValue(coregateway.SessionValueDeviceFlag, wkframe.APP)
	recipient.SetValue(coregateway.SessionValueDeviceLevel, wkframe.DeviceLevelMaster)

	require.NoError(t, handler.OnSessionOpen(&coregateway.Context{Session: sender, Listener: "tcp"}))
	require.NoError(t, handler.OnSessionOpen(&coregateway.Context{Session: recipient, Listener: "tcp"}))

	err := handler.OnFrame(&coregateway.Context{
		Session:        sender,
		Listener:       "tcp",
		RequestContext: context.Background(),
	}, &wkframe.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   1,
		ClientMsgNo: "m1",
	})
	require.NoError(t, err)

	require.Len(t, sender.Writes(), 1)
	ack := requireSendackPacket(t, sender.Writes()[0].frame)
	require.Equal(t, wkframe.ReasonSuccess, ack.ReasonCode)

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
	sess.SetValue(coregateway.SessionValueDeviceFlag, wkframe.APP)
	sess.SetValue(coregateway.SessionValueDeviceLevel, wkframe.DeviceLevelMaster)

	return &coregateway.Context{
		Session:        sess,
		Listener:       "tcp",
		RequestContext: context.Background(),
	}
}

func requireSendackPacket(t *testing.T, frame wkframe.Frame) *wkframe.SendackPacket {
	t.Helper()

	ack, ok := frame.(*wkframe.SendackPacket)
	require.True(t, ok, "expected *wkframe.SendackPacket, got %T", frame)
	return ack
}

func requireRecvPacket(t *testing.T, frame wkframe.Frame) *wkframe.RecvPacket {
	t.Helper()

	recv, ok := frame.(*wkframe.RecvPacket)
	require.True(t, ok, "expected *wkframe.RecvPacket, got %T", frame)
	return recv
}

type fakeMessageUsecase struct {
	sendCommands    []message.SendCommand
	sendContexts    []context.Context
	sendFn          func(context.Context, message.SendCommand) (message.SendResult, error)
	sendResult      message.SendResult
	sendErr         error
	recvAckCalls    int
	recvAckCommands []message.RecvAckCommand
	recvAckErr      error
}

func (f *fakeMessageUsecase) Send(ctx context.Context, cmd message.SendCommand) (message.SendResult, error) {
	f.sendContexts = append(f.sendContexts, ctx)
	f.sendCommands = append(f.sendCommands, cmd)
	if f.sendFn != nil {
		return f.sendFn(ctx, cmd)
	}
	return f.sendResult, f.sendErr
}

func (f *fakeMessageUsecase) RecvAck(cmd message.RecvAckCommand) error {
	f.recvAckCalls++
	f.recvAckCommands = append(f.recvAckCommands, cmd)
	return f.recvAckErr
}

type outboundWrite struct {
	frame wkframe.Frame
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
		WriteFrameFn: func(frame wkframe.Frame, meta gatewaysession.OutboundMeta) error {
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

func (*fakeIdentityStore) GetUser(context.Context, string) (metadb.User, error) {
	return metadb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (metadb.Channel, error) {
	return metadb.Channel{}, nil
}

var _ online.Registry = (*online.MemoryRegistry)(nil)
