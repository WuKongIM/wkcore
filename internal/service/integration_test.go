package service

import (
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
	"github.com/stretchr/testify/require"
)

const (
	serviceTestListenerName = "tcp-wkproto-service"
	serviceTestSenderUID    = "u1"
	serviceTestRecipientUID = "u2"
	serviceTestClientMsgNo  = "m1"
	serviceTestPayload      = "hi"
	serviceReadTimeout      = 2 * time.Second
)

func TestGatewayWKProtoServiceRoutesLocalPersonSend(t *testing.T) {
	svc := New(Options{})
	gw, err := gateway.New(gateway.Options{
		Handler: svc,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: false,
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto(serviceTestListenerName, "127.0.0.1:0"),
		},
	})
	require.NoError(t, err)
	require.NoError(t, gw.Start())
	t.Cleanup(func() { _ = gw.Stop() })

	senderConn := dialServiceGateway(t, gw, serviceTestListenerName)
	t.Cleanup(func() { _ = senderConn.Close() })

	recipientConn := dialServiceGateway(t, gw, serviceTestListenerName)
	t.Cleanup(func() { _ = recipientConn.Close() })

	senderConnack := connectWKProtoClient(t, senderConn, serviceTestSenderUID)
	require.Equal(t, wkpacket.ReasonSuccess, senderConnack.ReasonCode)

	recipientConnack := connectWKProtoClient(t, recipientConn, serviceTestRecipientUID)
	require.Equal(t, wkpacket.ReasonSuccess, recipientConnack.ReasonCode)

	const clientSeq uint64 = 1
	sendWKProtoFrame(t, senderConn, &wkpacket.SendPacket{
		ChannelID:   serviceTestRecipientUID,
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte(serviceTestPayload),
		ClientSeq:   clientSeq,
		ClientMsgNo: serviceTestClientMsgNo,
	})

	ack := readSendackPacket(t, senderConn)
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, clientSeq, ack.ClientSeq)
	require.Equal(t, serviceTestClientMsgNo, ack.ClientMsgNo)
	require.NotZero(t, ack.MessageID)
	require.NotZero(t, ack.MessageSeq)

	recv := readRecvPacket(t, recipientConn)
	require.Equal(t, serviceTestSenderUID, recv.FromUID)
	require.Equal(t, serviceTestSenderUID, recv.ChannelID)
	require.Equal(t, wkpacket.ChannelTypePerson, recv.ChannelType)
	require.Equal(t, []byte(serviceTestPayload), recv.Payload)
	require.Equal(t, serviceTestClientMsgNo, recv.ClientMsgNo)
	require.Equal(t, ack.MessageID, recv.MessageID)
	require.Equal(t, ack.MessageSeq, recv.MessageSeq)
}

func dialServiceGateway(t *testing.T, gw *gateway.Gateway, listener string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", gw.ListenerAddr(listener))
	require.NoError(t, err)
	return conn
}

func connectWKProtoClient(t *testing.T, conn net.Conn, uid string) *wkpacket.ConnackPacket {
	t.Helper()

	sendWKProtoFrame(t, conn, &wkpacket.ConnectPacket{
		Version:         wkpacket.LatestVersion,
		UID:             uid,
		DeviceID:        uid + "-device",
		DeviceFlag:      wkpacket.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})

	frame := readWKProtoFrame(t, conn)
	connack, ok := frame.(*wkpacket.ConnackPacket)
	require.True(t, ok, "expected *wkpacket.ConnackPacket, got %T", frame)
	return connack
}

func sendWKProtoFrame(t *testing.T, conn net.Conn, frame wkpacket.Frame) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(frame, wkpacket.LatestVersion)
	require.NoError(t, err)

	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readWKProtoFrame(t *testing.T, conn net.Conn) wkpacket.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(serviceReadTimeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkpacket.LatestVersion)
	require.NoError(t, err)
	return frame
}

func readSendackPacket(t *testing.T, conn net.Conn) *wkpacket.SendackPacket {
	t.Helper()

	frame := readWKProtoFrame(t, conn)
	ack, ok := frame.(*wkpacket.SendackPacket)
	require.True(t, ok, "expected *wkpacket.SendackPacket, got %T", frame)
	return ack
}

func readRecvPacket(t *testing.T, conn net.Conn) *wkpacket.RecvPacket {
	t.Helper()

	frame := readWKProtoFrame(t, conn)
	recv, ok := frame.(*wkpacket.RecvPacket)
	require.True(t, ok, "expected *wkpacket.RecvPacket, got %T", frame)
	return recv
}
