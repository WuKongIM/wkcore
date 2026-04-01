package gateway

import (
	"net"
	"testing"
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
	"github.com/stretchr/testify/require"
)

const (
	gatewayTestListenerName = "tcp-wkproto-access-gateway"
	gatewayTestSenderUID    = "u1"
	gatewayTestRecipientUID = "u2"
	gatewayTestClientMsgNo  = "m1"
	gatewayTestPayload      = "hi"
	gatewayReadTimeout      = 2 * time.Second
)

func TestGatewayWKProtoHandlerRoutesLocalPersonSend(t *testing.T) {
	handler := New(Options{})
	gw, err := coregateway.New(coregateway.Options{
		Handler: handler,
		Authenticator: coregateway.NewWKProtoAuthenticator(coregateway.WKProtoAuthOptions{
			TokenAuthOn: false,
		}),
		Listeners: []coregateway.ListenerOptions{
			binding.TCPWKProto(gatewayTestListenerName, "127.0.0.1:0"),
		},
	})
	require.NoError(t, err)
	require.NoError(t, gw.Start())
	t.Cleanup(func() { _ = gw.Stop() })

	senderConn := dialGateway(t, gw, gatewayTestListenerName)
	t.Cleanup(func() { _ = senderConn.Close() })

	recipientConn := dialGateway(t, gw, gatewayTestListenerName)
	t.Cleanup(func() { _ = recipientConn.Close() })

	senderConnack := connectWKProtoClient(t, senderConn, gatewayTestSenderUID)
	require.Equal(t, wkpacket.ReasonSuccess, senderConnack.ReasonCode)

	recipientConnack := connectWKProtoClient(t, recipientConn, gatewayTestRecipientUID)
	require.Equal(t, wkpacket.ReasonSuccess, recipientConnack.ReasonCode)

	const clientSeq uint64 = 1
	sendWKProtoFrame(t, senderConn, &wkpacket.SendPacket{
		ChannelID:   gatewayTestRecipientUID,
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte(gatewayTestPayload),
		ClientSeq:   clientSeq,
		ClientMsgNo: gatewayTestClientMsgNo,
	})

	ack := readSendackPacket(t, senderConn)
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, clientSeq, ack.ClientSeq)
	require.Equal(t, gatewayTestClientMsgNo, ack.ClientMsgNo)
	require.NotZero(t, ack.MessageID)
	require.NotZero(t, ack.MessageSeq)

	recv := readRecvPacket(t, recipientConn)
	require.Equal(t, gatewayTestSenderUID, recv.FromUID)
	require.Equal(t, gatewayTestSenderUID, recv.ChannelID)
	require.Equal(t, wkpacket.ChannelTypePerson, recv.ChannelType)
	require.Equal(t, []byte(gatewayTestPayload), recv.Payload)
	require.Equal(t, gatewayTestClientMsgNo, recv.ClientMsgNo)
	require.Equal(t, ack.MessageID, recv.MessageID)
	require.Equal(t, ack.MessageSeq, recv.MessageSeq)
}

func dialGateway(t *testing.T, gw *coregateway.Gateway, listener string) net.Conn {
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

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(gatewayReadTimeout)))
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
