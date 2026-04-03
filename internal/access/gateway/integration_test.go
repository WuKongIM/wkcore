package gateway

import (
	"net"
	"testing"
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/proto/wkproto"
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

func TestGatewayVersion5ClientGetsUpgradeRequiredOnSend(t *testing.T) {
	handler := New(Options{
		Messages: &fakeMessageUsecase{sendErr: channelcluster.ErrProtocolUpgradeRequired},
	})
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

	conn := dialGateway(t, gw, gatewayTestListenerName)
	t.Cleanup(func() { _ = conn.Close() })

	connack := connectWKProtoClientVersion(t, conn, gatewayTestSenderUID, wkpacket.LegacyMessageSeqVersion)
	require.Equal(t, wkpacket.ReasonSuccess, connack.ReasonCode)
	require.Equal(t, uint8(wkpacket.LegacyMessageSeqVersion), connack.ServerVersion)

	sendWKProtoFrameVersion(t, conn, &wkpacket.SendPacket{
		ChannelID:   gatewayTestRecipientUID,
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte(gatewayTestPayload),
		ClientSeq:   1,
		ClientMsgNo: gatewayTestClientMsgNo,
	}, wkpacket.LegacyMessageSeqVersion)

	ack := readSendackPacketVersion(t, conn, wkpacket.LegacyMessageSeqVersion)
	require.Equal(t, wkpacket.ReasonProtocolUpgradeRequired, ack.ReasonCode)
	require.Zero(t, ack.MessageID)
	require.Zero(t, ack.MessageSeq)
	require.Equal(t, uint64(1), ack.ClientSeq)
	require.Equal(t, gatewayTestClientMsgNo, ack.ClientMsgNo)
}

func dialGateway(t *testing.T, gw *coregateway.Gateway, listener string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", gw.ListenerAddr(listener))
	require.NoError(t, err)
	return conn
}

func connectWKProtoClient(t *testing.T, conn net.Conn, uid string) *wkpacket.ConnackPacket {
	t.Helper()
	return connectWKProtoClientVersion(t, conn, uid, wkpacket.LatestVersion)
}

func connectWKProtoClientVersion(t *testing.T, conn net.Conn, uid string, version uint8) *wkpacket.ConnackPacket {
	t.Helper()

	sendWKProtoFrameVersion(t, conn, &wkpacket.ConnectPacket{
		Version:         version,
		UID:             uid,
		DeviceID:        uid + "-device",
		DeviceFlag:      wkpacket.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	}, wkpacket.LatestVersion)

	frame := readWKProtoFrameVersion(t, conn, wkpacket.LatestVersion)
	connack, ok := frame.(*wkpacket.ConnackPacket)
	require.True(t, ok, "expected *wkpacket.ConnackPacket, got %T", frame)
	return connack
}

func sendWKProtoFrame(t *testing.T, conn net.Conn, frame wkpacket.Frame) {
	t.Helper()
	sendWKProtoFrameVersion(t, conn, frame, wkpacket.LatestVersion)
}

func sendWKProtoFrameVersion(t *testing.T, conn net.Conn, frame wkpacket.Frame, version uint8) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(frame, version)
	require.NoError(t, err)

	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readWKProtoFrame(t *testing.T, conn net.Conn) wkpacket.Frame {
	t.Helper()
	return readWKProtoFrameVersion(t, conn, wkpacket.LatestVersion)
}

func readWKProtoFrameVersion(t *testing.T, conn net.Conn, version uint8) wkpacket.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(gatewayReadTimeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, version)
	require.NoError(t, err)
	return frame
}

func readSendackPacket(t *testing.T, conn net.Conn) *wkpacket.SendackPacket {
	t.Helper()
	return readSendackPacketVersion(t, conn, wkpacket.LatestVersion)
}

func readSendackPacketVersion(t *testing.T, conn net.Conn, version uint8) *wkpacket.SendackPacket {
	t.Helper()

	frame := readWKProtoFrameVersion(t, conn, version)
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
