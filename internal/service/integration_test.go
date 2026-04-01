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

func TestGatewayWKProtoServiceRoutesLocalPersonSend(t *testing.T) {
	svc := New(Options{})
	gw, err := gateway.New(gateway.Options{
		Handler: svc,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: false,
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-service", "127.0.0.1:0"),
		},
	})
	require.NoError(t, err)
	require.NoError(t, gw.Start())
	t.Cleanup(func() { _ = gw.Stop() })

	senderConn := dialServiceGateway(t, gw, "tcp-wkproto-service")
	t.Cleanup(func() { _ = senderConn.Close() })

	recipientConn := dialServiceGateway(t, gw, "tcp-wkproto-service")
	t.Cleanup(func() { _ = recipientConn.Close() })

	senderConnack := connectWKProtoClient(t, senderConn, "u1")
	require.Equal(t, wkpacket.ReasonSuccess, senderConnack.ReasonCode)

	recipientConnack := connectWKProtoClient(t, recipientConn, "u2")
	require.Equal(t, wkpacket.ReasonSuccess, recipientConnack.ReasonCode)

	sendWKProtoFrame(t, senderConn, &wkpacket.SendPacket{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   1,
		ClientMsgNo: "m1",
	})

	ack := readSendackPacket(t, senderConn)
	require.Equal(t, wkpacket.ReasonSuccess, ack.ReasonCode)

	recv := readRecvPacket(t, recipientConn)
	require.Equal(t, "u1", recv.FromUID)
	require.Equal(t, "u1", recv.ChannelID)
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

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
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
