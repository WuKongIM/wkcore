package app

import (
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
	"github.com/stretchr/testify/require"
)

const appReadTimeout = 2 * time.Second

func TestAppStartAcceptsWKProtoConnectionAndStopsCleanly(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, app.Start())

	conn, err := net.Dial("tcp", app.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sendAppWKProtoFrame(t, conn, &wkpacket.ConnectPacket{
		Version:         wkpacket.LatestVersion,
		UID:             "app-user",
		DeviceID:        "app-device",
		DeviceFlag:      wkpacket.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})

	frame := readAppWKProtoFrame(t, conn)
	connack, ok := frame.(*wkpacket.ConnackPacket)
	require.True(t, ok, "expected *wkpacket.ConnackPacket, got %T", frame)
	require.Equal(t, wkpacket.ReasonSuccess, connack.ReasonCode)

	require.NoError(t, app.Stop())
}

func sendAppWKProtoFrame(t *testing.T, conn net.Conn, frame wkpacket.Frame) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(frame, wkpacket.LatestVersion)
	require.NoError(t, err)

	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readAppWKProtoFrame(t *testing.T, conn net.Conn) wkpacket.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(appReadTimeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkpacket.LatestVersion)
	require.NoError(t, err)
	return frame
}
