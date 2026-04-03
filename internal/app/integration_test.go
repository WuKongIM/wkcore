package app

import (
	"net"
	"testing"
	"time"

	codec "github.com/WuKongIM/WuKongIM/pkg/proto/wkproto"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

const appReadTimeout = 2 * time.Second

func TestAppStartAcceptsWKProtoConnectionAndStopsCleanly(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	conn, err := net.Dial("tcp", app.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sendAppWKProtoFrame(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "app-user",
		DeviceID:        "app-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})

	frame := readAppWKProtoFrame(t, conn)
	connack, ok := frame.(*wkframe.ConnackPacket)
	require.True(t, ok, "expected *wkframe.ConnackPacket, got %T", frame)
	require.Equal(t, wkframe.ReasonSuccess, connack.ReasonCode)
}

func sendAppWKProtoFrame(t *testing.T, conn net.Conn, frame wkframe.Frame) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(frame, wkframe.LatestVersion)
	require.NoError(t, err)

	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readAppWKProtoFrame(t *testing.T, conn net.Conn) wkframe.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(appReadTimeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkframe.LatestVersion)
	require.NoError(t, err)
	return frame
}
