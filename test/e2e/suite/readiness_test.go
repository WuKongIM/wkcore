//go:build e2e

package suite

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/wkprotoenc"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/codec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestWaitWKProtoReadyRejectsBareTCPListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = WaitWKProtoReady(ctx, ln.Addr().String())
	require.Error(t, err)
}

func TestWaitWKProtoReadyAcceptsSuccessfulConnack(t *testing.T) {
	ln := newWKProtoTestServer(t, func(conn net.Conn) {
		connect := readConnectPacket(t, conn)
		serverKeys, serverKey, err := wkprotoenc.NegotiateServerSession(connect.ClientKey)
		require.NoError(t, err)
		writeFrame(t, conn, &frame.ConnackPacket{
			ServerVersion: frame.LatestVersion,
			ServerKey:     serverKey,
			Salt:          string(serverKeys.AESIV),
			ReasonCode:    frame.ReasonSuccess,
		})
	})
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, WaitWKProtoReady(ctx, ln.Addr().String()))
}

func newWKProtoTestServer(t *testing.T, handler func(net.Conn)) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				handler(conn)
			}()
		}
	}()

	return ln
}

func readConnectPacket(t *testing.T, conn net.Conn) *frame.ConnectPacket {
	t.Helper()

	f, err := codec.New().DecodePacketWithConn(conn, frame.LatestVersion)
	require.NoError(t, err)

	connect, ok := f.(*frame.ConnectPacket)
	require.True(t, ok, "expected *frame.ConnectPacket, got %T", f)
	return connect
}

func writeFrame(t *testing.T, conn net.Conn, f frame.Frame) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(f, frame.LatestVersion)
	require.NoError(t, err)
	_, err = conn.Write(payload)
	require.NoError(t, err)
}
