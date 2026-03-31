package gateway_test

import (
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/internal/gateway/testkit"
	pkgjsonrpc "github.com/WuKongIM/WuKongIM/pkg/jsonrpc"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
	"github.com/gorilla/websocket"
)

func TestGatewayStartStopTCPWKProto(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn, err := net.Dial("tcp", gw.ListenerAddr("tcp-wkproto"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	payload, err := codec.New().EncodeFrame(&wkpacket.PingPacket{}, wkpacket.LatestVersion)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitUntil(t, time.Second, func() bool { return handler.FrameCount() == 1 })
}

func TestGatewayStartStopWSJSONRPC(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Listeners: []gateway.ListenerOptions{
			binding.WSJSONRPC("ws-jsonrpc", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	url := "ws://" + gw.ListenerAddr("ws-jsonrpc") + binding.DefaultWSPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	payload, err := pkgjsonrpc.Encode(pkgjsonrpc.PingRequest{
		BaseRequest: pkgjsonrpc.BaseRequest{
			Jsonrpc: "2.0",
			Method:  pkgjsonrpc.MethodPing,
			ID:      "1",
		},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	waitUntil(t, time.Second, func() bool { return handler.FrameCount() == 1 })
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}
