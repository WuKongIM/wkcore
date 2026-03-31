package stdnet_test

import (
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	"github.com/WuKongIM/WuKongIM/internal/gateway/transport/stdnet"
	"github.com/gorilla/websocket"
)

func TestWSListenerUpgradesAndDeliversMessages(t *testing.T) {
	handler := newConnRecordingHandler()
	listener, err := stdnet.NewWSListener(transport.ListenerOptions{
		Name:    "ws-jsonrpc",
		Address: "127.0.0.1:0",
		Path:    "/ws",
	}, handler)
	if err != nil {
		t.Fatalf("NewWSListener: %v", err)
	}
	defer func() { _ = listener.Stop() }()

	if err := listener.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	url := "ws://" + strings.TrimPrefix(listener.Addr(), "http://") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","method":"ping","id":"1"}`)); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	waitUntil(t, time.Second, func() bool {
		return handler.OpenCount() == 1 && handler.DataCount() == 1
	})
}
