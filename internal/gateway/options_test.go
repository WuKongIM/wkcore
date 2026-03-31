package gateway_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type noopHandler struct{}

func (noopHandler) OnListenerError(string, error)                  {}
func (noopHandler) OnSessionOpen(*gateway.Context) error           { return nil }
func (noopHandler) OnFrame(*gateway.Context, wkpacket.Frame) error { return nil }
func (noopHandler) OnSessionClose(*gateway.Context) error          { return nil }
func (noopHandler) OnSessionError(*gateway.Context, error)         {}

func TestOptionsValidateRejectsDuplicateListenerNames(t *testing.T) {
	opts := gateway.Options{
		Handler: noopHandler{},
		Listeners: []gateway.ListenerOptions{
			{Name: "dup", Network: "tcp", Address: ":5100", Transport: "stdnet", Protocol: "wkproto"},
			{Name: "dup", Network: "websocket", Address: ":5200", Transport: "stdnet", Protocol: "jsonrpc"},
		},
	}
	if err := opts.Validate(); err == nil {
		t.Fatal("expected duplicate listener validation error")
	}
}

func TestBuiltinPresetsPopulateCanonicalFields(t *testing.T) {
	tcp := binding.TCPWKProto("tcp-wkproto", ":5100")
	if tcp.Network != "tcp" || tcp.Transport != "stdnet" || tcp.Protocol != "wkproto" {
		t.Fatalf("unexpected tcp preset: %+v", tcp)
	}

	ws := binding.WSJSONRPC("ws-jsonrpc", ":5200")
	if ws.Network != "websocket" || ws.Transport != "stdnet" || ws.Protocol != "jsonrpc" || ws.Path != binding.DefaultWSPath {
		t.Fatalf("unexpected ws preset: %+v", ws)
	}
}

func TestOptionsValidateNormalizesDefaultSession(t *testing.T) {
	opts := gateway.Options{Handler: noopHandler{}}
	if err := opts.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if !opts.DefaultSession.CloseOnHandlerError {
		t.Fatal("expected default CloseOnHandlerError to be true")
	}
	if opts.DefaultSession.ReadBufferSize == 0 || opts.DefaultSession.WriteQueueSize == 0 || opts.DefaultSession.IdleTimeout == 0 {
		t.Fatalf("expected default session fields to be populated: %+v", opts.DefaultSession)
	}
}

func TestOptionsValidateNormalizesPartialSessionOverrides(t *testing.T) {
	opts := gateway.Options{
		Handler: noopHandler{},
		DefaultSession: gateway.SessionOptions{
			ReadBufferSize: 8192,
		},
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if opts.DefaultSession.ReadBufferSize != 8192 {
		t.Fatalf("expected custom read buffer size to be preserved, got %+v", opts.DefaultSession)
	}
	if !opts.DefaultSession.CloseOnHandlerError {
		t.Fatalf("expected CloseOnHandlerError to remain true after normalization, got %+v", opts.DefaultSession)
	}
}

func TestOptionsValidateRequiresWebsocketPath(t *testing.T) {
	opts := gateway.Options{
		Handler: noopHandler{},
		Listeners: []gateway.ListenerOptions{
			{Name: "ws", Network: "websocket", Address: ":5200", Transport: "stdnet", Protocol: "jsonrpc"},
		},
	}
	if err := opts.Validate(); err == nil {
		t.Fatal("expected websocket listener path validation error")
	}
}

func TestOptionsValidateTrimsListenerFieldsInPlace(t *testing.T) {
	opts := gateway.Options{
		Handler: noopHandler{},
		Listeners: []gateway.ListenerOptions{
			{
				Name:      "  ws-jsonrpc  ",
				Network:   "  websocket  ",
				Address:   "  :5200  ",
				Path:      "  /ws  ",
				Transport: "  stdnet  ",
				Protocol:  "  jsonrpc  ",
			},
		},
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	got := opts.Listeners[0]
	if got.Name != "ws-jsonrpc" || got.Network != "websocket" || got.Address != ":5200" || got.Path != "/ws" || got.Transport != "stdnet" || got.Protocol != "jsonrpc" {
		t.Fatalf("expected listener fields to be trimmed in place, got %+v", got)
	}
}
