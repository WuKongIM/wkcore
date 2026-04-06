package gateway_test

import (
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/WuKongIM/WuKongIM/internal/gateway/testkit"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	pkgjsonrpc "github.com/WuKongIM/WuKongIM/pkg/protocol/wkjsonrpc"
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

	payload, err := codec.New().EncodeFrame(&wkframe.PingPacket{}, wkframe.LatestVersion)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitUntil(t, time.Second, func() bool { return handler.FrameCount() == 1 })
}

func TestGatewayWKProtoAuthRejectsBadToken(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: true,
			VerifyToken: func(uid string, deviceFlag wkframe.DeviceFlag, token string) (wkframe.DeviceLevel, error) {
				if uid == "u1" && token == "good-token" {
					return wkframe.DeviceLevelMaster, nil
				}
				return 0, errors.New("token verify fail")
			},
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-auth", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn := dialTCPGateway(t, gw, "tcp-wkproto-auth")
	t.Cleanup(func() { _ = conn.Close() })

	connack := mustConnectWKProto(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "u1",
		Token:           "bad-token",
		DeviceID:        "d1",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	if connack.ReasonCode != wkframe.ReasonAuthFail {
		t.Fatalf("expected auth fail connack, got %v", connack.ReasonCode)
	}
	assertConnClosed(t, conn)
	if got := handler.FrameCount(); got != 0 {
		t.Fatalf("expected handler to see no frames, got %d", got)
	}
}

func TestGatewayWKProtoAuthRejectsBannedUID(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: false,
			IsBanned: func(uid string) (bool, error) {
				return uid == "banned-user", nil
			},
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-ban", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn := dialTCPGateway(t, gw, "tcp-wkproto-ban")
	t.Cleanup(func() { _ = conn.Close() })

	connack := mustConnectWKProto(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "banned-user",
		DeviceID:        "d1",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	if connack.ReasonCode != wkframe.ReasonBan {
		t.Fatalf("expected ban connack, got %v", connack.ReasonCode)
	}
	assertConnClosed(t, conn)
	if got := handler.FrameCount(); got != 0 {
		t.Fatalf("expected handler to see no frames, got %d", got)
	}
}

func TestGatewayWKProtoAuthAcceptsConnectBeforeDispatchingFrames(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: true,
			NodeID:      42,
			Now: func() time.Time {
				return time.UnixMilli(10_000)
			},
			VerifyToken: func(uid string, deviceFlag wkframe.DeviceFlag, token string) (wkframe.DeviceLevel, error) {
				if uid == "u1" && token == "good-token" {
					return wkframe.DeviceLevelMaster, nil
				}
				return 0, errors.New("token verify fail")
			},
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-auth-ok", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn := dialTCPGateway(t, gw, "tcp-wkproto-auth-ok")
	t.Cleanup(func() { _ = conn.Close() })

	connack := mustConnectWKProto(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "u1",
		Token:           "good-token",
		DeviceID:        "d1",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: 9_000,
	})
	if connack.ReasonCode != wkframe.ReasonSuccess {
		t.Fatalf("expected success connack, got %v", connack.ReasonCode)
	}
	if connack.NodeId != 42 || connack.TimeDiff != 1000 {
		t.Fatalf("unexpected connack: %+v", connack)
	}

	payload, err := codec.New().EncodeFrame(&wkframe.PingPacket{}, wkframe.LatestVersion)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitUntil(t, time.Second, func() bool { return handler.FrameCount() == 1 })
	if got := handler.Contexts[0].Session.Value(gateway.SessionValueUID); got != "u1" {
		t.Fatalf("expected session uid to be stored, got %#v", got)
	}
	if got := handler.Contexts[0].Session.Value(gateway.SessionValueDeviceLevel); got != wkframe.DeviceLevelMaster {
		t.Fatalf("expected device level to be stored, got %#v", got)
	}
}

func TestGatewayWKProtoActivationSeesDeviceIDBeforeConnectSucceeds(t *testing.T) {
	handler := &activationHandler{}
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Authenticator: gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
			TokenAuthOn: true,
			VerifyToken: func(uid string, deviceFlag wkframe.DeviceFlag, token string) (wkframe.DeviceLevel, error) {
				if uid == "u1" && token == "good-token" {
					return wkframe.DeviceLevelMaster, nil
				}
				return 0, errors.New("token verify fail")
			},
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-activate", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn := dialTCPGateway(t, gw, "tcp-wkproto-activate")
	t.Cleanup(func() { _ = conn.Close() })

	connack := mustConnectWKProto(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "u1",
		Token:           "good-token",
		DeviceID:        "d1",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	if connack.ReasonCode != wkframe.ReasonSuccess {
		t.Fatalf("expected success connack, got %v", connack.ReasonCode)
	}

	waitUntil(t, time.Second, func() bool { return handler.openSeen() })
	if got := handler.deviceID(); got != "d1" {
		t.Fatalf("expected activation to see device id, got %#v", got)
	}
	if got := handler.callOrder(); !reflect.DeepEqual(got, []string{"activate", "open"}) {
		t.Fatalf("unexpected call order: %v", got)
	}
}

func TestGatewayWKProtoActivationSeesDeviceIDWithCustomAuthenticator(t *testing.T) {
	handler := &activationHandler{}
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Authenticator: gateway.AuthenticatorFunc(func(*gateway.Context, *wkframe.ConnectPacket) (*gateway.AuthResult, error) {
			return &gateway.AuthResult{
				Connack: &wkframe.ConnackPacket{ReasonCode: wkframe.ReasonSuccess},
			}, nil
		}),
		Listeners: []gateway.ListenerOptions{
			binding.TCPWKProto("tcp-wkproto-custom-auth", "127.0.0.1:0"),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn := dialTCPGateway(t, gw, "tcp-wkproto-custom-auth")
	t.Cleanup(func() { _ = conn.Close() })

	connack := mustConnectWKProto(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "u1",
		DeviceID:        "d1",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})
	if connack.ReasonCode != wkframe.ReasonSuccess {
		t.Fatalf("expected success connack, got %v", connack.ReasonCode)
	}

	waitUntil(t, time.Second, func() bool { return handler.openSeen() })
	if got := handler.deviceID(); got != "d1" {
		t.Fatalf("expected activation to see device id, got %#v", got)
	}
	if got := handler.callOrder(); !reflect.DeepEqual(got, []string{"activate", "open"}) {
		t.Fatalf("unexpected call order: %v", got)
	}
}

type activationHandler struct {
	mu            sync.Mutex
	order         []string
	deviceIDValue string
	open          bool
}

func (h *activationHandler) OnListenerError(string, error) {}

func (h *activationHandler) OnSessionActivate(ctx *gateway.Context) (*wkframe.ConnackPacket, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.order = append(h.order, "activate")
	if ctx != nil && ctx.Session != nil {
		if got := ctx.Session.Value(gateway.SessionValueDeviceID); got != nil {
			h.deviceIDValue, _ = got.(string)
		}
	}
	return nil, nil
}

func (h *activationHandler) OnSessionOpen(ctx *gateway.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.order = append(h.order, "open")
	h.open = true
	return nil
}

func (h *activationHandler) OnFrame(*gateway.Context, wkframe.Frame) error { return nil }
func (h *activationHandler) OnSessionClose(*gateway.Context) error         { return nil }
func (h *activationHandler) OnSessionError(*gateway.Context, error)        {}

func (h *activationHandler) deviceID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deviceIDValue
}

func (h *activationHandler) openSeen() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.open
}

func (h *activationHandler) callOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.order...)
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

func TestGatewayStartStopTCPWKProtoOverStdnet(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Listeners: []gateway.ListenerOptions{
			{
				Name:      "tcp-wkproto-stdnet",
				Network:   "tcp",
				Address:   "127.0.0.1:0",
				Transport: "stdnet",
				Protocol:  "wkproto",
			},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	conn, err := net.Dial("tcp", gw.ListenerAddr("tcp-wkproto-stdnet"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	payload, err := codec.New().EncodeFrame(&wkframe.PingPacket{}, wkframe.LatestVersion)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitUntil(t, time.Second, func() bool { return handler.FrameCount() == 1 })
}

func TestGatewayStartStopWSJSONRPCOverStdnet(t *testing.T) {
	handler := testkit.NewRecordingHandler()
	gw, err := gateway.New(gateway.Options{
		Handler: handler,
		Listeners: []gateway.ListenerOptions{
			{
				Name:      "ws-jsonrpc-stdnet",
				Network:   "websocket",
				Address:   "127.0.0.1:0",
				Path:      binding.DefaultWSPath,
				Transport: "stdnet",
				Protocol:  "jsonrpc",
			},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop() })

	url := "ws://" + gw.ListenerAddr("ws-jsonrpc-stdnet") + binding.DefaultWSPath
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

func dialTCPGateway(t *testing.T, gw *gateway.Gateway, listener string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", gw.ListenerAddr(listener))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return conn
}

func mustConnectWKProto(t *testing.T, conn net.Conn, connect *wkframe.ConnectPacket) *wkframe.ConnackPacket {
	t.Helper()

	payload, err := codec.New().EncodeFrame(connect, wkframe.LatestVersion)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkframe.LatestVersion)
	if err != nil {
		t.Fatalf("DecodePacketWithConn: %v", err)
	}
	connack, ok := frame.(*wkframe.ConnackPacket)
	if !ok {
		t.Fatalf("expected connack packet, got %T", frame)
	}
	return connack
}

func assertConnClosed(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("expected closed connection, got timeout: %v", err)
	}
	if !errors.Is(err, io.EOF) {
		return
	}
}
