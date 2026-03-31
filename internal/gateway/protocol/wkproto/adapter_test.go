package wkproto_test

import (
	"testing"

	adapterpkg "github.com/WuKongIM/WuKongIM/internal/gateway/protocol/wkproto"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/gateway/testkit"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
)

func TestAdapterDecodeReturnsZeroUntilFrameIsComplete(t *testing.T) {
	adapter := adapterpkg.New()
	sess := testkit.NewProtocolSession()

	wire, err := codec.New().EncodeFrame(&wkpacket.PingPacket{}, wkpacket.LatestVersion)
	if err != nil {
		t.Fatalf("encode frame failed: %v", err)
	}

	frames, consumed, err := adapter.Decode(sess, wire[:0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(frames) != 0 || consumed != 0 {
		t.Fatalf("expected no progress for incomplete frame, got frames=%d consumed=%d", len(frames), consumed)
	}
}

func TestAdapterEncodeRoundTrip(t *testing.T) {
	adapter := adapterpkg.New()
	sess := testkit.NewProtocolSession()

	encoded, err := adapter.Encode(sess, &wkpacket.PingPacket{}, session.OutboundMeta{})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	frames, consumed, err := adapter.Decode(sess, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("expected consumed=%d, got %d", len(encoded), consumed)
	}
	if len(frames) != 1 {
		t.Fatalf("expected one frame, got %d", len(frames))
	}
	if _, ok := frames[0].(*wkpacket.PingPacket); !ok {
		t.Fatalf("expected ping packet, got %T", frames[0])
	}
}

func TestAdapterDecodeStickyFrames(t *testing.T) {
	adapter := adapterpkg.New()
	sess := testkit.NewProtocolSession()

	codec := codec.New()
	first, err := codec.EncodeFrame(&wkpacket.PingPacket{}, wkpacket.LatestVersion)
	if err != nil {
		t.Fatalf("encode first frame failed: %v", err)
	}
	second, err := codec.EncodeFrame(&wkpacket.PongPacket{}, wkpacket.LatestVersion)
	if err != nil {
		t.Fatalf("encode second frame failed: %v", err)
	}

	wire := append(append([]byte(nil), first...), second...)
	frames, consumed, err := adapter.Decode(sess, wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if consumed != len(wire) {
		t.Fatalf("expected consumed=%d, got %d", len(wire), consumed)
	}
	if len(frames) != 2 {
		t.Fatalf("expected two frames, got %d", len(frames))
	}
}
