package gateway_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

func TestAuthenticatorStoresNegotiatedProtocolVersion(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{})

	result, err := auth.Authenticate(nil, &wkframe.ConnectPacket{
		Version: 5,
		UID:     "u1",
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.SessionValues[gateway.SessionValueProtocolVersion] != uint8(5) {
		t.Fatalf("protocol version = %#v, want 5", result.SessionValues[gateway.SessionValueProtocolVersion])
	}
}
