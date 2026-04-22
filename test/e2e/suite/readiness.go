//go:build e2e

package suite

import "context"

// WaitWKProtoReady waits until a real WKProto handshake succeeds on the address.
func WaitWKProtoReady(ctx context.Context, addr string) error {
	client, err := NewWKProtoClient()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	_, err = client.ConnectContext(ctx, addr, "e2e-ready", "e2e-ready-device")
	return err
}
