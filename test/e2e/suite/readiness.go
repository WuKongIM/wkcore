//go:build e2e

package suite

import (
	"context"
	"time"
)

// WaitWKProtoReady waits until a real WKProto handshake succeeds on the address.
func WaitWKProtoReady(ctx context.Context, addr string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		client, err := NewWKProtoClient()
		if err != nil {
			return err
		}

		_, err = client.ConnectContext(ctx, addr, "e2e-ready", "e2e-ready-device")
		_ = client.Close()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
