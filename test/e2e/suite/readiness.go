//go:build e2e

package suite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const readyPollInterval = 100 * time.Millisecond

// WaitWKProtoReady waits until a real WKProto handshake succeeds on the address.
func WaitWKProtoReady(ctx context.Context, addr string) error {
	ticker := time.NewTicker(readyPollInterval)
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

// WaitHTTPReady waits until the HTTP endpoint starts returning status 200.
func WaitHTTPReady(ctx context.Context, addr, path string) error {
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	var lastErr error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode == http.StatusOK {
				return nil
			} else {
				lastErr = fmt.Errorf("http readiness %s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
			}
		} else {
			lastErr = err
		}

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

// WaitNodeReady waits for both HTTP readiness and a real WKProto handshake.
func WaitNodeReady(ctx context.Context, node StartedNode) error {
	if err := WaitHTTPReady(ctx, node.Spec.APIAddr, "/readyz"); err != nil {
		return err
	}
	return WaitWKProtoReady(ctx, node.Spec.GatewayAddr)
}
