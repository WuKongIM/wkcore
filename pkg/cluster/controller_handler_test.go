package cluster

import (
	"context"
	"testing"
	"time"

	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

func TestControllerHandlerAcceptsBinaryRequestBeforeHostChecks(t *testing.T) {
	handler := &controllerHandler{cluster: &Cluster{}}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCHeartbeat,
		Report: &slotcontroller.AgentReport{
			NodeID:         1,
			Addr:           "127.0.0.1:1111",
			ObservedAt:     time.Unix(1710000000, 0),
			CapacityWeight: 1,
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	_, err = handler.Handle(context.Background(), body)
	if err != ErrNotStarted {
		t.Fatalf("controllerHandler.Handle() error = %v, want %v", err, ErrNotStarted)
	}
}
