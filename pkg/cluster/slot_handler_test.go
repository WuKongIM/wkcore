package cluster

import (
	"context"
	"testing"
)

func TestSlotHandlerRejectsUnknownKind(t *testing.T) {
	handler := &slotHandler{cluster: &Cluster{}}
	body := make([]byte, managedSlotRequestSize)
	body[0] = managedSlotCodecVersion

	_, err := handler.Handle(context.Background(), body)
	if err != ErrInvalidConfig {
		t.Fatalf("slotHandler.Handle() error = %v, want %v", err, ErrInvalidConfig)
	}
}
