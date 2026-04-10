package cluster

import (
	"fmt"
	"testing"
)

func TestSlotForKey_Deterministic(t *testing.T) {
	r := &Router{slotCount: 3}
	a := r.SlotForKey("test-channel")
	b := r.SlotForKey("test-channel")
	if a != b {
		t.Fatalf("non-deterministic: %d != %d", a, b)
	}
}

func TestSlotForKey_Range(t *testing.T) {
	r := &Router{slotCount: 10}
	for _, id := range []string{"a", "b", "c", "d", "e", "channel-123", "xyz"} {
		slot := r.SlotForKey(id)
		if slot < 1 || slot > 10 {
			t.Fatalf("slot %d out of range [1,10] for channelID=%s", slot, id)
		}
	}
}

func TestSlotForKey_Distribution(t *testing.T) {
	r := &Router{slotCount: 3}
	counts := make(map[uint64]int)
	for i := 0; i < 1000; i++ {
		slot := r.SlotForKey(fmt.Sprintf("channel-%d", i))
		counts[uint64(slot)]++
	}
	for g := uint64(1); g <= 3; g++ {
		if counts[g] == 0 {
			t.Fatalf("slot %d has 0 channels — bad distribution", g)
		}
	}
}

func TestIsLocal(t *testing.T) {
	r := &Router{localNode: 1}
	if !r.IsLocal(1) {
		t.Fatal("expected local")
	}
	if r.IsLocal(2) {
		t.Fatal("expected not local")
	}
}
