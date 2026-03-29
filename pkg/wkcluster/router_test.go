package wkcluster

import (
	"fmt"
	"testing"
)

func TestSlotForChannel_Deterministic(t *testing.T) {
	r := &Router{groupCount: 3}
	a := r.SlotForChannel("test-channel")
	b := r.SlotForChannel("test-channel")
	if a != b {
		t.Fatalf("non-deterministic: %d != %d", a, b)
	}
}

func TestSlotForChannel_Range(t *testing.T) {
	r := &Router{groupCount: 10}
	for _, id := range []string{"a", "b", "c", "d", "e", "channel-123", "xyz"} {
		slot := r.SlotForChannel(id)
		if slot < 1 || slot > 10 {
			t.Fatalf("slot %d out of range [1,10] for channelID=%s", slot, id)
		}
	}
}

func TestSlotForChannel_Distribution(t *testing.T) {
	r := &Router{groupCount: 3}
	counts := make(map[uint64]int)
	for i := 0; i < 1000; i++ {
		slot := r.SlotForChannel(fmt.Sprintf("channel-%d", i))
		counts[uint64(slot)]++
	}
	for g := uint64(1); g <= 3; g++ {
		if counts[g] == 0 {
			t.Fatalf("group %d has 0 channels — bad distribution", g)
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
