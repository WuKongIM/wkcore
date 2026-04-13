package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestKeyFromChannelID(t *testing.T) {
	id := channel.ChannelID{ID: "a/b:c", Type: 3}

	got := KeyFromChannelID(id)
	want := channel.ChannelKey("channel/3/YS9iOmM")
	if got != want {
		t.Fatalf("KeyFromChannelID() = %q, want %q", got, want)
	}
}
