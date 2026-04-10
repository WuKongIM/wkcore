package log

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func TestChannelChannelKeyIsDeterministicAndEscaped(t *testing.T) {
	key := ChannelKey{ChannelID: "a/b:c", ChannelType: 3}
	got := isrChannelKeyForChannel(key)
	if got != isr.ChannelKey("channel/3/YS9iOmM") {
		t.Fatalf("isrChannelKeyForChannel() = %q", got)
	}
}
