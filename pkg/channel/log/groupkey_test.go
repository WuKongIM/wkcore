package log

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func TestChannelGroupKeyIsDeterministicAndEscaped(t *testing.T) {
	key := ChannelKey{ChannelID: "a/b:c", ChannelType: 3}
	got := channelGroupKey(key)
	if got != isr.GroupKey("channel/3/YS9iOmM") {
		t.Fatalf("channelGroupKey() = %q", got)
	}
}
