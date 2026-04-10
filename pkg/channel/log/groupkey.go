package log

import (
	"encoding/base64"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func channelGroupKey(key ChannelKey) isr.GroupKey {
	return isr.GroupKey(fmt.Sprintf(
		"channel/%d/%s",
		key.ChannelType,
		base64.RawURLEncoding.EncodeToString([]byte(key.ChannelID)),
	))
}

func GroupKeyForChannel(key ChannelKey) isr.GroupKey {
	return channelGroupKey(key)
}
