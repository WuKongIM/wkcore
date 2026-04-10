package log

import (
	"encoding/base64"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func isrChannelKeyForChannel(key ChannelKey) isr.ChannelKey {
	return isr.ChannelKey(fmt.Sprintf(
		"channel/%d/%s",
		key.ChannelType,
		base64.RawURLEncoding.EncodeToString([]byte(key.ChannelID)),
	))
}

func ISRChannelKeyForChannel(key ChannelKey) isr.ChannelKey {
	return isrChannelKeyForChannel(key)
}
