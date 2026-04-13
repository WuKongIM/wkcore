package handler

import (
	"encoding/base64"
	"strconv"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

const keyPrefix = "channel/"

func KeyFromChannelID(id channel.ChannelID) channel.ChannelKey {
	encodedID := base64.RawURLEncoding.EncodeToString([]byte(id.ID))
	buf := make([]byte, 0, len(keyPrefix)+4+1+len(encodedID))
	buf = append(buf, keyPrefix...)
	buf = strconv.AppendUint(buf, uint64(id.Type), 10)
	buf = append(buf, '/')
	buf = append(buf, encodedID...)
	return channel.ChannelKey(buf)
}
