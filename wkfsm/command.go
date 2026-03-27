package wkfsm

import (
	"encoding/json"

	"github.com/WuKongIM/wraft/wkdb"
)

type commandEnvelope struct {
	Type    string        `json:"type"`
	User    *wkdb.User    `json:"user,omitempty"`
	Channel *wkdb.Channel `json:"channel,omitempty"`
}

func EncodeUpsertUserCommand(user wkdb.User) []byte {
	data, _ := json.Marshal(commandEnvelope{
		Type: "upsert_user",
		User: &user,
	})
	return data
}

func EncodeUpsertChannelCommand(channel wkdb.Channel) []byte {
	data, _ := json.Marshal(commandEnvelope{
		Type:    "upsert_channel",
		Channel: &channel,
	})
	return data
}
