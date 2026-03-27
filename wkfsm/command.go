package wkfsm

import (
	"encoding/json"

	"github.com/WuKongIM/wraft/wkdb"
)

const (
	commandTypeUpsertUser    = "upsert_user"
	commandTypeUpsertChannel = "upsert_channel"
	applyResultOK            = "ok"
)

type commandEnvelope struct {
	Type    string        `json:"type"`
	User    *wkdb.User    `json:"user,omitempty"`
	Channel *wkdb.Channel `json:"channel,omitempty"`
}

func EncodeUpsertUserCommand(user wkdb.User) []byte {
	data, _ := json.Marshal(commandEnvelope{
		Type: commandTypeUpsertUser,
		User: &user,
	})
	return data
}

func EncodeUpsertChannelCommand(channel wkdb.Channel) []byte {
	data, _ := json.Marshal(commandEnvelope{
		Type:    commandTypeUpsertChannel,
		Channel: &channel,
	})
	return data
}
