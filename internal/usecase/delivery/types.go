package delivery

import (
	runtimedelivery "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
)

func committedEnvelopeFromMessage(env message.CommittedMessageEnvelope) runtimedelivery.CommittedEnvelope {
	return runtimedelivery.CommittedEnvelope{
		ChannelID:   env.ChannelID,
		ChannelType: env.ChannelType,
		MessageID:   env.MessageID,
		MessageSeq:  env.MessageSeq,
		SenderUID:   env.SenderUID,
		ClientMsgNo: env.ClientMsgNo,
		Topic:       env.Topic,
		Payload:     append([]byte(nil), env.Payload...),
		Framer:      env.Framer,
		Setting:     env.Setting,
		MsgKey:      env.MsgKey,
		Expire:      env.Expire,
		StreamNo:    env.StreamNo,
		ClientSeq:   env.ClientSeq,
	}
}

func routeAckFromMessage(cmd message.RouteAckCommand) runtimedelivery.RouteAck {
	return runtimedelivery.RouteAck{
		UID:        cmd.UID,
		SessionID:  cmd.SessionID,
		MessageID:  cmd.MessageID,
		MessageSeq: cmd.MessageSeq,
	}
}

func sessionClosedFromMessage(cmd message.SessionClosedCommand) runtimedelivery.SessionClosed {
	return runtimedelivery.SessionClosed{
		UID:       cmd.UID,
		SessionID: cmd.SessionID,
	}
}
