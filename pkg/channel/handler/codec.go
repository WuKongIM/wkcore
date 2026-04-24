package handler

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/store"
)

type messageView = channel.DurableMessageView

var errUnknownMessageCodecVersion = errors.New("channelhandler: unknown message codec version")

func encodeMessage(message channel.Message) ([]byte, error) {
	return channel.EncodeDurableMessage(message)
}

func encodeMessageWithPayloadHash(message channel.Message, payloadHash uint64) ([]byte, error) {
	return channel.EncodeDurableMessageWithPayloadHash(message, payloadHash)
}

func decodeMessage(payload []byte) (channel.Message, error) {
	msg, err := channel.DecodeDurableMessage(payload)
	if err != nil {
		return channel.Message{}, translateDurableCodecError(err)
	}
	return msg, nil
}

func decodeMessageView(payload []byte) (messageView, error) {
	view, err := channel.DecodeDurableMessageView(payload)
	if err != nil {
		return messageView{}, translateDurableCodecError(err)
	}
	return view, nil
}

func decodeMessageRecord(record store.LogRecord) (channel.Message, error) {
	view, err := decodeMessageView(record.Payload)
	if err != nil {
		return channel.Message{}, err
	}
	msg := view.Message
	msg.MessageSeq = record.Offset + 1
	return msg, nil
}

func hashPayload(payload []byte) uint64 {
	return channel.DurableMessagePayloadHash(payload)
}

func translateDurableCodecError(err error) error {
	if errors.Is(err, channel.ErrUnknownDurableMessageCodecVersion) {
		return errUnknownMessageCodecVersion
	}
	return err
}
