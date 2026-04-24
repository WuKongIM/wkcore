package store

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// messageRow is the structured form of one persisted message record.
type messageRow struct {
	MessageSeq  uint64
	MessageID   uint64
	FramerFlags uint8
	Setting     uint8
	StreamFlag  uint8
	MsgKey      string
	Expire      uint32
	ClientSeq   uint64
	ClientMsgNo string
	StreamNo    string
	StreamID    uint64
	Timestamp   int32
	ChannelID   string
	ChannelType uint8
	Topic       string
	FromUID     string
	Payload     []byte
	PayloadHash uint64
}

// messageRowFromChannelMessage projects a channel message into the structured row model.
func messageRowFromChannelMessage(msg channel.Message) messageRow {
	return messageRow{
		MessageSeq:  msg.MessageSeq,
		MessageID:   msg.MessageID,
		FramerFlags: channel.EncodeDurableMessageFramerFlags(msg.Framer),
		Setting:     uint8(msg.Setting),
		StreamFlag:  uint8(msg.StreamFlag),
		MsgKey:      msg.MsgKey,
		Expire:      msg.Expire,
		ClientSeq:   msg.ClientSeq,
		ClientMsgNo: msg.ClientMsgNo,
		StreamNo:    msg.StreamNo,
		StreamID:    msg.StreamID,
		Timestamp:   msg.Timestamp,
		ChannelID:   msg.ChannelID,
		ChannelType: msg.ChannelType,
		Topic:       msg.Topic,
		FromUID:     msg.FromUID,
		Payload:     append([]byte(nil), msg.Payload...),
		PayloadHash: channel.DurableMessagePayloadHash(msg.Payload),
	}
}

// toChannelMessage rebuilds the handler-facing message value from a structured row.
func (r messageRow) toChannelMessage() channel.Message {
	return channel.Message{
		MessageID:   r.MessageID,
		MessageSeq:  r.MessageSeq,
		Framer:      channel.DecodeDurableMessageFramerFlags(r.FramerFlags),
		Setting:     frame.Setting(r.Setting),
		MsgKey:      r.MsgKey,
		Expire:      r.Expire,
		ClientSeq:   r.ClientSeq,
		ClientMsgNo: r.ClientMsgNo,
		StreamNo:    r.StreamNo,
		StreamID:    r.StreamID,
		StreamFlag:  frame.StreamFlag(r.StreamFlag),
		Timestamp:   r.Timestamp,
		ChannelID:   r.ChannelID,
		ChannelType: r.ChannelType,
		Topic:       r.Topic,
		FromUID:     r.FromUID,
		Payload:     append([]byte(nil), r.Payload...),
	}
}

// messageRowFromRecordPayload decodes the legacy durable payload into a structured row.
func messageRowFromRecordPayload(payload []byte) (messageRow, error) {
	view, err := channel.DecodeDurableMessageView(payload)
	if err != nil {
		if errors.Is(err, channel.ErrUnknownDurableMessageCodecVersion) {
			return messageRow{}, channel.ErrCorruptValue
		}
		return messageRow{}, err
	}
	if view.Message.MessageID == 0 {
		return messageRow{}, channel.ErrCorruptValue
	}
	row := messageRowFromChannelMessage(view.Message)
	row.PayloadHash = view.PayloadHash
	row.Payload = append([]byte(nil), view.Message.Payload...)
	return row, nil
}

// toRecord encodes the structured row using the legacy durable message payload layout.
func (r messageRow) toRecord() (channel.Record, error) {
	if err := r.validate(); err != nil {
		return channel.Record{}, err
	}

	payloadHash := r.PayloadHash
	if payloadHash == 0 {
		payloadHash = channel.DurableMessagePayloadHash(r.Payload)
	}

	payload, err := channel.EncodeDurableMessageWithPayloadHash(r.toChannelMessage(), payloadHash)
	if err != nil {
		return channel.Record{}, err
	}
	return channel.Record{Payload: payload, SizeBytes: len(payload)}, nil
}

func (r messageRow) validate() error {
	if r.MessageID == 0 {
		return channel.ErrInvalidArgument
	}
	return nil
}
