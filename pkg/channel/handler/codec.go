package handler

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

type messageView struct {
	Message     channel.Message
	PayloadHash uint64
}

var errUnknownMessageCodecVersion = errors.New("channelhandler: unknown message codec version")

func encodeMessage(message channel.Message) ([]byte, error) {
	return encodeMessageWithPayloadHash(message, hashPayload(message.Payload))
}

func encodeMessageWithPayloadHash(message channel.Message, payloadHash uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := buf.WriteByte(channel.DurableMessageCodecVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.MessageID); err != nil {
		return nil, err
	}
	if _, err := buf.Write(make([]byte, channel.DurableMessageHeaderSize-9)); err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint64(buf.Bytes()[37:45], payloadHash)
	if err := writeString(&buf, ""); err != nil {
		return nil, err
	}
	if err := writeString(&buf, message.ClientMsgNo); err != nil {
		return nil, err
	}
	if err := writeString(&buf, ""); err != nil {
		return nil, err
	}
	if err := writeString(&buf, message.ChannelID); err != nil {
		return nil, err
	}
	if err := writeString(&buf, ""); err != nil {
		return nil, err
	}
	if err := writeString(&buf, message.FromUID); err != nil {
		return nil, err
	}
	if err := writeBytes(&buf, message.Payload); err != nil {
		return nil, err
	}
	payload := buf.Bytes()
	payload[12] = message.ChannelType
	return payload, nil
}

func decodeMessage(payload []byte) (channel.Message, error) {
	view, err := decodeMessageView(payload)
	if err != nil {
		return channel.Message{}, err
	}
	msg := view.Message
	msg.Payload = append([]byte(nil), view.Message.Payload...)
	return msg, nil
}

func decodeMessageView(payload []byte) (messageView, error) {
	if len(payload) < channel.DurableMessageHeaderSize {
		return messageView{}, io.ErrUnexpectedEOF
	}
	if payload[0] != channel.DurableMessageCodecVersion {
		return messageView{}, errUnknownMessageCodecVersion
	}

	msg := channel.Message{
		MessageID:   binary.BigEndian.Uint64(payload[1:9]),
		ChannelType: payload[12],
	}
	view := messageView{Message: msg, PayloadHash: binary.BigEndian.Uint64(payload[37:45])}

	pos := channel.DurableMessageHeaderSize
	_, nextPos, err := readSizedBytesView(payload, pos)
	if err != nil {
		return messageView{}, err
	}
	clientMsgNo, nextPos, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	_, nextPos, err = readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	channelID, nextPos, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	_, nextPos, err = readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	fromUID, nextPos, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	body, _, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return messageView{}, err
	}
	view.Message.ChannelID = string(channelID)
	view.Message.ClientMsgNo = string(clientMsgNo)
	view.Message.FromUID = string(fromUID)
	view.Message.Payload = body
	return view, nil
}

func decodeMessageRecord(record storeRecord) (channel.Message, error) {
	view, err := decodeMessageView(record.Payload)
	if err != nil {
		return channel.Message{}, err
	}
	msg := view.Message
	msg.MessageSeq = record.Offset + 1
	return msg, nil
}

type storeRecord struct {
	Offset  uint64
	Payload []byte
}

func readSizedBytesView(payload []byte, pos int) ([]byte, int, error) {
	if len(payload)-pos < 4 {
		return nil, pos, io.ErrUnexpectedEOF
	}
	size := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if len(payload)-pos < size {
		return nil, pos, io.ErrUnexpectedEOF
	}
	return payload[pos : pos+size], pos + size, nil
}

func hashPayload(payload []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(payload)
	return hasher.Sum64()
}

func writeString(buf *bytes.Buffer, value string) error {
	return writeBytes(buf, []byte(value))
}

func writeBytes(buf *bytes.Buffer, value []byte) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}
