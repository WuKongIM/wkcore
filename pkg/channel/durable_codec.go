package channel

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

const (
	// DurableMessageCodecVersion and DurableMessageHeaderSize define the durable
	// message payload contract shared by log encoding and store-side apply-fetch
	// idempotency reconstruction.
	DurableMessageCodecVersion byte = 1
	DurableMessageHeaderSize        = 45
)

const (
	durableMessageFramerFlagNoPersist uint8 = 1 << iota
	durableMessageFramerFlagRedDot
	durableMessageFramerFlagSyncOnce
	durableMessageFramerFlagDUP
	durableMessageFramerFlagHasServerVersion
	durableMessageFramerFlagEnd
)

// DurableMessageView exposes the compatibility payload plus its cached hash.
type DurableMessageView struct {
	Message     Message
	PayloadHash uint64
}

// EncodeDurableMessage serializes a channel message using the compatibility log format.
func EncodeDurableMessage(message Message) ([]byte, error) {
	return EncodeDurableMessageWithPayloadHash(message, DurableMessagePayloadHash(message.Payload))
}

// EncodeDurableMessageWithPayloadHash serializes a message with a caller-supplied payload hash.
func EncodeDurableMessageWithPayloadHash(message Message, payloadHash uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := buf.WriteByte(DurableMessageCodecVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.MessageID); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(EncodeDurableMessageFramerFlags(message.Framer)); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(byte(message.Setting)); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(byte(message.StreamFlag)); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(message.ChannelType); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.Expire); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.ClientSeq); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.StreamID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, payloadHash); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.MsgKey); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.ClientMsgNo); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.StreamNo); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.ChannelID); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.Topic); err != nil {
		return nil, err
	}
	if err := writeDurableString(&buf, message.FromUID); err != nil {
		return nil, err
	}
	if err := writeDurableBytes(&buf, message.Payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeDurableMessage decodes the compatibility payload and copies the payload bytes.
func DecodeDurableMessage(payload []byte) (Message, error) {
	view, err := DecodeDurableMessageView(payload)
	if err != nil {
		return Message{}, err
	}
	msg := view.Message
	msg.Payload = append([]byte(nil), view.Message.Payload...)
	return msg, nil
}

// DecodeDurableMessageView decodes the compatibility payload without copying payload bytes.
func DecodeDurableMessageView(payload []byte) (DurableMessageView, error) {
	if len(payload) < DurableMessageHeaderSize {
		return DurableMessageView{}, io.ErrUnexpectedEOF
	}
	if payload[0] != DurableMessageCodecVersion {
		return DurableMessageView{}, ErrUnknownDurableMessageCodecVersion
	}

	msg := Message{
		MessageID:   binary.BigEndian.Uint64(payload[1:9]),
		Framer:      DecodeDurableMessageFramerFlags(payload[9]),
		Setting:     frame.Setting(payload[10]),
		StreamFlag:  frame.StreamFlag(payload[11]),
		ChannelType: payload[12],
		Expire:      binary.BigEndian.Uint32(payload[13:17]),
		ClientSeq:   binary.BigEndian.Uint64(payload[17:25]),
		StreamID:    binary.BigEndian.Uint64(payload[25:33]),
		Timestamp:   int32(binary.BigEndian.Uint32(payload[33:37])),
	}
	view := DurableMessageView{Message: msg, PayloadHash: binary.BigEndian.Uint64(payload[37:45])}

	pos := DurableMessageHeaderSize
	msgKey, nextPos, err := readDurableSizedBytesView(payload, pos)
	if err != nil {
		return DurableMessageView{}, err
	}
	clientMsgNo, nextPos, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	streamNo, nextPos, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	channelID, nextPos, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	topic, nextPos, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	fromUID, nextPos, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	body, _, err := readDurableSizedBytesView(payload, nextPos)
	if err != nil {
		return DurableMessageView{}, err
	}
	view.Message.MsgKey = string(msgKey)
	view.Message.ChannelID = string(channelID)
	view.Message.ClientMsgNo = string(clientMsgNo)
	view.Message.StreamNo = string(streamNo)
	view.Message.Topic = string(topic)
	view.Message.FromUID = string(fromUID)
	view.Message.Payload = body
	return view, nil
}

// EncodeDurableMessageFramerFlags maps a frame framer into compatibility header bits.
func EncodeDurableMessageFramerFlags(framer frame.Framer) uint8 {
	var flags uint8
	if framer.NoPersist {
		flags |= durableMessageFramerFlagNoPersist
	}
	if framer.RedDot {
		flags |= durableMessageFramerFlagRedDot
	}
	if framer.SyncOnce {
		flags |= durableMessageFramerFlagSyncOnce
	}
	if framer.DUP {
		flags |= durableMessageFramerFlagDUP
	}
	if framer.HasServerVersion {
		flags |= durableMessageFramerFlagHasServerVersion
	}
	if framer.End {
		flags |= durableMessageFramerFlagEnd
	}
	return flags
}

// DecodeDurableMessageFramerFlags maps compatibility header bits back into a framer.
func DecodeDurableMessageFramerFlags(flags uint8) frame.Framer {
	return frame.Framer{
		NoPersist:        flags&durableMessageFramerFlagNoPersist != 0,
		RedDot:           flags&durableMessageFramerFlagRedDot != 0,
		SyncOnce:         flags&durableMessageFramerFlagSyncOnce != 0,
		DUP:              flags&durableMessageFramerFlagDUP != 0,
		HasServerVersion: flags&durableMessageFramerFlagHasServerVersion != 0,
		End:              flags&durableMessageFramerFlagEnd != 0,
	}
}

// DurableMessagePayloadHash computes the compatibility FNV-64a payload hash.
func DurableMessagePayloadHash(payload []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(payload)
	return hasher.Sum64()
}

func readDurableSizedBytesView(payload []byte, pos int) ([]byte, int, error) {
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

func writeDurableString(buf *bytes.Buffer, value string) error {
	return writeDurableBytes(buf, []byte(value))
}

func writeDurableBytes(buf *bytes.Buffer, value []byte) error {
	if len(value) > math.MaxUint32 {
		return ErrInvalidArgument
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}
