package store

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

const (
	messageRowFramerFlagNoPersist uint8 = 1 << iota
	messageRowFramerFlagRedDot
	messageRowFramerFlagSyncOnce
	messageRowFramerFlagDUP
	messageRowFramerFlagHasServerVersion
	messageRowFramerFlagEnd
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
		FramerFlags: encodeMessageRowFramerFlags(msg.Framer),
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
		PayloadHash: hashMessagePayload(msg.Payload),
	}
}

// toChannelMessage rebuilds the handler-facing message value from a structured row.
func (r messageRow) toChannelMessage() channel.Message {
	return channel.Message{
		MessageID:   r.MessageID,
		MessageSeq:  r.MessageSeq,
		Framer:      decodeMessageRowFramerFlags(r.FramerFlags),
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
	if len(payload) < channel.DurableMessageHeaderSize {
		return messageRow{}, io.ErrUnexpectedEOF
	}
	if payload[0] != channel.DurableMessageCodecVersion {
		return messageRow{}, channel.ErrCorruptValue
	}

	row := messageRow{
		MessageID:   binary.BigEndian.Uint64(payload[1:9]),
		FramerFlags: payload[9],
		Setting:     payload[10],
		StreamFlag:  payload[11],
		ChannelType: payload[12],
		Expire:      binary.BigEndian.Uint32(payload[13:17]),
		ClientSeq:   binary.BigEndian.Uint64(payload[17:25]),
		StreamID:    binary.BigEndian.Uint64(payload[25:33]),
		Timestamp:   int32(binary.BigEndian.Uint32(payload[33:37])),
		PayloadHash: binary.BigEndian.Uint64(payload[37:45]),
	}
	if row.MessageID == 0 {
		return messageRow{}, channel.ErrCorruptValue
	}

	pos := channel.DurableMessageHeaderSize
	msgKey, nextPos, err := readRecordSizedBytesView(payload, pos)
	if err != nil {
		return messageRow{}, err
	}
	clientMsgNo, nextPos, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}
	streamNo, nextPos, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}
	channelID, nextPos, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}
	topic, nextPos, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}
	fromUID, nextPos, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}
	body, _, err := readRecordSizedBytesView(payload, nextPos)
	if err != nil {
		return messageRow{}, err
	}

	row.MsgKey = string(msgKey)
	row.ClientMsgNo = string(clientMsgNo)
	row.StreamNo = string(streamNo)
	row.ChannelID = string(channelID)
	row.Topic = string(topic)
	row.FromUID = string(fromUID)
	row.Payload = append([]byte(nil), body...)
	return row, nil
}

// toRecord encodes the structured row using the legacy durable message payload layout.
func (r messageRow) toRecord() (channel.Record, error) {
	if err := r.validate(); err != nil {
		return channel.Record{}, err
	}

	payloadHash := r.PayloadHash
	if payloadHash == 0 {
		payloadHash = hashMessagePayload(r.Payload)
	}

	var buf bytes.Buffer
	if err := buf.WriteByte(channel.DurableMessageCodecVersion); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, r.MessageID); err != nil {
		return channel.Record{}, err
	}
	if err := buf.WriteByte(r.FramerFlags); err != nil {
		return channel.Record{}, err
	}
	if err := buf.WriteByte(r.Setting); err != nil {
		return channel.Record{}, err
	}
	if err := buf.WriteByte(r.StreamFlag); err != nil {
		return channel.Record{}, err
	}
	if err := buf.WriteByte(r.ChannelType); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, r.Expire); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, r.ClientSeq); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, r.StreamID); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, r.Timestamp); err != nil {
		return channel.Record{}, err
	}
	if err := binary.Write(&buf, binary.BigEndian, payloadHash); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.MsgKey); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.ClientMsgNo); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.StreamNo); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.ChannelID); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.Topic); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordString(&buf, r.FromUID); err != nil {
		return channel.Record{}, err
	}
	if err := writeRecordBytes(&buf, r.Payload); err != nil {
		return channel.Record{}, err
	}

	payload := buf.Bytes()
	return channel.Record{Payload: payload, SizeBytes: len(payload)}, nil
}

func (r messageRow) validate() error {
	if r.MessageID == 0 {
		return channel.ErrInvalidArgument
	}
	return nil
}

func encodeMessageRowFramerFlags(framer frame.Framer) uint8 {
	var flags uint8
	if framer.NoPersist {
		flags |= messageRowFramerFlagNoPersist
	}
	if framer.RedDot {
		flags |= messageRowFramerFlagRedDot
	}
	if framer.SyncOnce {
		flags |= messageRowFramerFlagSyncOnce
	}
	if framer.DUP {
		flags |= messageRowFramerFlagDUP
	}
	if framer.HasServerVersion {
		flags |= messageRowFramerFlagHasServerVersion
	}
	if framer.End {
		flags |= messageRowFramerFlagEnd
	}
	return flags
}

func decodeMessageRowFramerFlags(flags uint8) frame.Framer {
	return frame.Framer{
		NoPersist:        flags&messageRowFramerFlagNoPersist != 0,
		RedDot:           flags&messageRowFramerFlagRedDot != 0,
		SyncOnce:         flags&messageRowFramerFlagSyncOnce != 0,
		DUP:              flags&messageRowFramerFlagDUP != 0,
		HasServerVersion: flags&messageRowFramerFlagHasServerVersion != 0,
		End:              flags&messageRowFramerFlagEnd != 0,
	}
}

func readRecordSizedBytesView(payload []byte, pos int) ([]byte, int, error) {
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

func writeRecordString(buf *bytes.Buffer, value string) error {
	return writeRecordBytes(buf, []byte(value))
}

func writeRecordBytes(buf *bytes.Buffer, value []byte) error {
	if len(value) > math.MaxUint32 {
		return channel.ErrInvalidArgument
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}

func hashMessagePayload(payload []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(payload)
	return hasher.Sum64()
}
