package store

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestMessageRowRoundTripPreservesStructuredFields(t *testing.T) {
	row := messageRow{
		MessageSeq:  9,
		MessageID:   42,
		ClientMsgNo: "c-1",
		FromUID:     "u1",
		ChannelID:   "room",
		ChannelType: 1,
		Payload:     []byte("hello"),
		PayloadHash: 123,
	}

	primary, payload, err := encodeMessageFamilies(row)
	require.NoError(t, err)

	decoded, err := decodeMessageFamilies(9, primary, payload)
	require.NoError(t, err)
	require.Equal(t, row.MessageID, decoded.MessageID)
	require.Equal(t, row.Payload, decoded.Payload)
	require.Equal(t, row.PayloadHash, decoded.PayloadHash)
}

func TestMessageRowRejectsZeroMessageID(t *testing.T) {
	_, _, err := encodeMessageFamilies(messageRow{MessageSeq: 1})
	require.Error(t, err)
	require.True(t, errors.Is(err, channel.ErrInvalidArgument) || errors.Is(err, channel.ErrCorruptValue))
}

func TestMessageRowFromChannelMessageCopiesPayloadAndCompatibilityFields(t *testing.T) {
	msg := channel.Message{
		MessageID:   42,
		MessageSeq:  9,
		Framer:      frame.Framer{NoPersist: true, RedDot: true, SyncOnce: true, DUP: true, HasServerVersion: true, End: true},
		Setting:     frame.SettingReceiptEnabled,
		MsgKey:      "k-1",
		Expire:      60,
		ClientSeq:   7,
		ClientMsgNo: "c-1",
		StreamNo:    "s-1",
		StreamID:    88,
		StreamFlag:  frame.StreamFlagIng,
		Timestamp:   99,
		ChannelID:   "room",
		ChannelType: 1,
		Topic:       "topic",
		FromUID:     "u1",
		Payload:     []byte("hello"),
	}

	row := messageRowFromChannelMessage(msg)
	msg.Payload[0] = 'x'

	require.Equal(t, msg.MessageID, row.MessageID)
	require.Equal(t, msg.MessageSeq, row.MessageSeq)
	require.Equal(t, channel.EncodeDurableMessageFramerFlags(msg.Framer), row.FramerFlags)
	require.Equal(t, uint8(msg.Setting), row.Setting)
	require.Equal(t, uint8(msg.StreamFlag), row.StreamFlag)
	require.Equal(t, msg.MsgKey, row.MsgKey)
	require.Equal(t, msg.ClientMsgNo, row.ClientMsgNo)
	require.Equal(t, msg.ChannelID, row.ChannelID)
	require.Equal(t, []byte("hello"), row.Payload)
	require.Equal(t, channel.DurableMessagePayloadHash([]byte("hello")), row.PayloadHash)
}

func TestMessageRowToChannelMessageCopiesPayloadAndCompatibilityFields(t *testing.T) {
	row := messageRow{
		MessageSeq: 9,
		MessageID:  42,
		FramerFlags: channel.EncodeDurableMessageFramerFlags(frame.Framer{
			NoPersist: true, RedDot: true, SyncOnce: true, DUP: true, HasServerVersion: true, End: true,
		}),
		Setting:     uint8(frame.SettingReceiptEnabled),
		MsgKey:      "k-1",
		Expire:      60,
		ClientSeq:   7,
		ClientMsgNo: "c-1",
		StreamNo:    "s-1",
		StreamID:    88,
		StreamFlag:  uint8(frame.StreamFlagIng),
		Timestamp:   99,
		ChannelID:   "room",
		ChannelType: 1,
		Topic:       "topic",
		FromUID:     "u1",
		Payload:     []byte("hello"),
	}

	msg := row.toChannelMessage()
	msg.Payload[0] = 'x'

	require.Equal(t, row.MessageID, msg.MessageID)
	require.Equal(t, row.MessageSeq, msg.MessageSeq)
	require.Equal(t, channel.DecodeDurableMessageFramerFlags(row.FramerFlags), msg.Framer)
	require.Equal(t, frame.Setting(row.Setting), msg.Setting)
	require.Equal(t, frame.StreamFlag(row.StreamFlag), msg.StreamFlag)
	require.Equal(t, row.MsgKey, msg.MsgKey)
	require.Equal(t, row.ClientMsgNo, msg.ClientMsgNo)
	require.Equal(t, row.ChannelID, msg.ChannelID)
	require.Equal(t, []byte("hello"), row.Payload)
}

func TestMessageRowFromRecordPayloadDecodesSharedCompatibilityCodec(t *testing.T) {
	msg := channel.Message{
		MessageID:   42,
		Framer:      frame.Framer{NoPersist: true, RedDot: true, SyncOnce: true, DUP: true, HasServerVersion: true, End: true},
		Setting:     frame.SettingReceiptEnabled,
		MsgKey:      "k-1",
		Expire:      60,
		ClientSeq:   7,
		ClientMsgNo: "c-1",
		StreamNo:    "s-1",
		StreamID:    88,
		StreamFlag:  frame.StreamFlagIng,
		Timestamp:   99,
		ChannelID:   "room",
		ChannelType: 1,
		Topic:       "topic",
		FromUID:     "u1",
		Payload:     []byte("hello"),
	}

	payload, err := channel.EncodeDurableMessage(msg)
	require.NoError(t, err)

	row, err := messageRowFromRecordPayload(payload)
	require.NoError(t, err)
	payload[len(payload)-1] = 'x'

	require.Equal(t, msg.MessageID, row.MessageID)
	require.Equal(t, channel.EncodeDurableMessageFramerFlags(msg.Framer), row.FramerFlags)
	require.Equal(t, uint8(msg.Setting), row.Setting)
	require.Equal(t, uint8(msg.StreamFlag), row.StreamFlag)
	require.Equal(t, msg.MsgKey, row.MsgKey)
	require.Equal(t, msg.ClientMsgNo, row.ClientMsgNo)
	require.Equal(t, msg.ChannelID, row.ChannelID)
	require.Equal(t, []byte("hello"), row.Payload)
	require.Equal(t, channel.DurableMessagePayloadHash([]byte("hello")), row.PayloadHash)
}

func TestMessageRowToRecordEncodesSharedCompatibilityCodec(t *testing.T) {
	row := messageRow{
		MessageSeq: 9,
		MessageID:  42,
		FramerFlags: channel.EncodeDurableMessageFramerFlags(frame.Framer{
			NoPersist: true, RedDot: true, SyncOnce: true, DUP: true, HasServerVersion: true, End: true,
		}),
		Setting:     uint8(frame.SettingReceiptEnabled),
		MsgKey:      "k-1",
		Expire:      60,
		ClientSeq:   7,
		ClientMsgNo: "c-1",
		StreamNo:    "s-1",
		StreamID:    88,
		StreamFlag:  uint8(frame.StreamFlagIng),
		Timestamp:   99,
		ChannelID:   "room",
		ChannelType: 1,
		Topic:       "topic",
		FromUID:     "u1",
		Payload:     []byte("hello"),
		PayloadHash: 123,
	}

	record, err := row.toRecord()
	require.NoError(t, err)
	require.Equal(t, len(record.Payload), record.SizeBytes)

	view, err := channel.DecodeDurableMessageView(record.Payload)
	require.NoError(t, err)
	row.Payload[0] = 'x'

	require.Equal(t, row.MessageID, view.Message.MessageID)
	require.Equal(t, channel.DecodeDurableMessageFramerFlags(row.FramerFlags), view.Message.Framer)
	require.Equal(t, frame.Setting(row.Setting), view.Message.Setting)
	require.Equal(t, frame.StreamFlag(row.StreamFlag), view.Message.StreamFlag)
	require.Equal(t, row.MsgKey, view.Message.MsgKey)
	require.Equal(t, row.ClientMsgNo, view.Message.ClientMsgNo)
	require.Equal(t, row.ChannelID, view.Message.ChannelID)
	require.Equal(t, []byte("hello"), view.Message.Payload)
	require.Equal(t, row.PayloadHash, view.PayloadHash)
}

func TestMessageRowToRecordRoundTripPreservesHeaderLayout(t *testing.T) {
	row := messageRow{
		MessageID:   42,
		FramerFlags: 3,
		Setting:     4,
		StreamFlag:  5,
		Expire:      6,
		ClientSeq:   7,
		StreamID:    8,
		Timestamp:   9,
		ChannelType: 10,
		ClientMsgNo: "c-1",
		ChannelID:   "room",
		FromUID:     "u1",
		Payload:     []byte("hello"),
	}

	record, err := row.toRecord()
	require.NoError(t, err)

	require.Equal(t, channel.DurableMessageCodecVersion, record.Payload[0])
	require.Equal(t, row.MessageID, binary.BigEndian.Uint64(record.Payload[1:9]))
	require.Equal(t, row.FramerFlags, record.Payload[9])
	require.Equal(t, row.Setting, record.Payload[10])
	require.Equal(t, row.StreamFlag, record.Payload[11])
	require.Equal(t, row.ChannelType, record.Payload[12])
	require.Equal(t, row.Expire, binary.BigEndian.Uint32(record.Payload[13:17]))
	require.Equal(t, row.ClientSeq, binary.BigEndian.Uint64(record.Payload[17:25]))
	require.Equal(t, row.StreamID, binary.BigEndian.Uint64(record.Payload[25:33]))
	require.Equal(t, row.Timestamp, int32(binary.BigEndian.Uint32(record.Payload[33:37])))
}
