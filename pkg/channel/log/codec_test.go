package log

import (
	"bytes"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func TestEncodeMessageRoundTripPreservesDurableFields(t *testing.T) {
	msg := Message{
		MessageID:   42,
		Framer:      frame.Framer{NoPersist: true, RedDot: true, SyncOnce: true},
		Setting:     3,
		MsgKey:      "k1",
		Expire:      60,
		ClientSeq:   9,
		ClientMsgNo: "m-1",
		StreamNo:    "s-1",
		StreamID:    77,
		StreamFlag:  frame.StreamFlagEnd,
		Timestamp:   123456,
		ChannelID:   "u1@u2",
		ChannelType: frame.ChannelTypePerson,
		Topic:       "chat",
		FromUID:     "u1",
		Payload:     []byte("hello"),
	}

	encoded, err := encodeMessage(msg)
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}

	view, err := decodeMessageView(encoded)
	if err != nil {
		t.Fatalf("decodeMessageView() error = %v", err)
	}

	if view.MessageID != msg.MessageID {
		t.Fatalf("MessageID = %d, want %d", view.MessageID, msg.MessageID)
	}
	if view.Framer != msg.Framer {
		t.Fatalf("Framer = %+v, want %+v", view.Framer, msg.Framer)
	}
	if view.Setting != msg.Setting {
		t.Fatalf("Setting = %d, want %d", view.Setting, msg.Setting)
	}
	if view.MsgKey != msg.MsgKey {
		t.Fatalf("MsgKey = %q, want %q", view.MsgKey, msg.MsgKey)
	}
	if view.Expire != msg.Expire {
		t.Fatalf("Expire = %d, want %d", view.Expire, msg.Expire)
	}
	if view.ClientSeq != msg.ClientSeq {
		t.Fatalf("ClientSeq = %d, want %d", view.ClientSeq, msg.ClientSeq)
	}
	if view.ClientMsgNo != msg.ClientMsgNo {
		t.Fatalf("ClientMsgNo = %q, want %q", view.ClientMsgNo, msg.ClientMsgNo)
	}
	if view.StreamNo != msg.StreamNo {
		t.Fatalf("StreamNo = %q, want %q", view.StreamNo, msg.StreamNo)
	}
	if view.StreamID != msg.StreamID {
		t.Fatalf("StreamID = %d, want %d", view.StreamID, msg.StreamID)
	}
	if view.StreamFlag != msg.StreamFlag {
		t.Fatalf("StreamFlag = %d, want %d", view.StreamFlag, msg.StreamFlag)
	}
	if view.Timestamp != msg.Timestamp {
		t.Fatalf("Timestamp = %d, want %d", view.Timestamp, msg.Timestamp)
	}
	if view.ChannelID != msg.ChannelID {
		t.Fatalf("ChannelID = %q, want %q", view.ChannelID, msg.ChannelID)
	}
	if view.ChannelType != msg.ChannelType {
		t.Fatalf("ChannelType = %d, want %d", view.ChannelType, msg.ChannelType)
	}
	if view.Topic != msg.Topic {
		t.Fatalf("Topic = %q, want %q", view.Topic, msg.Topic)
	}
	if view.FromUID != msg.FromUID {
		t.Fatalf("FromUID = %q, want %q", view.FromUID, msg.FromUID)
	}
	if !bytes.Equal(view.Payload, msg.Payload) {
		t.Fatalf("Payload = %q, want %q", view.Payload, msg.Payload)
	}
}

func TestDecodeMessageViewRejectsTruncatedPayload(t *testing.T) {
	encoded, err := encodeMessage(Message{
		MessageID:   9,
		FromUID:     "u-9",
		ClientMsgNo: "m-9",
		Payload:     []byte("payload"),
	})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}

	for _, truncated := range [][]byte{
		encoded[:0],
		encoded[:8],
		encoded[:16],
		encoded[:len(encoded)-1],
	} {
		if _, err := decodeMessageView(truncated); err == nil {
			t.Fatalf("decodeMessageView(%d bytes) error = nil, want non-nil", len(truncated))
		}
	}
}

func TestDecodeMessageReturnsCopiedFullMessage(t *testing.T) {
	encoded, err := encodeMessage(Message{
		MessageID:   11,
		Framer:      frame.Framer{RedDot: true},
		Setting:     frame.SettingReceiptEnabled,
		MsgKey:      "k-11",
		Expire:      120,
		ClientSeq:   5,
		ClientMsgNo: "m-11",
		StreamNo:    "s-11",
		StreamID:    111,
		StreamFlag:  frame.StreamFlagStart,
		Timestamp:   222,
		ChannelID:   "c-11",
		ChannelType: frame.ChannelTypeGroup,
		Topic:       "topic-11",
		FromUID:     "u-11",
		Payload:     []byte("payload-11"),
	})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}

	msg, err := decodeMessage(encoded)
	if err != nil {
		t.Fatalf("decodeMessage() error = %v", err)
	}

	if msg.MessageID != 11 || msg.ClientMsgNo != "m-11" || msg.FromUID != "u-11" {
		t.Fatalf("decoded message = %+v", msg)
	}
	if !bytes.Equal(msg.Payload, []byte("payload-11")) {
		t.Fatalf("Payload = %q, want %q", msg.Payload, "payload-11")
	}

	msg.Payload[0] = 'P'
	if bytes.Equal(encoded, mustEncodeMessage(t, msg)) {
		t.Fatalf("expected decodeMessage payload copy to detach from encoded bytes")
	}
	view, err := decodeMessageView(encoded)
	if err != nil {
		t.Fatalf("decodeMessageView() error = %v", err)
	}
	if !bytes.Equal(view.Payload, []byte("payload-11")) {
		t.Fatalf("encoded payload mutated to %q", view.Payload)
	}
}

func TestDecodeMessageViewRejectsUnknownVersion(t *testing.T) {
	encoded := mustEncodeMessage(t, Message{
		MessageID:   21,
		ClientMsgNo: "m-21",
		FromUID:     "u-21",
		Payload:     []byte("payload-21"),
	})
	encoded[0]++

	if _, err := decodeMessageView(encoded); !errors.Is(err, errUnknownMessageCodecVersion) {
		t.Fatalf("decodeMessageView() error = %v, want %v", err, errUnknownMessageCodecVersion)
	}
}

func TestDecodeStoredMessageViewParsesEncodedMessage(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   42,
		FromUID:     "u-42",
		ClientMsgNo: "m-42",
		PayloadHash: hashPayload([]byte("payload")),
		Payload:     []byte("payload"),
	})

	view, err := decodeStoredMessageView(encoded)
	if err != nil {
		t.Fatalf("decodeStoredMessageView() error = %v", err)
	}

	if view.MessageID != 42 {
		t.Fatalf("MessageID = %d, want 42", view.MessageID)
	}
	if view.FromUID != "u-42" {
		t.Fatalf("FromUID = %q, want %q", view.FromUID, "u-42")
	}
	if view.ClientMsgNo != "m-42" {
		t.Fatalf("ClientMsgNo = %q, want %q", view.ClientMsgNo, "m-42")
	}
	if !bytes.Equal(view.Payload, []byte("payload")) {
		t.Fatalf("Payload = %q, want %q", view.Payload, "payload")
	}
}

func TestEncodeStoredMessagePreservesProvidedPayloadHash(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   42,
		FromUID:     "u-42",
		ClientMsgNo: "m-42",
		PayloadHash: 99,
		Payload:     []byte("payload"),
	})

	view, err := decodeStoredMessageView(encoded)
	if err != nil {
		t.Fatalf("decodeStoredMessageView() error = %v", err)
	}
	if view.PayloadHash != 99 {
		t.Fatalf("PayloadHash = %d, want %d", view.PayloadHash, 99)
	}
}

func mustEncodeMessage(t *testing.T, message Message) []byte {
	t.Helper()

	encoded, err := encodeMessage(message)
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	return encoded
}

func TestDecodeStoredMessageViewReusesPayloadBacking(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   7,
		FromUID:     "u-7",
		ClientMsgNo: "m-7",
		PayloadHash: hashPayload([]byte("payload")),
		Payload:     []byte("payload"),
	})
	before := append([]byte(nil), encoded...)

	view, err := decodeStoredMessageView(encoded)
	if err != nil {
		t.Fatalf("decodeStoredMessageView() error = %v", err)
	}
	view.Payload[0] = 'P'

	if bytes.Equal(encoded, before) {
		t.Fatalf("expected payload mutation to affect encoded backing bytes")
	}
	if view.Payload[0] != 'P' {
		t.Fatalf("Payload[0] = %q, want %q", view.Payload[0], 'P')
	}
}

func TestDecodeStoredMessageViewRejectsTruncatedPayload(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   9,
		FromUID:     "u-9",
		ClientMsgNo: "m-9",
		PayloadHash: hashPayload([]byte("payload")),
		Payload:     []byte("payload"),
	})

	for _, truncated := range [][]byte{
		encoded[:0],
		encoded[:8],
		encoded[:16],
		encoded[:len(encoded)-1],
	} {
		if _, err := decodeStoredMessageView(truncated); err == nil {
			t.Fatalf("decodeStoredMessageView(%d bytes) error = nil, want non-nil", len(truncated))
		}
	}
}
