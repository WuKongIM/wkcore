package channellog

import (
	"bytes"
	"testing"
)

func TestDecodeStoredMessageViewParsesEncodedMessage(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   42,
		SenderUID:   "u-42",
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
	if view.SenderUID != "u-42" {
		t.Fatalf("SenderUID = %q, want %q", view.SenderUID, "u-42")
	}
	if view.ClientMsgNo != "m-42" {
		t.Fatalf("ClientMsgNo = %q, want %q", view.ClientMsgNo, "m-42")
	}
	if !bytes.Equal(view.Payload, []byte("payload")) {
		t.Fatalf("Payload = %q, want %q", view.Payload, "payload")
	}
}

func TestDecodeStoredMessageViewReusesPayloadBacking(t *testing.T) {
	encoded := mustEncodeStoredMessage(t, storedMessage{
		MessageID:   7,
		SenderUID:   "u-7",
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
		SenderUID:   "u-9",
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
