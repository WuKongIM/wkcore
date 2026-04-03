package nodetransport

import (
	"bytes"
	"testing"
)

func TestWriteReadMessage_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	body := []byte("hello")
	if err := WriteMessage(&buf, 1, body); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	msgType, decoded, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != 1 {
		t.Fatalf("expected msgType=1, got %d", msgType)
	}
	if !bytes.Equal(decoded, body) {
		t.Fatalf("body mismatch")
	}
}

func TestWriteReadMessage_EmptyBody(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, 42, nil); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	msgType, body, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != 42 || len(body) != 0 {
		t.Fatalf("unexpected: msgType=%d bodyLen=%d", msgType, len(body))
	}
}

func TestReadMessage_InvalidMsgType0(t *testing.T) {
	frame := []byte{0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
	r := bytes.NewReader(frame)
	_, _, err := ReadMessage(r)
	if err != ErrInvalidMsgType {
		t.Fatalf("expected ErrInvalidMsgType, got: %v", err)
	}
}

func TestReadMessage_TooLarge(t *testing.T) {
	hdr := []byte{1, 0x06, 0x40, 0x00, 0x00}
	r := bytes.NewReader(hdr)
	_, _, err := ReadMessage(r)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}

func TestWriteReadMessage_MultipleMessages(t *testing.T) {
	var buf bytes.Buffer
	msgs := []struct {
		msgType uint8
		body    []byte
	}{
		{1, []byte("first")},
		{2, []byte("second")},
		{0xFE, []byte("rpc-req")},
	}
	for _, m := range msgs {
		if err := WriteMessage(&buf, m.msgType, m.body); err != nil {
			t.Fatalf("WriteMessage(%d): %v", m.msgType, err)
		}
	}
	for _, m := range msgs {
		msgType, body, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if msgType != m.msgType || !bytes.Equal(body, m.body) {
			t.Fatalf("expected type=%d body=%q, got type=%d body=%q",
				m.msgType, m.body, msgType, body)
		}
	}
}
