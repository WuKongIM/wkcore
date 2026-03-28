package wkcluster

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeMessage(t *testing.T) {
	body := []byte("hello")
	encoded := encodeMessage(msgTypeRaft, body)

	r := bytes.NewReader(encoded)
	msgType, decoded, err := readMessage(r)
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if msgType != msgTypeRaft {
		t.Fatalf("expected msgTypeRaft, got %d", msgType)
	}
	if !bytes.Equal(decoded, body) {
		t.Fatalf("body mismatch")
	}
}

func TestRaftBodyRoundTrip(t *testing.T) {
	data := []byte("raft-data")
	body := encodeRaftBody(7, data)
	groupID, decoded, err := decodeRaftBody(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if groupID != 7 || !bytes.Equal(decoded, data) {
		t.Fatalf("mismatch: groupID=%d", groupID)
	}
}

func TestForwardBodyRoundTrip(t *testing.T) {
	cmd := []byte("test-command")
	body := encodeForwardBody(42, 7, cmd)
	reqID, groupID, decoded, err := decodeForwardBody(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if reqID != 42 || groupID != 7 || !bytes.Equal(decoded, cmd) {
		t.Fatalf("mismatch: reqID=%d groupID=%d", reqID, groupID)
	}
}

func TestRespBodyRoundTrip(t *testing.T) {
	data := []byte("result")
	body := encodeRespBody(42, errCodeOK, data)
	reqID, code, decoded, err := decodeRespBody(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if reqID != 42 || code != errCodeOK || !bytes.Equal(decoded, data) {
		t.Fatalf("mismatch: reqID=%d code=%d", reqID, code)
	}
}

func TestForwardBodyTooShort(t *testing.T) {
	_, _, _, err := decodeForwardBody([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short body")
	}
}

func TestMessageTooLarge(t *testing.T) {
	// Craft a header claiming 100MB body
	hdr := make([]byte, 5)
	hdr[0] = msgTypeRaft
	hdr[1] = 0x06 // 100MB > 64MB limit
	hdr[2] = 0x40
	hdr[3] = 0x00
	hdr[4] = 0x00

	r := bytes.NewReader(hdr)
	_, _, err := readMessage(r)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}
