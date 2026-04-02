package wkcluster

import (
	"bytes"
	"testing"
)

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

func TestForwardPayloadRoundTrip(t *testing.T) {
	cmd := []byte("test-command")
	payload := encodeForwardPayload(7, cmd)
	groupID, decoded, err := decodeForwardPayload(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if groupID != 7 || !bytes.Equal(decoded, cmd) {
		t.Fatalf("mismatch: groupID=%d", groupID)
	}
}

func TestForwardRespRoundTrip(t *testing.T) {
	data := []byte("result")
	resp := encodeForwardResp(errCodeOK, data)
	code, decoded, err := decodeForwardResp(resp)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if code != errCodeOK || !bytes.Equal(decoded, data) {
		t.Fatalf("mismatch: code=%d", code)
	}
}
