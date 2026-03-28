package wkcluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	msgTypeRaft    uint8 = 1
	msgTypeForward uint8 = 2
	msgTypeResp    uint8 = 3

	msgHeaderSize = 5 // [msgType:1][bodyLen:4]

	// maxMessageSize is the upper bound for a single wire message body.
	maxMessageSize = 64 << 20 // 64 MB

	// maxPooledBufCap prevents the buffer pool from retaining huge slices.
	maxPooledBufCap = 64 * 1024
)

// Error codes for forward responses.
const (
	errCodeOK        uint8 = 0
	errCodeNotLeader uint8 = 1
	errCodeTimeout   uint8 = 2
	errCodeNoGroup   uint8 = 3
)

// bufPool reuses byte slices for encoding, eliminating per-message allocations
// on the hot path. Callers must return buffers via putBuf after use.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return b
	},
}

func getBuf(n int) []byte {
	buf := bufPool.Get().([]byte)
	if cap(buf) >= n {
		return buf[:n]
	}
	return make([]byte, n)
}

func putBuf(buf []byte) {
	if cap(buf) <= maxPooledBufCap {
		//nolint:staticcheck // SA6002: slice header is fine here
		bufPool.Put(buf[:0])
	}
}

// writeRaftMessage encodes a raft message into a pooled buffer and writes it
// in a single call, avoiding intermediate allocations.
// Wire format: [msgType:1][bodyLen:4][groupID:8][data:N]
func writeRaftMessage(w io.Writer, groupID uint64, data []byte) error {
	bodySize := 8 + len(data)
	totalSize := msgHeaderSize + bodySize
	buf := getBuf(totalSize)
	buf[0] = msgTypeRaft
	binary.BigEndian.PutUint32(buf[1:5], uint32(bodySize))
	binary.BigEndian.PutUint64(buf[5:13], groupID)
	copy(buf[13:], data)
	_, err := w.Write(buf)
	putBuf(buf)
	return err
}

// writeForwardMessage encodes a forward request into a pooled buffer and
// writes it in a single call.
// Wire format: [msgType:1][bodyLen:4][requestID:8][groupID:8][cmd:N]
func writeForwardMessage(w io.Writer, requestID, groupID uint64, cmd []byte) error {
	bodySize := 16 + len(cmd)
	totalSize := msgHeaderSize + bodySize
	buf := getBuf(totalSize)
	buf[0] = msgTypeForward
	binary.BigEndian.PutUint32(buf[1:5], uint32(bodySize))
	binary.BigEndian.PutUint64(buf[5:13], requestID)
	binary.BigEndian.PutUint64(buf[13:21], groupID)
	copy(buf[21:], cmd)
	_, err := w.Write(buf)
	putBuf(buf)
	return err
}

// writeRespMessage encodes a response into a pooled buffer and writes it
// in a single call.
// Wire format: [msgType:1][bodyLen:4][requestID:8][errCode:1][data:N]
func writeRespMessage(w io.Writer, requestID uint64, errCode uint8, data []byte) error {
	bodySize := 9 + len(data)
	totalSize := msgHeaderSize + bodySize
	buf := getBuf(totalSize)
	buf[0] = msgTypeResp
	binary.BigEndian.PutUint32(buf[1:5], uint32(bodySize))
	binary.BigEndian.PutUint64(buf[5:13], requestID)
	buf[13] = errCode
	copy(buf[14:], data)
	_, err := w.Write(buf)
	putBuf(buf)
	return err
}

func readMessage(r io.Reader) (msgType uint8, body []byte, err error) {
	var hdr [msgHeaderSize]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	msgType = hdr[0]
	bodyLen := binary.BigEndian.Uint32(hdr[1:5])
	if bodyLen > maxMessageSize {
		return 0, nil, fmt.Errorf("message too large: %d bytes", bodyLen)
	}
	body = make([]byte, bodyLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}

func decodeRaftBody(body []byte) (groupID uint64, data []byte, err error) {
	if len(body) < 8 {
		return 0, nil, fmt.Errorf("raft body too short: %d", len(body))
	}
	groupID = binary.BigEndian.Uint64(body[0:8])
	data = body[8:]
	return groupID, data, nil
}

func decodeForwardBody(body []byte) (requestID uint64, groupID uint64, cmd []byte, err error) {
	if len(body) < 16 {
		return 0, 0, nil, fmt.Errorf("forward body too short: %d", len(body))
	}
	requestID = binary.BigEndian.Uint64(body[0:8])
	groupID = binary.BigEndian.Uint64(body[8:16])
	cmd = body[16:]
	return requestID, groupID, cmd, nil
}

func decodeRespBody(body []byte) (requestID uint64, errCode uint8, data []byte, err error) {
	if len(body) < 9 {
		return 0, 0, nil, fmt.Errorf("resp body too short: %d", len(body))
	}
	requestID = binary.BigEndian.Uint64(body[0:8])
	errCode = body[8]
	data = body[9:]
	return requestID, errCode, data, nil
}
