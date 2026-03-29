package wktransport

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

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

// WriteMessage encodes and writes a framed message [msgType:1][bodyLen:4][body:N].
// Uses a buffer pool to avoid per-message allocations on the hot path.
func WriteMessage(w io.Writer, msgType uint8, body []byte) error {
	totalSize := msgHeaderSize + len(body)
	buf := getBuf(totalSize)
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(body)))
	copy(buf[5:], body)
	_, err := w.Write(buf)
	putBuf(buf)
	return err
}

// ReadMessage reads a framed message from r.
// Returns ErrInvalidMsgType for msgType=0, ErrMsgTooLarge if body exceeds MaxMessageSize.
func ReadMessage(r io.Reader) (msgType uint8, body []byte, err error) {
	var hdr [msgHeaderSize]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	msgType = hdr[0]
	if msgType == 0 {
		return 0, nil, ErrInvalidMsgType
	}
	bodyLen := binary.BigEndian.Uint32(hdr[1:5])
	if bodyLen > MaxMessageSize {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrMsgTooLarge, bodyLen)
	}
	if bodyLen == 0 {
		return msgType, nil, nil
	}
	body = make([]byte, bodyLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}
