package wktransport

import "errors"

var (
	ErrStopped        = errors.New("wktransport: stopped")
	ErrTimeout        = errors.New("wktransport: request timeout")
	ErrNodeNotFound   = errors.New("wktransport: node not found")
	ErrMsgTooLarge    = errors.New("wktransport: message too large")
	ErrInvalidMsgType = errors.New("wktransport: invalid message type 0")
)

const (
	// MaxMessageSize is the upper bound for a single wire message body.
	MaxMessageSize = 64 << 20 // 64 MB

	// Reserved message types for built-in RPC mechanism.
	MsgTypeRPCRequest  uint8 = 0xFE
	MsgTypeRPCResponse uint8 = 0xFF

	// maxPooledBufCap prevents the buffer pool from retaining huge slices.
	maxPooledBufCap = 64 * 1024

	// msgHeaderSize is [msgType:1][bodyLen:4].
	msgHeaderSize = 5
)
