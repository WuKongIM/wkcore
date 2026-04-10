package cluster

import (
	"encoding/binary"
	"fmt"
)

// Raft message types used when registering with transport.Server.
const (
	msgTypeRaft uint8 = 1
)

// Forward error codes (encoded within RPC payload, not wire-level).
const (
	errCodeOK        uint8 = 0
	errCodeNotLeader uint8 = 1
	errCodeTimeout   uint8 = 2
	errCodeNoGroup   uint8 = 3
)

// encodeRaftBody encodes [groupID:8][data:N].
func encodeRaftBody(groupID uint64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[0:8], groupID)
	copy(buf[8:], data)
	return buf
}

// decodeRaftBody decodes [groupID:8][data:N].
func decodeRaftBody(body []byte) (groupID uint64, data []byte, err error) {
	if len(body) < 8 {
		return 0, nil, fmt.Errorf("raft body too short: %d", len(body))
	}
	groupID = binary.BigEndian.Uint64(body[0:8])
	data = body[8:]
	return groupID, data, nil
}

// encodeForwardPayload encodes [groupID:8][cmd:N] for RPC payload.
func encodeForwardPayload(groupID uint64, cmd []byte) []byte {
	buf := make([]byte, 8+len(cmd))
	binary.BigEndian.PutUint64(buf[0:8], groupID)
	copy(buf[8:], cmd)
	return buf
}

// decodeForwardPayload decodes [groupID:8][cmd:N] from RPC payload.
func decodeForwardPayload(payload []byte) (groupID uint64, cmd []byte, err error) {
	if len(payload) < 8 {
		return 0, nil, fmt.Errorf("forward payload too short: %d", len(payload))
	}
	groupID = binary.BigEndian.Uint64(payload[0:8])
	cmd = payload[8:]
	return groupID, cmd, nil
}

// encodeForwardResp encodes [errCode:1][data:N] for RPC response payload.
func encodeForwardResp(errCode uint8, data []byte) []byte {
	buf := make([]byte, 1+len(data))
	buf[0] = errCode
	copy(buf[1:], data)
	return buf
}

// decodeForwardResp decodes [errCode:1][data:N] from RPC response payload.
func decodeForwardResp(payload []byte) (errCode uint8, data []byte, err error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("forward resp too short")
	}
	return payload[0], payload[1:], nil
}
