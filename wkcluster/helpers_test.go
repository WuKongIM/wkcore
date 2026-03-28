package wkcluster

import "encoding/binary"

// encodeMessage is a test helper that builds a complete wire message.
func encodeMessage(msgType uint8, body []byte) []byte {
	buf := make([]byte, msgHeaderSize+len(body))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(body)))
	copy(buf[5:], body)
	return buf
}

// encodeRaftBody is a test helper: [groupID:8][data:N]
func encodeRaftBody(groupID uint64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[0:8], groupID)
	copy(buf[8:], data)
	return buf
}

// encodeForwardBody is a test helper: [requestID:8][groupID:8][cmd:N]
func encodeForwardBody(requestID uint64, groupID uint64, cmd []byte) []byte {
	buf := make([]byte, 16+len(cmd))
	binary.BigEndian.PutUint64(buf[0:8], requestID)
	binary.BigEndian.PutUint64(buf[8:16], groupID)
	copy(buf[16:], cmd)
	return buf
}

// encodeRespBody is a test helper: [requestID:8][errCode:1][data:N]
func encodeRespBody(requestID uint64, errCode uint8, data []byte) []byte {
	buf := make([]byte, 9+len(data))
	binary.BigEndian.PutUint64(buf[0:8], requestID)
	buf[8] = errCode
	copy(buf[9:], data)
	return buf
}
