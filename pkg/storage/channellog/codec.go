package channellog

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
)

type storedMessage struct {
	MessageID   uint64
	SenderUID   string
	ClientMsgNo string
	PayloadHash uint64
	Payload     []byte
}

type storedMessageView struct {
	MessageID   uint64
	SenderUID   string
	ClientMsgNo string
	PayloadHash uint64
	Payload     []byte
}

func encodeStoredMessage(message storedMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, message.MessageID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, message.PayloadHash); err != nil {
		return nil, err
	}
	if err := writeString(&buf, message.SenderUID); err != nil {
		return nil, err
	}
	if err := writeString(&buf, message.ClientMsgNo); err != nil {
		return nil, err
	}
	if err := writeBytes(&buf, message.Payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeStoredMessage(payload []byte) (storedMessage, error) {
	view, err := decodeStoredMessageView(payload)
	if err != nil {
		return storedMessage{}, err
	}
	return storedMessage{
		MessageID:   view.MessageID,
		SenderUID:   view.SenderUID,
		ClientMsgNo: view.ClientMsgNo,
		PayloadHash: view.PayloadHash,
		Payload:     append([]byte(nil), view.Payload...),
	}, nil
}

func decodeStoredMessageView(payload []byte) (storedMessageView, error) {
	if len(payload) < 16 {
		return storedMessageView{}, io.ErrUnexpectedEOF
	}

	view := storedMessageView{
		MessageID:   binary.BigEndian.Uint64(payload[:8]),
		PayloadHash: binary.BigEndian.Uint64(payload[8:16]),
	}
	pos := 16

	senderUID, nextPos, err := readSizedBytesView(payload, pos)
	if err != nil {
		return storedMessageView{}, err
	}
	clientMsgNo, nextPos, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return storedMessageView{}, err
	}
	body, _, err := readSizedBytesView(payload, nextPos)
	if err != nil {
		return storedMessageView{}, err
	}

	view.SenderUID = string(senderUID)
	view.ClientMsgNo = string(clientMsgNo)
	// The caller already owns the encoded record bytes and may reuse the body
	// slice directly to avoid one more payload allocation during reads.
	view.Payload = body
	return view, nil
}

func readSizedBytesView(payload []byte, pos int) ([]byte, int, error) {
	if len(payload)-pos < 4 {
		return nil, pos, io.ErrUnexpectedEOF
	}
	size := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if len(payload)-pos < size {
		return nil, pos, io.ErrUnexpectedEOF
	}
	return payload[pos : pos+size], pos + size, nil
}

func hashPayload(payload []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(payload)
	return hasher.Sum64()
}

func writeString(buf *bytes.Buffer, value string) error {
	return writeBytes(buf, []byte(value))
}

func writeBytes(buf *bytes.Buffer, value []byte) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}

func readString(reader *bytes.Reader) (string, error) {
	value, err := readBytes(reader)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func readBytes(reader *bytes.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	value := make([]byte, size)
	if _, err := reader.Read(value); err != nil {
		return nil, err
	}
	return value, nil
}
