package channelcluster

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
)

type storedMessage struct {
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
	reader := bytes.NewReader(payload)
	var message storedMessage
	if err := binary.Read(reader, binary.BigEndian, &message.MessageID); err != nil {
		return storedMessage{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &message.PayloadHash); err != nil {
		return storedMessage{}, err
	}

	var err error
	if message.SenderUID, err = readString(reader); err != nil {
		return storedMessage{}, err
	}
	if message.ClientMsgNo, err = readString(reader); err != nil {
		return storedMessage{}, err
	}
	if message.Payload, err = readBytes(reader); err != nil {
		return storedMessage{}, err
	}
	return message, nil
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
