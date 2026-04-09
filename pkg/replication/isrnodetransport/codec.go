package isrnodetransport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
)

const (
	RPCServiceFetch       uint8 = 20
	RPCServiceProgressAck uint8 = 21
	RPCServiceFetchBatch  uint8 = 22

	fetchRequestCodecVersion  byte = 1
	fetchResponseCodecVersion byte = 1
	fetchBatchRequestVersion  byte = 1
	fetchBatchResponseVersion byte = 1
	progressAckCodecVersion   byte = 1
)

func encodeFetchRequest(req isrnode.FetchRequestEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 80))
	buf.WriteByte(fetchRequestCodecVersion)
	if err := writeGroupKey(buf, req.GroupKey); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, req.Epoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, req.Generation); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(req.ReplicaID)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, req.FetchOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, req.OffsetEpoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(req.MaxBytes)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeFetchRequest(data []byte) (isrnode.FetchRequestEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	if version != fetchRequestCodecVersion {
		return isrnode.FetchRequestEnvelope{}, fmt.Errorf("isrnodetransport: unknown fetch request codec version %d", version)
	}

	groupKey, err := readGroupKey(rd)
	if err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	var req isrnode.FetchRequestEnvelope
	req.GroupKey = groupKey
	if err := binary.Read(rd, binary.BigEndian, &req.Epoch); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &req.Generation); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	req.ReplicaID = isr.NodeID(replicaID)
	if err := binary.Read(rd, binary.BigEndian, &req.FetchOffset); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &req.OffsetEpoch); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	var maxBytes int64
	if err := binary.Read(rd, binary.BigEndian, &maxBytes); err != nil {
		return isrnode.FetchRequestEnvelope{}, err
	}
	req.MaxBytes = int(maxBytes)
	if rd.Len() != 0 {
		return isrnode.FetchRequestEnvelope{}, fmt.Errorf("isrnodetransport: trailing fetch request payload bytes")
	}
	return req, nil
}

func encodeFetchResponse(resp isrnode.FetchResponseEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 96))
	buf.WriteByte(fetchResponseCodecVersion)
	if err := writeGroupKey(buf, resp.GroupKey); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.Epoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.Generation); err != nil {
		return nil, err
	}
	if resp.TruncateTo != nil {
		buf.WriteByte(1)
		if err := binary.Write(buf, binary.BigEndian, *resp.TruncateTo); err != nil {
			return nil, err
		}
	} else {
		buf.WriteByte(0)
	}
	if err := binary.Write(buf, binary.BigEndian, resp.LeaderHW); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(resp.Records))); err != nil {
		return nil, err
	}
	for _, record := range resp.Records {
		if err := binary.Write(buf, binary.BigEndian, int64(record.SizeBytes)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(record.Payload))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(record.Payload); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeFetchResponse(data []byte) (isrnode.FetchResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	if version != fetchResponseCodecVersion {
		return isrnode.FetchResponseEnvelope{}, fmt.Errorf("isrnodetransport: unknown fetch response codec version %d", version)
	}

	groupKey, err := readGroupKey(rd)
	if err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	resp := isrnode.FetchResponseEnvelope{GroupKey: groupKey}
	if err := binary.Read(rd, binary.BigEndian, &resp.Epoch); err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.Generation); err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	var truncateFlag byte
	if err := binary.Read(rd, binary.BigEndian, &truncateFlag); err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	if truncateFlag == 1 {
		var truncateTo uint64
		if err := binary.Read(rd, binary.BigEndian, &truncateTo); err != nil {
			return isrnode.FetchResponseEnvelope{}, err
		}
		resp.TruncateTo = &truncateTo
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.LeaderHW); err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return isrnode.FetchResponseEnvelope{}, err
	}
	resp.Records = make([]isr.Record, 0, count)
	for i := uint32(0); i < count; i++ {
		var sizeBytes int64
		if err := binary.Read(rd, binary.BigEndian, &sizeBytes); err != nil {
			return isrnode.FetchResponseEnvelope{}, err
		}
		var payloadLen uint32
		if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
			return isrnode.FetchResponseEnvelope{}, err
		}
		recordPayload := make([]byte, payloadLen)
		if _, err := io.ReadFull(rd, recordPayload); err != nil {
			return isrnode.FetchResponseEnvelope{}, err
		}
		resp.Records = append(resp.Records, isr.Record{
			Payload:   recordPayload,
			SizeBytes: int(sizeBytes),
		})
	}
	if rd.Len() != 0 {
		return isrnode.FetchResponseEnvelope{}, fmt.Errorf("isrnodetransport: trailing fetch response payload bytes")
	}
	return resp, nil
}

func encodeFetchBatchRequest(req isrnode.FetchBatchRequestEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteByte(fetchBatchRequestVersion)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(req.Items))); err != nil {
		return nil, err
	}
	for _, item := range req.Items {
		if err := binary.Write(buf, binary.BigEndian, item.RequestID); err != nil {
			return nil, err
		}
		itemPayload, err := encodeFetchRequest(item.Request)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(itemPayload))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(itemPayload); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeFetchBatchRequest(data []byte) (isrnode.FetchBatchRequestEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return isrnode.FetchBatchRequestEnvelope{}, err
	}
	if version != fetchBatchRequestVersion {
		return isrnode.FetchBatchRequestEnvelope{}, fmt.Errorf("isrnodetransport: unknown fetch batch request codec version %d", version)
	}

	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return isrnode.FetchBatchRequestEnvelope{}, err
	}
	req := isrnode.FetchBatchRequestEnvelope{
		Items: make([]isrnode.FetchBatchRequestItem, 0, count),
	}
	for i := uint32(0); i < count; i++ {
		var item isrnode.FetchBatchRequestItem
		if err := binary.Read(rd, binary.BigEndian, &item.RequestID); err != nil {
			return isrnode.FetchBatchRequestEnvelope{}, err
		}
		var payloadLen uint32
		if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
			return isrnode.FetchBatchRequestEnvelope{}, err
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(rd, payload); err != nil {
			return isrnode.FetchBatchRequestEnvelope{}, err
		}
		item.Request, err = decodeFetchRequest(payload)
		if err != nil {
			return isrnode.FetchBatchRequestEnvelope{}, err
		}
		req.Items = append(req.Items, item)
	}
	if rd.Len() != 0 {
		return isrnode.FetchBatchRequestEnvelope{}, fmt.Errorf("isrnodetransport: trailing fetch batch request payload bytes")
	}
	return req, nil
}

func encodeFetchBatchResponse(resp isrnode.FetchBatchResponseEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteByte(fetchBatchResponseVersion)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(resp.Items))); err != nil {
		return nil, err
	}
	for _, item := range resp.Items {
		if err := binary.Write(buf, binary.BigEndian, item.RequestID); err != nil {
			return nil, err
		}
		if item.Error != "" {
			buf.WriteByte(1)
			if err := binary.Write(buf, binary.BigEndian, uint32(len(item.Error))); err != nil {
				return nil, err
			}
			if _, err := buf.WriteString(item.Error); err != nil {
				return nil, err
			}
			continue
		}
		buf.WriteByte(0)
		if item.Response == nil {
			if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
				return nil, err
			}
			continue
		}
		payload, err := encodeFetchResponse(*item.Response)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(payload))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(payload); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeFetchBatchResponse(data []byte) (isrnode.FetchBatchResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return isrnode.FetchBatchResponseEnvelope{}, err
	}
	if version != fetchBatchResponseVersion {
		return isrnode.FetchBatchResponseEnvelope{}, fmt.Errorf("isrnodetransport: unknown fetch batch response codec version %d", version)
	}

	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return isrnode.FetchBatchResponseEnvelope{}, err
	}
	resp := isrnode.FetchBatchResponseEnvelope{
		Items: make([]isrnode.FetchBatchResponseItem, 0, count),
	}
	for i := uint32(0); i < count; i++ {
		var item isrnode.FetchBatchResponseItem
		if err := binary.Read(rd, binary.BigEndian, &item.RequestID); err != nil {
			return isrnode.FetchBatchResponseEnvelope{}, err
		}
		flag, err := rd.ReadByte()
		if err != nil {
			return isrnode.FetchBatchResponseEnvelope{}, err
		}
		switch flag {
		case 0:
			var payloadLen uint32
			if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
				return isrnode.FetchBatchResponseEnvelope{}, err
			}
			if payloadLen > 0 {
				payload := make([]byte, payloadLen)
				if _, err := io.ReadFull(rd, payload); err != nil {
					return isrnode.FetchBatchResponseEnvelope{}, err
				}
				fetchResp, err := decodeFetchResponse(payload)
				if err != nil {
					return isrnode.FetchBatchResponseEnvelope{}, err
				}
				item.Response = &fetchResp
			}
		case 1:
			var errLen uint32
			if err := binary.Read(rd, binary.BigEndian, &errLen); err != nil {
				return isrnode.FetchBatchResponseEnvelope{}, err
			}
			errPayload := make([]byte, errLen)
			if _, err := io.ReadFull(rd, errPayload); err != nil {
				return isrnode.FetchBatchResponseEnvelope{}, err
			}
			item.Error = string(errPayload)
		default:
			return isrnode.FetchBatchResponseEnvelope{}, fmt.Errorf("isrnodetransport: unknown fetch batch response item flag %d", flag)
		}
		resp.Items = append(resp.Items, item)
	}
	if rd.Len() != 0 {
		return isrnode.FetchBatchResponseEnvelope{}, fmt.Errorf("isrnodetransport: trailing fetch batch response payload bytes")
	}
	return resp, nil
}

func encodeProgressAck(ack isrnode.ProgressAckEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteByte(progressAckCodecVersion)
	if err := writeGroupKey(buf, ack.GroupKey); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, ack.Epoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, ack.Generation); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(ack.ReplicaID)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, ack.MatchOffset); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeProgressAck(data []byte) (isrnode.ProgressAckEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	if version != progressAckCodecVersion {
		return isrnode.ProgressAckEnvelope{}, fmt.Errorf("isrnodetransport: unknown progress ack codec version %d", version)
	}

	groupKey, err := readGroupKey(rd)
	if err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	ack := isrnode.ProgressAckEnvelope{GroupKey: groupKey}
	if err := binary.Read(rd, binary.BigEndian, &ack.Epoch); err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &ack.Generation); err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	ack.ReplicaID = isr.NodeID(replicaID)
	if err := binary.Read(rd, binary.BigEndian, &ack.MatchOffset); err != nil {
		return isrnode.ProgressAckEnvelope{}, err
	}
	if rd.Len() != 0 {
		return isrnode.ProgressAckEnvelope{}, fmt.Errorf("isrnodetransport: trailing progress ack payload bytes")
	}
	return ack, nil
}

func writeGroupKey(buf *bytes.Buffer, groupKey isr.GroupKey) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(groupKey))); err != nil {
		return err
	}
	if _, err := buf.WriteString(string(groupKey)); err != nil {
		return err
	}
	return nil
}

func readGroupKey(rd *bytes.Reader) (isr.GroupKey, error) {
	var length uint32
	if err := binary.Read(rd, binary.BigEndian, &length); err != nil {
		return "", err
	}
	groupKey := make([]byte, length)
	if _, err := io.ReadFull(rd, groupKey); err != nil {
		return "", err
	}
	return isr.GroupKey(groupKey), nil
}
