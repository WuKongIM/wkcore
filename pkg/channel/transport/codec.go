package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
)

const (
	// Keep ISR node transport services out of the shared cluster RPC range.
	RPCServiceFetch          uint8 = 30
	RPCServiceProgressAck    uint8 = 31
	RPCServiceFetchBatch     uint8 = 32
	RPCServiceLongPollFetch  uint8 = 33
	RPCServiceReconcileProbe uint8 = 34

	fetchRequestCodecVersion  byte = 1
	fetchResponseCodecVersion byte = 1
	fetchBatchRequestVersion  byte = 1
	fetchBatchResponseVersion byte = 1
	progressAckCodecVersion   byte = 1
	progressAckResponseVer    byte = 1
	longPollRequestCodecVer   byte = 1
	longPollResponseCodecVer  byte = 1
	reconcileProbeCodecVer    byte = 1
	reconcileProbeRespVer     byte = 1
)

func encodeFetchRequest(req runtime.FetchRequestEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 80))
	buf.WriteByte(fetchRequestCodecVersion)
	if err := writeChannelKey(buf, req.ChannelKey); err != nil {
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

func decodeFetchRequest(data []byte) (runtime.FetchRequestEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	if version != fetchRequestCodecVersion {
		return runtime.FetchRequestEnvelope{}, fmt.Errorf("channeltransport: unknown fetch request codec version %d", version)
	}

	channelKey, err := readChannelKey(rd)
	if err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	var req runtime.FetchRequestEnvelope
	req.ChannelKey = channelKey
	if err := binary.Read(rd, binary.BigEndian, &req.Epoch); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &req.Generation); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	req.ReplicaID = channel.NodeID(replicaID)
	if err := binary.Read(rd, binary.BigEndian, &req.FetchOffset); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &req.OffsetEpoch); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	var maxBytes int64
	if err := binary.Read(rd, binary.BigEndian, &maxBytes); err != nil {
		return runtime.FetchRequestEnvelope{}, err
	}
	req.MaxBytes = int(maxBytes)
	if rd.Len() != 0 {
		return runtime.FetchRequestEnvelope{}, fmt.Errorf("channeltransport: trailing fetch request payload bytes")
	}
	return req, nil
}

func encodeFetchResponse(resp runtime.FetchResponseEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 96))
	buf.WriteByte(fetchResponseCodecVersion)
	if err := writeChannelKey(buf, resp.ChannelKey); err != nil {
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

func decodeFetchResponse(data []byte) (runtime.FetchResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	if version != fetchResponseCodecVersion {
		return runtime.FetchResponseEnvelope{}, fmt.Errorf("channeltransport: unknown fetch response codec version %d", version)
	}

	channelKey, err := readChannelKey(rd)
	if err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	resp := runtime.FetchResponseEnvelope{ChannelKey: channelKey}
	if err := binary.Read(rd, binary.BigEndian, &resp.Epoch); err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.Generation); err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	var truncateFlag byte
	if err := binary.Read(rd, binary.BigEndian, &truncateFlag); err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	if truncateFlag == 1 {
		var truncateTo uint64
		if err := binary.Read(rd, binary.BigEndian, &truncateTo); err != nil {
			return runtime.FetchResponseEnvelope{}, err
		}
		resp.TruncateTo = &truncateTo
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.LeaderHW); err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return runtime.FetchResponseEnvelope{}, err
	}
	resp.Records = make([]channel.Record, 0, count)
	for i := uint32(0); i < count; i++ {
		var sizeBytes int64
		if err := binary.Read(rd, binary.BigEndian, &sizeBytes); err != nil {
			return runtime.FetchResponseEnvelope{}, err
		}
		var payloadLen uint32
		if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
			return runtime.FetchResponseEnvelope{}, err
		}
		recordPayload := make([]byte, payloadLen)
		if _, err := io.ReadFull(rd, recordPayload); err != nil {
			return runtime.FetchResponseEnvelope{}, err
		}
		resp.Records = append(resp.Records, channel.Record{
			Payload:   recordPayload,
			SizeBytes: int(sizeBytes),
		})
	}
	if rd.Len() != 0 {
		return runtime.FetchResponseEnvelope{}, fmt.Errorf("channeltransport: trailing fetch response payload bytes")
	}
	return resp, nil
}

func encodeFetchBatchRequest(req runtime.FetchBatchRequestEnvelope) ([]byte, error) {
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

func decodeFetchBatchRequest(data []byte) (runtime.FetchBatchRequestEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.FetchBatchRequestEnvelope{}, err
	}
	if version != fetchBatchRequestVersion {
		return runtime.FetchBatchRequestEnvelope{}, fmt.Errorf("channeltransport: unknown fetch batch request codec version %d", version)
	}

	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return runtime.FetchBatchRequestEnvelope{}, err
	}
	req := runtime.FetchBatchRequestEnvelope{
		Items: make([]runtime.FetchBatchRequestItem, 0, count),
	}
	for i := uint32(0); i < count; i++ {
		var item runtime.FetchBatchRequestItem
		if err := binary.Read(rd, binary.BigEndian, &item.RequestID); err != nil {
			return runtime.FetchBatchRequestEnvelope{}, err
		}
		var payloadLen uint32
		if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
			return runtime.FetchBatchRequestEnvelope{}, err
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(rd, payload); err != nil {
			return runtime.FetchBatchRequestEnvelope{}, err
		}
		item.Request, err = decodeFetchRequest(payload)
		if err != nil {
			return runtime.FetchBatchRequestEnvelope{}, err
		}
		req.Items = append(req.Items, item)
	}
	if rd.Len() != 0 {
		return runtime.FetchBatchRequestEnvelope{}, fmt.Errorf("channeltransport: trailing fetch batch request payload bytes")
	}
	return req, nil
}

func encodeFetchBatchResponse(resp runtime.FetchBatchResponseEnvelope) ([]byte, error) {
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

func decodeFetchBatchResponse(data []byte) (runtime.FetchBatchResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.FetchBatchResponseEnvelope{}, err
	}
	if version != fetchBatchResponseVersion {
		return runtime.FetchBatchResponseEnvelope{}, fmt.Errorf("channeltransport: unknown fetch batch response codec version %d", version)
	}

	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return runtime.FetchBatchResponseEnvelope{}, err
	}
	resp := runtime.FetchBatchResponseEnvelope{
		Items: make([]runtime.FetchBatchResponseItem, 0, count),
	}
	for i := uint32(0); i < count; i++ {
		var item runtime.FetchBatchResponseItem
		if err := binary.Read(rd, binary.BigEndian, &item.RequestID); err != nil {
			return runtime.FetchBatchResponseEnvelope{}, err
		}
		flag, err := rd.ReadByte()
		if err != nil {
			return runtime.FetchBatchResponseEnvelope{}, err
		}
		switch flag {
		case 0:
			var payloadLen uint32
			if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
				return runtime.FetchBatchResponseEnvelope{}, err
			}
			if payloadLen > 0 {
				payload := make([]byte, payloadLen)
				if _, err := io.ReadFull(rd, payload); err != nil {
					return runtime.FetchBatchResponseEnvelope{}, err
				}
				fetchResp, err := decodeFetchResponse(payload)
				if err != nil {
					return runtime.FetchBatchResponseEnvelope{}, err
				}
				item.Response = &fetchResp
			}
		case 1:
			var errLen uint32
			if err := binary.Read(rd, binary.BigEndian, &errLen); err != nil {
				return runtime.FetchBatchResponseEnvelope{}, err
			}
			errPayload := make([]byte, errLen)
			if _, err := io.ReadFull(rd, errPayload); err != nil {
				return runtime.FetchBatchResponseEnvelope{}, err
			}
			item.Error = string(errPayload)
		default:
			return runtime.FetchBatchResponseEnvelope{}, fmt.Errorf("channeltransport: unknown fetch batch response item flag %d", flag)
		}
		resp.Items = append(resp.Items, item)
	}
	if rd.Len() != 0 {
		return runtime.FetchBatchResponseEnvelope{}, fmt.Errorf("channeltransport: trailing fetch batch response payload bytes")
	}
	return resp, nil
}

func encodeProgressAck(ack runtime.ProgressAckEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteByte(progressAckCodecVersion)
	if err := writeChannelKey(buf, ack.ChannelKey); err != nil {
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

func decodeProgressAck(data []byte) (runtime.ProgressAckEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	if version != progressAckCodecVersion {
		return runtime.ProgressAckEnvelope{}, fmt.Errorf("channeltransport: unknown progress ack codec version %d", version)
	}

	channelKey, err := readChannelKey(rd)
	if err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	ack := runtime.ProgressAckEnvelope{ChannelKey: channelKey}
	if err := binary.Read(rd, binary.BigEndian, &ack.Epoch); err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &ack.Generation); err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	ack.ReplicaID = channel.NodeID(replicaID)
	if err := binary.Read(rd, binary.BigEndian, &ack.MatchOffset); err != nil {
		return runtime.ProgressAckEnvelope{}, err
	}
	if rd.Len() != 0 {
		return runtime.ProgressAckEnvelope{}, fmt.Errorf("channeltransport: trailing progress ack payload bytes")
	}
	return ack, nil
}

type progressAckResponseEnvelope struct {
	LeaderHW uint64
}

func encodeProgressAckResponse(resp progressAckResponseEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteByte(progressAckResponseVer)
	if err := binary.Write(buf, binary.BigEndian, resp.LeaderHW); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeProgressAckResponse(data []byte) (progressAckResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return progressAckResponseEnvelope{}, err
	}
	if version != progressAckResponseVer {
		return progressAckResponseEnvelope{}, fmt.Errorf("channeltransport: unknown progress ack response codec version %d", version)
	}

	var resp progressAckResponseEnvelope
	if err := binary.Read(rd, binary.BigEndian, &resp.LeaderHW); err != nil {
		return progressAckResponseEnvelope{}, err
	}
	if rd.Len() != 0 {
		return progressAckResponseEnvelope{}, fmt.Errorf("channeltransport: trailing progress ack response payload bytes")
	}
	return resp, nil
}

func encodeReconcileProbeRequest(req runtime.ReconcileProbeRequestEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 48))
	buf.WriteByte(reconcileProbeCodecVer)
	if err := writeChannelKey(buf, req.ChannelKey); err != nil {
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
	return buf.Bytes(), nil
}

func decodeReconcileProbeRequest(data []byte) (runtime.ReconcileProbeRequestEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.ReconcileProbeRequestEnvelope{}, err
	}
	if version != reconcileProbeCodecVer {
		return runtime.ReconcileProbeRequestEnvelope{}, fmt.Errorf("channeltransport: unknown reconcile probe codec version %d", version)
	}

	channelKey, err := readChannelKey(rd)
	if err != nil {
		return runtime.ReconcileProbeRequestEnvelope{}, err
	}
	req := runtime.ReconcileProbeRequestEnvelope{ChannelKey: channelKey}
	if err := binary.Read(rd, binary.BigEndian, &req.Epoch); err != nil {
		return runtime.ReconcileProbeRequestEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &req.Generation); err != nil {
		return runtime.ReconcileProbeRequestEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return runtime.ReconcileProbeRequestEnvelope{}, err
	}
	req.ReplicaID = channel.NodeID(replicaID)
	if rd.Len() != 0 {
		return runtime.ReconcileProbeRequestEnvelope{}, fmt.Errorf("channeltransport: trailing reconcile probe payload bytes")
	}
	return req, nil
}

func encodeReconcileProbeResponse(resp runtime.ReconcileProbeResponseEnvelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteByte(reconcileProbeRespVer)
	if err := writeChannelKey(buf, resp.ChannelKey); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.Epoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.Generation); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(resp.ReplicaID)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.OffsetEpoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.LogEndOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, resp.CheckpointHW); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeReconcileProbeResponse(data []byte) (runtime.ReconcileProbeResponseEnvelope, error) {
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	if version != reconcileProbeRespVer {
		return runtime.ReconcileProbeResponseEnvelope{}, fmt.Errorf("channeltransport: unknown reconcile probe response codec version %d", version)
	}

	channelKey, err := readChannelKey(rd)
	if err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	resp := runtime.ReconcileProbeResponseEnvelope{ChannelKey: channelKey}
	if err := binary.Read(rd, binary.BigEndian, &resp.Epoch); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.Generation); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	var replicaID uint64
	if err := binary.Read(rd, binary.BigEndian, &replicaID); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	resp.ReplicaID = channel.NodeID(replicaID)
	if err := binary.Read(rd, binary.BigEndian, &resp.OffsetEpoch); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.LogEndOffset); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	if err := binary.Read(rd, binary.BigEndian, &resp.CheckpointHW); err != nil {
		return runtime.ReconcileProbeResponseEnvelope{}, err
	}
	if rd.Len() != 0 {
		return runtime.ReconcileProbeResponseEnvelope{}, fmt.Errorf("channeltransport: trailing reconcile probe response payload bytes")
	}
	return resp, nil
}

func writeChannelKey(buf *bytes.Buffer, channelKey channel.ChannelKey) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(channelKey))); err != nil {
		return err
	}
	if _, err := buf.WriteString(string(channelKey)); err != nil {
		return err
	}
	return nil
}

func readChannelKey(rd *bytes.Reader) (channel.ChannelKey, error) {
	var length uint32
	if err := binary.Read(rd, binary.BigEndian, &length); err != nil {
		return "", err
	}
	channelKey := make([]byte, length)
	if _, err := io.ReadFull(rd, channelKey); err != nil {
		return "", err
	}
	return channel.ChannelKey(channelKey), nil
}
