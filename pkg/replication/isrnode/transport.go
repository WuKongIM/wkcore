package isrnode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const fetchResponseCodecVersion1 byte = 1

type fetchResponsePayload struct {
	TruncateTo *uint64      `json:"truncate_to,omitempty"`
	LeaderHW   uint64       `json:"leader_hw"`
	Records    []isr.Record `json:"records,omitempty"`
}

func encodeFetchResponsePayload(payload fetchResponsePayload) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteByte(fetchResponseCodecVersion1)
	if payload.TruncateTo != nil {
		buf.WriteByte(1)
		if err := binary.Write(buf, binary.BigEndian, *payload.TruncateTo); err != nil {
			return nil, err
		}
	} else {
		buf.WriteByte(0)
	}
	if err := binary.Write(buf, binary.BigEndian, payload.LeaderHW); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(payload.Records))); err != nil {
		return nil, err
	}
	for _, record := range payload.Records {
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

func decodeFetchResponsePayload(data []byte) (fetchResponsePayload, error) {
	if len(data) == 0 {
		return fetchResponsePayload{}, nil
	}
	rd := bytes.NewReader(data)
	version, err := rd.ReadByte()
	if err != nil {
		return fetchResponsePayload{}, err
	}
	if version != fetchResponseCodecVersion1 {
		return fetchResponsePayload{}, errors.New("isrnode: unknown fetch response codec version")
	}

	var truncateFlag byte
	if err := binary.Read(rd, binary.BigEndian, &truncateFlag); err != nil {
		return fetchResponsePayload{}, err
	}

	payload := fetchResponsePayload{}
	if truncateFlag == 1 {
		var truncateTo uint64
		if err := binary.Read(rd, binary.BigEndian, &truncateTo); err != nil {
			return fetchResponsePayload{}, err
		}
		payload.TruncateTo = &truncateTo
	}

	if err := binary.Read(rd, binary.BigEndian, &payload.LeaderHW); err != nil {
		return fetchResponsePayload{}, err
	}
	var count uint32
	if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
		return fetchResponsePayload{}, err
	}
	payload.Records = make([]isr.Record, 0, count)
	for i := uint32(0); i < count; i++ {
		var sizeBytes int64
		if err := binary.Read(rd, binary.BigEndian, &sizeBytes); err != nil {
			return fetchResponsePayload{}, err
		}
		var payloadLen uint32
		if err := binary.Read(rd, binary.BigEndian, &payloadLen); err != nil {
			return fetchResponsePayload{}, err
		}
		recordPayload := make([]byte, payloadLen)
		if _, err := io.ReadFull(rd, recordPayload); err != nil {
			return fetchResponsePayload{}, err
		}
		payload.Records = append(payload.Records, isr.Record{
			Payload:   recordPayload,
			SizeBytes: int(sizeBytes),
		})
	}
	if rd.Len() != 0 {
		return fetchResponsePayload{}, errors.New("isrnode: trailing fetch response payload bytes")
	}
	return payload, nil
}

func (r *runtime) handleEnvelope(env Envelope) {
	var (
		g         *group
		knownDrop bool
	)

	r.mu.Lock()
	r.dropExpiredTombstonesLocked(r.cfg.Now())
	if active, ok := r.groups[env.GroupKey]; ok && active.generation == env.Generation {
		g = active
	} else if generations, ok := r.tombstones[env.GroupKey]; ok {
		if _, ok := generations[env.Generation]; ok {
			knownDrop = true
		}
	}
	r.mu.Unlock()

	if env.Kind == MessageKindFetchResponse && (g != nil || knownDrop) {
		r.releasePeerInflight(env.Peer)
		defer r.drainPeerQueue(env.Peer)
	}

	if g == nil || knownDrop {
		return
	}
	r.deliverEnvelope(g, env)
}

func (r *runtime) deliverEnvelope(g *group, env Envelope) {
	switch env.Kind {
	case MessageKindFetchResponse:
		meta := g.metaSnapshot()
		if env.Epoch != meta.Epoch {
			return
		}
		payload, err := decodeFetchResponsePayload(env.Payload)
		if err != nil {
			return
		}
		_ = g.replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
			GroupKey:   env.GroupKey,
			Epoch:      env.Epoch,
			Leader:     env.Peer,
			TruncateTo: payload.TruncateTo,
			Records:    payload.Records,
			LeaderHW:   payload.LeaderHW,
		})
	}
}
