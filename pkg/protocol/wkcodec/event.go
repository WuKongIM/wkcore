package wkcodec

import "github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"

func decodeEvent(frame wkframe.Frame, data []byte, _ uint8) (wkframe.Frame, error) {
	dec := NewDecoder(data)
	eventPacket := &wkframe.EventPacket{}
	eventPacket.Framer = frame.(wkframe.Framer)
	var err error
	if eventPacket.Id, err = dec.String(); err != nil {
		return nil, err
	}

	if eventPacket.Type, err = dec.String(); err != nil {
		return nil, err
	}
	if eventPacket.Timestamp, err = dec.Int64(); err != nil {
		return nil, err
	}
	if eventPacket.Data, err = dec.BinaryAll(); err != nil {
		return nil, err
	}

	return eventPacket, nil
}

func encodeEvent(eventPacket *wkframe.EventPacket, enc *Encoder, _ uint8) error {
	enc.WriteString(eventPacket.Id)
	enc.WriteString(eventPacket.Type)
	enc.WriteInt64(eventPacket.Timestamp)
	enc.WriteBytes(eventPacket.Data)
	return nil
}

func encodeEventSize(eventPacket *wkframe.EventPacket, _ uint8) int {
	size := 0
	size += len(eventPacket.Id) + wkframe.StringFixLenByteSize
	size += len(eventPacket.Type) + wkframe.StringFixLenByteSize
	size += wkframe.BigTimestampByteSize
	size += len(eventPacket.Data)
	return size
}
