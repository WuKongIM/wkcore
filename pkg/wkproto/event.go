package wkproto

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

func decodeEvent(frame wkpacket.Frame, data []byte, _ uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)
	eventPacket := &wkpacket.EventPacket{}
	eventPacket.Framer = frame.(wkpacket.Framer)
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

func encodeEvent(eventPacket *wkpacket.EventPacket, enc *Encoder, _ uint8) error {
	enc.WriteString(eventPacket.Id)
	enc.WriteString(eventPacket.Type)
	enc.WriteInt64(eventPacket.Timestamp)
	enc.WriteBytes(eventPacket.Data)
	return nil
}

func encodeEventSize(eventPacket *wkpacket.EventPacket, _ uint8) int {
	size := 0
	size += len(eventPacket.Id) + wkpacket.StringFixLenByteSize
	size += len(eventPacket.Type) + wkpacket.StringFixLenByteSize
	size += wkpacket.BigTimestampByteSize
	size += len(eventPacket.Data)
	return size
}
