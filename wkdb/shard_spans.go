package wkdb

import "encoding/binary"

type Span struct {
	Start []byte
	End   []byte
}

func slotStateSpan(slot uint64) Span {
	return newPrefixSpan(encodeSlotKeyspacePrefix(keyspaceState, slot))
}

func slotIndexSpan(slot uint64) Span {
	return newPrefixSpan(encodeSlotKeyspacePrefix(keyspaceIndex, slot))
}

func slotMetaSpan(slot uint64) Span {
	return newPrefixSpan(encodeSlotKeyspacePrefix(keyspaceMeta, slot))
}

func slotAllDataSpans(slot uint64) []Span {
	return []Span{
		slotStateSpan(slot),
		slotIndexSpan(slot),
		slotMetaSpan(slot),
	}
}

func encodeSlotKeyspacePrefix(kind byte, slot uint64) []byte {
	prefix := make([]byte, 0, 1+8)
	prefix = append(prefix, kind)
	prefix = binary.BigEndian.AppendUint64(prefix, slot)
	return prefix
}

func newPrefixSpan(prefix []byte) Span {
	return Span{
		Start: append([]byte(nil), prefix...),
		End:   nextPrefix(prefix),
	}
}

func nextPrefix(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] == 0xff {
			continue
		}
		end[i]++
		return end[:i+1]
	}
	return []byte{0xff}
}
