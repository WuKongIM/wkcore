package store

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageTableCatalogDeclaresPrimaryPayloadAndIndexes(t *testing.T) {
	require.Equal(t, "message", MessageTable.Name)
	require.Len(t, MessageTable.Families, 2)
	require.Equal(t, "primary", MessageTable.Families[0].Name)
	require.Equal(t, "payload", MessageTable.Families[1].Name)
	require.Equal(t, "uidx_message_id", MessageTable.SecondaryIndexes[0].Name)
}

func TestDecodeMessageFamiliesSkipsUnknownColumns(t *testing.T) {
	row := messageRow{
		MessageSeq:  9,
		MessageID:   42,
		ClientMsgNo: "c-1",
		FromUID:     "u1",
		ChannelID:   "room",
		ChannelType: 1,
		Payload:     []byte("hello"),
		PayloadHash: 123,
	}

	primary, payload, err := encodeMessageFamilies(row)
	require.NoError(t, err)

	primary = appendTestBytesColumn(primary, messageColumnIDPayloadHash+1, messageColumnIDPayloadHash, []byte("future"))

	decoded, err := decodeMessageFamilies(row.MessageSeq, primary, payload)
	require.NoError(t, err)
	require.Equal(t, row.MessageID, decoded.MessageID)
	require.Equal(t, row.ClientMsgNo, decoded.ClientMsgNo)
	require.Equal(t, row.FromUID, decoded.FromUID)
	require.Equal(t, row.Payload, decoded.Payload)
}

func appendTestBytesColumn(dst []byte, columnID uint16, prevColumnID uint16, value []byte) []byte {
	delta := columnID - prevColumnID
	dst = append(dst, byte(delta<<4)|familyValueTypeBytes)
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
