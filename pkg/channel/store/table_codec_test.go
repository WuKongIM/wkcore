package store

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

const expectedMessageFamilyCodecVersion byte = 1

func TestMessageTableCatalogDeclaresPrimaryPayloadAndIndexes(t *testing.T) {
	require.Equal(t, "message", MessageTable.Name)
	require.Equal(t, []uint16{messageColumnIDMessageSeq}, MessageTable.PrimaryIndex.ColumnIDs)
	require.Len(t, MessageTable.Families, 2)
	require.Equal(t, "primary", MessageTable.Families[0].Name)
	require.Equal(t, "payload", MessageTable.Families[1].Name)
	require.Len(t, MessageTable.SecondaryIndexes, 3)
	require.Equal(t, "uidx_message_id", MessageTable.SecondaryIndexes[0].Name)
	require.Equal(t, []uint16{messageColumnIDMessageID}, MessageTable.SecondaryIndexes[0].ColumnIDs)
	require.Equal(t, "idx_client_msg_no", MessageTable.SecondaryIndexes[1].Name)
	require.Equal(t, []uint16{messageColumnIDClientMsgNo, messageColumnIDMessageSeq}, MessageTable.SecondaryIndexes[1].ColumnIDs)
	require.Equal(t, "uidx_from_uid_client_msg_no", MessageTable.SecondaryIndexes[2].Name)
	require.Equal(t, []uint16{messageColumnIDFromUID, messageColumnIDClientMsgNo}, MessageTable.SecondaryIndexes[2].ColumnIDs)
}

func TestEncodeMessageFamiliesUsesVersionedColumnLengthWireFormat(t *testing.T) {
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
	require.Equal(t, expectedMessageFamilyCodecVersion, primary[0])
	require.Equal(t, expectedMessageFamilyCodecVersion, payload[0])
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

	primary = appendTestBytesColumn(primary, messageColumnIDPayloadHash+1, []byte("future"))

	decoded, err := decodeMessageFamilies(row.MessageSeq, primary, payload)
	require.NoError(t, err)
	require.Equal(t, row.MessageID, decoded.MessageID)
	require.Equal(t, row.ClientMsgNo, decoded.ClientMsgNo)
	require.Equal(t, row.FromUID, decoded.FromUID)
	require.Equal(t, row.Payload, decoded.Payload)
}

func appendTestBytesColumn(dst []byte, columnID uint16, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(columnID))
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
