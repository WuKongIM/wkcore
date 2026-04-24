package store

// ColumnType describes the encoded value type stored for a column.
type ColumnType int

const (
	ColumnString ColumnType = iota + 1
	ColumnInt64
	ColumnUint64
	ColumnBytes
)

const (
	TableIDMessage uint32 = 1

	messagePrimaryFamilyID uint16 = 0
	messagePayloadFamilyID uint16 = 1

	messagePrimaryIndexID            uint16 = 1
	messageIndexIDMessageID          uint16 = 2
	messageIndexIDClientMsgNo        uint16 = 3
	messageIndexIDFromUIDClientMsgNo uint16 = 4

	messageColumnIDMessageSeq  uint16 = 1
	messageColumnIDMessageID   uint16 = 2
	messageColumnIDFramerFlags uint16 = 3
	messageColumnIDSetting     uint16 = 4
	messageColumnIDStreamFlag  uint16 = 5
	messageColumnIDMsgKey      uint16 = 6
	messageColumnIDExpire      uint16 = 7
	messageColumnIDClientSeq   uint16 = 8
	messageColumnIDClientMsgNo uint16 = 9
	messageColumnIDStreamNo    uint16 = 10
	messageColumnIDStreamID    uint16 = 11
	messageColumnIDTimestamp   uint16 = 12
	messageColumnIDChannelID   uint16 = 13
	messageColumnIDChannelType uint16 = 14
	messageColumnIDTopic       uint16 = 15
	messageColumnIDFromUID     uint16 = 16
	messageColumnIDPayloadHash uint16 = 17
	messageColumnIDPayload     uint16 = 18
)

// ColumnDesc defines one logical column in a table schema.
type ColumnDesc struct {
	ID       uint16
	Name     string
	Type     ColumnType
	Nullable bool
}

// ColumnFamilyDesc groups encoded columns into one value family.
type ColumnFamilyDesc struct {
	ID              uint16
	Name            string
	ColumnIDs       []uint16
	DefaultColumnID uint16
}

// IndexDesc defines one primary or secondary index layout.
type IndexDesc struct {
	ID        uint16
	Name      string
	Unique    bool
	ColumnIDs []uint16
	Primary   bool
}

// TableDesc defines the structured message table schema.
type TableDesc struct {
	ID               uint32
	Name             string
	Columns          []ColumnDesc
	Families         []ColumnFamilyDesc
	PrimaryIndex     IndexDesc
	SecondaryIndexes []IndexDesc
}

// MessageTable describes the structured row layout used for channel messages.
var MessageTable = &TableDesc{
	ID:   TableIDMessage,
	Name: "message",
	Columns: []ColumnDesc{
		{ID: messageColumnIDMessageSeq, Name: "message_seq", Type: ColumnUint64},
		{ID: messageColumnIDMessageID, Name: "message_id", Type: ColumnUint64},
		{ID: messageColumnIDFramerFlags, Name: "framer_flags", Type: ColumnUint64},
		{ID: messageColumnIDSetting, Name: "setting", Type: ColumnUint64},
		{ID: messageColumnIDStreamFlag, Name: "stream_flag", Type: ColumnUint64},
		{ID: messageColumnIDMsgKey, Name: "msg_key", Type: ColumnString},
		{ID: messageColumnIDExpire, Name: "expire", Type: ColumnUint64},
		{ID: messageColumnIDClientSeq, Name: "client_seq", Type: ColumnUint64},
		{ID: messageColumnIDClientMsgNo, Name: "client_msg_no", Type: ColumnString, Nullable: true},
		{ID: messageColumnIDStreamNo, Name: "stream_no", Type: ColumnString, Nullable: true},
		{ID: messageColumnIDStreamID, Name: "stream_id", Type: ColumnUint64},
		{ID: messageColumnIDTimestamp, Name: "timestamp", Type: ColumnInt64},
		{ID: messageColumnIDChannelID, Name: "channel_id", Type: ColumnString},
		{ID: messageColumnIDChannelType, Name: "channel_type", Type: ColumnUint64},
		{ID: messageColumnIDTopic, Name: "topic", Type: ColumnString, Nullable: true},
		{ID: messageColumnIDFromUID, Name: "from_uid", Type: ColumnString, Nullable: true},
		{ID: messageColumnIDPayloadHash, Name: "payload_hash", Type: ColumnUint64},
		{ID: messageColumnIDPayload, Name: "payload", Type: ColumnBytes, Nullable: true},
	},
	Families: []ColumnFamilyDesc{
		{
			ID:   messagePrimaryFamilyID,
			Name: "primary",
			ColumnIDs: []uint16{
				messageColumnIDMessageID,
				messageColumnIDFramerFlags,
				messageColumnIDSetting,
				messageColumnIDStreamFlag,
				messageColumnIDMsgKey,
				messageColumnIDExpire,
				messageColumnIDClientSeq,
				messageColumnIDClientMsgNo,
				messageColumnIDStreamNo,
				messageColumnIDStreamID,
				messageColumnIDTimestamp,
				messageColumnIDChannelID,
				messageColumnIDChannelType,
				messageColumnIDTopic,
				messageColumnIDFromUID,
				messageColumnIDPayloadHash,
			},
			DefaultColumnID: messageColumnIDMessageID,
		},
		{
			ID:              messagePayloadFamilyID,
			Name:            "payload",
			ColumnIDs:       []uint16{messageColumnIDPayload},
			DefaultColumnID: messageColumnIDPayload,
		},
	},
	PrimaryIndex: IndexDesc{
		ID:        messagePrimaryIndexID,
		Name:      "pk_message",
		Unique:    true,
		Primary:   true,
		ColumnIDs: []uint16{messageColumnIDMessageSeq},
	},
	SecondaryIndexes: []IndexDesc{
		{
			ID:        messageIndexIDMessageID,
			Name:      "uidx_message_id",
			Unique:    true,
			ColumnIDs: []uint16{messageColumnIDMessageID},
		},
		{
			ID:        messageIndexIDClientMsgNo,
			Name:      "idx_client_msg_no",
			ColumnIDs: []uint16{messageColumnIDClientMsgNo},
		},
		{
			ID:        messageIndexIDFromUIDClientMsgNo,
			Name:      "uidx_from_uid_client_msg_no",
			Unique:    true,
			ColumnIDs: []uint16{messageColumnIDFromUID, messageColumnIDClientMsgNo},
		},
	},
}
