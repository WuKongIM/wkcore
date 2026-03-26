package wkdb

const (
	TableIDUser    uint32 = 1
	TableIDChannel uint32 = 2

	maxKeyStringLen = 1<<16 - 1
)

const (
	userPrimaryFamilyID uint16 = 0
	userPrimaryIndexID  uint16 = 1

	userColumnIDUID         uint16 = 1
	userColumnIDToken       uint16 = 2
	userColumnIDDeviceFlag  uint16 = 3
	userColumnIDDeviceLevel uint16 = 4
)

const (
	channelPrimaryFamilyID   uint16 = 0
	channelPrimaryIndexID    uint16 = 1
	channelIndexIDChannelID  uint16 = 2
	channelColumnIDChannelID uint16 = 1
	channelColumnIDType      uint16 = 2
	channelColumnIDBan       uint16 = 3
)

type ColumnType int

const (
	ColumnString ColumnType = iota + 1
	ColumnInt64
)

type ColumnDesc struct {
	ID       uint16
	Name     string
	Type     ColumnType
	Nullable bool
}

type ColumnFamilyDesc struct {
	ID              uint16
	Name            string
	ColumnIDs       []uint16
	DefaultColumnID uint16
}

type IndexDesc struct {
	ID        uint16
	Name      string
	Unique    bool
	ColumnIDs []uint16
	Primary   bool
}

type TableDesc struct {
	ID               uint32
	Name             string
	Columns          []ColumnDesc
	Families         []ColumnFamilyDesc
	PrimaryIndex     IndexDesc
	SecondaryIndexes []IndexDesc
}

var UserTable = &TableDesc{
	ID:   TableIDUser,
	Name: "user",
	Columns: []ColumnDesc{
		{ID: userColumnIDUID, Name: "uid", Type: ColumnString},
		{ID: userColumnIDToken, Name: "token", Type: ColumnString},
		{ID: userColumnIDDeviceFlag, Name: "device_flag", Type: ColumnInt64},
		{ID: userColumnIDDeviceLevel, Name: "device_level", Type: ColumnInt64},
	},
	Families: []ColumnFamilyDesc{
		{
			ID:              userPrimaryFamilyID,
			Name:            "primary",
			ColumnIDs:       []uint16{userColumnIDToken, userColumnIDDeviceFlag, userColumnIDDeviceLevel},
			DefaultColumnID: userColumnIDToken,
		},
	},
	PrimaryIndex: IndexDesc{
		ID:        userPrimaryIndexID,
		Name:      "pk_user",
		Unique:    true,
		Primary:   true,
		ColumnIDs: []uint16{userColumnIDUID},
	},
}

var ChannelTable = &TableDesc{
	ID:   TableIDChannel,
	Name: "channel",
	Columns: []ColumnDesc{
		{ID: channelColumnIDChannelID, Name: "channel_id", Type: ColumnString},
		{ID: channelColumnIDType, Name: "channel_type", Type: ColumnInt64},
		{ID: channelColumnIDBan, Name: "ban", Type: ColumnInt64},
	},
	Families: []ColumnFamilyDesc{
		{
			ID:              channelPrimaryFamilyID,
			Name:            "primary",
			ColumnIDs:       []uint16{channelColumnIDBan},
			DefaultColumnID: channelColumnIDBan,
		},
	},
	PrimaryIndex: IndexDesc{
		ID:        channelPrimaryIndexID,
		Name:      "pk_channel",
		Unique:    true,
		Primary:   true,
		ColumnIDs: []uint16{channelColumnIDChannelID, channelColumnIDType},
	},
	SecondaryIndexes: []IndexDesc{
		{
			ID:        channelIndexIDChannelID,
			Name:      "idx_channel_id",
			Unique:    false,
			ColumnIDs: []uint16{channelColumnIDChannelID},
		},
	},
}
