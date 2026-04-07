package metafsm

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

// Wire format (version 1):
//
//	[version:1][cmdType:1][TLV fields...]
//
// Each TLV field:
//
//	[tag:1][length:4 big-endian][value:length bytes]
//
// Unknown tags are skipped by the decoder, so new fields can be added
// without breaking older readers (forward-compatible).

const (
	commandVersion uint8 = 1

	cmdTypeUpsertUser               uint8 = 1
	cmdTypeUpsertChannel            uint8 = 2
	cmdTypeDeleteChannel            uint8 = 3
	cmdTypeUpsertChannelRuntimeMeta uint8 = 4
	cmdTypeDeleteChannelRuntimeMeta uint8 = 5
	cmdTypeCreateUser               uint8 = 6
	cmdTypeUpsertDevice             uint8 = 7
	cmdTypeAddSubscribers           uint8 = 8
	cmdTypeRemoveSubscribers        uint8 = 9

	// User field tags.
	tagUserUID         uint8 = 1
	tagUserToken       uint8 = 2
	tagUserDeviceFlag  uint8 = 3
	tagUserDeviceLevel uint8 = 4

	// Device field tags.
	tagDeviceUID   uint8 = 1
	tagDeviceFlag  uint8 = 2
	tagDeviceToken uint8 = 3
	tagDeviceLevel uint8 = 4

	// Channel field tags.
	tagChannelID   uint8 = 1
	tagChannelType uint8 = 2
	tagChannelBan  uint8 = 3

	// Channel runtime metadata field tags.
	tagRuntimeMetaChannelID    uint8 = 1
	tagRuntimeMetaChannelType  uint8 = 2
	tagRuntimeMetaChannelEpoch uint8 = 3
	tagRuntimeMetaLeaderEpoch  uint8 = 4
	tagRuntimeMetaReplicas     uint8 = 5
	tagRuntimeMetaISR          uint8 = 6
	tagRuntimeMetaLeader       uint8 = 7
	tagRuntimeMetaMinISR       uint8 = 8
	tagRuntimeMetaStatus       uint8 = 9
	tagRuntimeMetaFeatures     uint8 = 10
	tagRuntimeMetaLeaseUntilMS uint8 = 11

	// Subscriber field tags.
	tagSubscriberChannelID   uint8 = 1
	tagSubscriberChannelType uint8 = 2
	tagSubscriberUIDs        uint8 = 3

	// ApplyResultOK is the result returned by Apply/ApplyBatch on success.
	ApplyResultOK = "ok"

	// headerSize is version (1) + cmdType (1).
	headerSize = 2
	// tlvOverhead is tag (1) + length (4).
	tlvOverhead = 5
)

// command is the decoded representation of a state machine command.
// Each command type implements this interface, carrying its own typed
// payload and knowing how to apply itself to a WriteBatch.
type command interface {
	apply(wb *metadb.WriteBatch, slot uint64) error
}

// commandDecoder parses TLV fields after the header into a typed command.
type commandDecoder func(data []byte) (command, error)

// commandDecoders maps command type bytes to their decoders.
// To add a new command type, create a struct implementing command,
// a corresponding encode function, a decoder, and register it here.
var commandDecoders = map[uint8]commandDecoder{
	cmdTypeUpsertUser:               decodeUpsertUser,
	cmdTypeUpsertChannel:            decodeUpsertChannel,
	cmdTypeDeleteChannel:            decodeDeleteChannel,
	cmdTypeUpsertChannelRuntimeMeta: decodeUpsertChannelRuntimeMeta,
	cmdTypeDeleteChannelRuntimeMeta: decodeDeleteChannelRuntimeMeta,
	cmdTypeCreateUser:               decodeCreateUser,
	cmdTypeUpsertDevice:             decodeUpsertDevice,
	cmdTypeAddSubscribers:           decodeAddSubscribers,
	cmdTypeRemoveSubscribers:        decodeRemoveSubscribers,
}

// --- UpsertUser ---

type upsertUserCmd struct {
	user metadb.User
}

func (c *upsertUserCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.UpsertUser(slot, c.user)
}

// --- CreateUser ---

type createUserCmd struct {
	user metadb.User
}

func (c *createUserCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.CreateUser(slot, c.user)
}

// --- UpsertDevice ---

type upsertDeviceCmd struct {
	device metadb.Device
}

func (c *upsertDeviceCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.UpsertDevice(slot, c.device)
}

// --- UpsertChannel ---

type upsertChannelCmd struct {
	channel metadb.Channel
}

func (c *upsertChannelCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.UpsertChannel(slot, c.channel)
}

// --- DeleteChannel ---

type deleteChannelCmd struct {
	channelID   string
	channelType int64
}

func (c *deleteChannelCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.DeleteChannel(slot, c.channelID, c.channelType)
}

// --- UpsertChannelRuntimeMeta ---

type upsertChannelRuntimeMetaCmd struct {
	meta metadb.ChannelRuntimeMeta
}

func (c *upsertChannelRuntimeMetaCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.UpsertChannelRuntimeMeta(slot, c.meta)
}

// --- DeleteChannelRuntimeMeta ---

type deleteChannelRuntimeMetaCmd struct {
	channelID   string
	channelType int64
}

func (c *deleteChannelRuntimeMetaCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.DeleteChannelRuntimeMeta(slot, c.channelID, c.channelType)
}

// --- AddSubscribers ---

type addSubscribersCmd struct {
	channelID   string
	channelType int64
	uids        []string
}

func (c *addSubscribersCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.AddSubscribers(slot, c.channelID, c.channelType, c.uids)
}

// --- RemoveSubscribers ---

type removeSubscribersCmd struct {
	channelID   string
	channelType int64
	uids        []string
}

func (c *removeSubscribersCmd) apply(wb *metadb.WriteBatch, slot uint64) error {
	return wb.RemoveSubscribers(slot, c.channelID, c.channelType, c.uids)
}

// EncodeUpsertUserCommand encodes a User into a binary command.
func EncodeUpsertUserCommand(u metadb.User) []byte {
	return encodeUserCommand(cmdTypeUpsertUser, u)
}

// EncodeCreateUserCommand encodes a create-only User command.
func EncodeCreateUserCommand(u metadb.User) []byte {
	return encodeUserCommand(cmdTypeCreateUser, u)
}

func encodeUserCommand(cmdType uint8, u metadb.User) []byte {
	uidLen := len(u.UID)
	tokenLen := len(u.Token)
	// header + 2 string fields + 2 int64 fields
	size := headerSize +
		tlvOverhead + uidLen +
		tlvOverhead + tokenLen +
		tlvOverhead + 8 +
		tlvOverhead + 8

	buf := make([]byte, size)
	off := 0

	buf[off] = commandVersion
	off++
	buf[off] = cmdType
	off++

	off = putStringField(buf, off, tagUserUID, u.UID)
	off = putStringField(buf, off, tagUserToken, u.Token)
	off = putInt64Field(buf, off, tagUserDeviceFlag, u.DeviceFlag)
	_ = putInt64Field(buf, off, tagUserDeviceLevel, u.DeviceLevel)

	return buf
}

// EncodeUpsertDeviceCommand encodes a Device into a binary command.
func EncodeUpsertDeviceCommand(d metadb.Device) []byte {
	uidLen := len(d.UID)
	tokenLen := len(d.Token)
	size := headerSize +
		tlvOverhead + uidLen +
		tlvOverhead + 8 +
		tlvOverhead + tokenLen +
		tlvOverhead + 8

	buf := make([]byte, size)
	off := 0

	buf[off] = commandVersion
	off++
	buf[off] = cmdTypeUpsertDevice
	off++

	off = putStringField(buf, off, tagDeviceUID, d.UID)
	off = putInt64Field(buf, off, tagDeviceFlag, d.DeviceFlag)
	off = putStringField(buf, off, tagDeviceToken, d.Token)
	_ = putInt64Field(buf, off, tagDeviceLevel, d.DeviceLevel)

	return buf
}

// EncodeUpsertChannelCommand encodes a Channel into a binary command.
func EncodeUpsertChannelCommand(ch metadb.Channel) []byte {
	idLen := len(ch.ChannelID)
	// header + 1 string field + 2 int64 fields
	size := headerSize +
		tlvOverhead + idLen +
		tlvOverhead + 8 +
		tlvOverhead + 8

	buf := make([]byte, size)
	off := 0

	buf[off] = commandVersion
	off++
	buf[off] = cmdTypeUpsertChannel
	off++

	off = putStringField(buf, off, tagChannelID, ch.ChannelID)
	off = putInt64Field(buf, off, tagChannelType, ch.ChannelType)
	_ = putInt64Field(buf, off, tagChannelBan, ch.Ban)

	return buf
}

// EncodeDeleteChannelCommand encodes a channel deletion into a binary command.
func EncodeDeleteChannelCommand(channelID string, channelType int64) []byte {
	size := headerSize +
		tlvOverhead + len(channelID) +
		tlvOverhead + 8
	buf := make([]byte, size)
	buf[0] = commandVersion
	buf[1] = cmdTypeDeleteChannel
	off := headerSize
	off = putStringField(buf, off, tagChannelID, channelID)
	putInt64Field(buf, off, tagChannelType, channelType)
	return buf
}

// EncodeUpsertChannelRuntimeMetaCommand encodes channel runtime metadata into a binary command.
func EncodeUpsertChannelRuntimeMetaCommand(meta metadb.ChannelRuntimeMeta) []byte {
	meta = canonicalizeChannelRuntimeMeta(meta)

	buf := make([]byte, 0, headerSize+128)
	buf = append(buf, commandVersion, cmdTypeUpsertChannelRuntimeMeta)
	buf = appendStringTLVField(buf, tagRuntimeMetaChannelID, meta.ChannelID)
	buf = appendInt64TLVField(buf, tagRuntimeMetaChannelType, meta.ChannelType)
	buf = appendUint64TLVField(buf, tagRuntimeMetaChannelEpoch, meta.ChannelEpoch)
	buf = appendUint64TLVField(buf, tagRuntimeMetaLeaderEpoch, meta.LeaderEpoch)
	buf = appendBytesTLVField(buf, tagRuntimeMetaReplicas, encodeUint64Slice(meta.Replicas))
	buf = appendBytesTLVField(buf, tagRuntimeMetaISR, encodeUint64Slice(meta.ISR))
	buf = appendUint64TLVField(buf, tagRuntimeMetaLeader, meta.Leader)
	buf = appendInt64TLVField(buf, tagRuntimeMetaMinISR, meta.MinISR)
	buf = appendUint64TLVField(buf, tagRuntimeMetaStatus, uint64(meta.Status))
	buf = appendUint64TLVField(buf, tagRuntimeMetaFeatures, meta.Features)
	buf = appendInt64TLVField(buf, tagRuntimeMetaLeaseUntilMS, meta.LeaseUntilMS)
	return buf
}

// EncodeDeleteChannelRuntimeMetaCommand encodes runtime metadata deletion into a binary command.
func EncodeDeleteChannelRuntimeMetaCommand(channelID string, channelType int64) []byte {
	buf := make([]byte, 0, headerSize+len(channelID)+18)
	buf = append(buf, commandVersion, cmdTypeDeleteChannelRuntimeMeta)
	buf = appendStringTLVField(buf, tagRuntimeMetaChannelID, channelID)
	buf = appendInt64TLVField(buf, tagRuntimeMetaChannelType, channelType)
	return buf
}

// EncodeAddSubscribersCommand encodes a subscriber add command.
func EncodeAddSubscribersCommand(channelID string, channelType int64, uids []string) []byte {
	return encodeSubscribersCommand(cmdTypeAddSubscribers, channelID, channelType, uids)
}

// EncodeRemoveSubscribersCommand encodes a subscriber removal command.
func EncodeRemoveSubscribersCommand(channelID string, channelType int64, uids []string) []byte {
	return encodeSubscribersCommand(cmdTypeRemoveSubscribers, channelID, channelType, uids)
}

func encodeSubscribersCommand(cmdType uint8, channelID string, channelType int64, uids []string) []byte {
	buf := make([]byte, 0, headerSize+len(channelID)+len(uids)*8)
	buf = append(buf, commandVersion, cmdType)
	buf = appendStringTLVField(buf, tagSubscriberChannelID, channelID)
	buf = appendInt64TLVField(buf, tagSubscriberChannelType, channelType)
	buf = appendBytesTLVField(buf, tagSubscriberUIDs, encodeStringSet(uids))
	return buf
}

// decodeCommand parses a binary-encoded command using the decoder registry.
func decodeCommand(data []byte) (command, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("%w: command too short", metadb.ErrCorruptValue)
	}

	version := data[0]
	if version != commandVersion {
		return nil, fmt.Errorf("%w: unsupported command version %d", metadb.ErrCorruptValue, version)
	}

	cmdType := data[1]
	decoder, ok := commandDecoders[cmdType]
	if !ok {
		return nil, fmt.Errorf("%w: unknown command type %d", metadb.ErrInvalidArgument, cmdType)
	}
	return decoder(data[headerSize:])
}

func decodeUpsertUser(data []byte) (command, error) {
	u, err := decodeUser(data)
	if err != nil {
		return nil, err
	}
	return &upsertUserCmd{user: u}, nil
}

func decodeCreateUser(data []byte) (command, error) {
	u, err := decodeUser(data)
	if err != nil {
		return nil, err
	}
	return &createUserCmd{user: u}, nil
}

func decodeUser(data []byte) (metadb.User, error) {
	var u metadb.User
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return metadb.User{}, err
		}
		off += n
		switch tag {
		case tagUserUID:
			u.UID = string(value)
		case tagUserToken:
			u.Token = string(value)
		case tagUserDeviceFlag:
			if len(value) != 8 {
				return metadb.User{}, fmt.Errorf("%w: bad DeviceFlag length", metadb.ErrCorruptValue)
			}
			u.DeviceFlag = int64(binary.BigEndian.Uint64(value))
		case tagUserDeviceLevel:
			if len(value) != 8 {
				return metadb.User{}, fmt.Errorf("%w: bad DeviceLevel length", metadb.ErrCorruptValue)
			}
			u.DeviceLevel = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return u, nil
}

func decodeUpsertDevice(data []byte) (command, error) {
	d, err := decodeDevice(data)
	if err != nil {
		return nil, err
	}
	return &upsertDeviceCmd{device: d}, nil
}

func decodeDevice(data []byte) (metadb.Device, error) {
	var d metadb.Device
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return metadb.Device{}, err
		}
		off += n
		switch tag {
		case tagDeviceUID:
			d.UID = string(value)
		case tagDeviceFlag:
			if len(value) != 8 {
				return metadb.Device{}, fmt.Errorf("%w: bad DeviceFlag length", metadb.ErrCorruptValue)
			}
			d.DeviceFlag = int64(binary.BigEndian.Uint64(value))
		case tagDeviceToken:
			d.Token = string(value)
		case tagDeviceLevel:
			if len(value) != 8 {
				return metadb.Device{}, fmt.Errorf("%w: bad DeviceLevel length", metadb.ErrCorruptValue)
			}
			d.DeviceLevel = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return d, nil
}

func decodeUpsertChannel(data []byte) (command, error) {
	var ch metadb.Channel
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n
		switch tag {
		case tagChannelID:
			ch.ChannelID = string(value)
		case tagChannelType:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad ChannelType length", metadb.ErrCorruptValue)
			}
			ch.ChannelType = int64(binary.BigEndian.Uint64(value))
		case tagChannelBan:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad Ban length", metadb.ErrCorruptValue)
			}
			ch.Ban = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return &upsertChannelCmd{channel: ch}, nil
}

func decodeDeleteChannel(data []byte) (command, error) {
	var cmd deleteChannelCmd
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n
		switch tag {
		case tagChannelID:
			cmd.channelID = string(value)
		case tagChannelType:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad ChannelType length", metadb.ErrCorruptValue)
			}
			cmd.channelType = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return &cmd, nil
}

func decodeUpsertChannelRuntimeMeta(data []byte) (command, error) {
	var meta metadb.ChannelRuntimeMeta
	var (
		haveChannelID    bool
		haveChannelType  bool
		haveChannelEpoch bool
		haveLeaderEpoch  bool
		haveReplicas     bool
		haveISR          bool
		haveLeader       bool
		haveMinISR       bool
		haveStatus       bool
		haveFeatures     bool
		haveLeaseUntilMS bool
	)
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n

		switch tag {
		case tagRuntimeMetaChannelID:
			meta.ChannelID = string(value)
			haveChannelID = true
		case tagRuntimeMetaChannelType:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime ChannelType length", metadb.ErrCorruptValue)
			}
			meta.ChannelType = int64(binary.BigEndian.Uint64(value))
			haveChannelType = true
		case tagRuntimeMetaChannelEpoch:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime ChannelEpoch length", metadb.ErrCorruptValue)
			}
			meta.ChannelEpoch = binary.BigEndian.Uint64(value)
			haveChannelEpoch = true
		case tagRuntimeMetaLeaderEpoch:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime LeaderEpoch length", metadb.ErrCorruptValue)
			}
			meta.LeaderEpoch = binary.BigEndian.Uint64(value)
			haveLeaderEpoch = true
		case tagRuntimeMetaReplicas:
			meta.Replicas, err = decodeUint64Slice(value)
			if err != nil {
				return nil, err
			}
			haveReplicas = true
		case tagRuntimeMetaISR:
			meta.ISR, err = decodeUint64Slice(value)
			if err != nil {
				return nil, err
			}
			haveISR = true
		case tagRuntimeMetaLeader:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime Leader length", metadb.ErrCorruptValue)
			}
			meta.Leader = binary.BigEndian.Uint64(value)
			haveLeader = true
		case tagRuntimeMetaMinISR:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime MinISR length", metadb.ErrCorruptValue)
			}
			meta.MinISR = int64(binary.BigEndian.Uint64(value))
			haveMinISR = true
		case tagRuntimeMetaStatus:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime Status length", metadb.ErrCorruptValue)
			}
			raw := binary.BigEndian.Uint64(value)
			if raw > uint64(^uint8(0)) {
				return nil, fmt.Errorf("%w: bad runtime Status value %d", metadb.ErrCorruptValue, raw)
			}
			meta.Status = uint8(raw)
			haveStatus = true
		case tagRuntimeMetaFeatures:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime Features length", metadb.ErrCorruptValue)
			}
			meta.Features = binary.BigEndian.Uint64(value)
			haveFeatures = true
		case tagRuntimeMetaLeaseUntilMS:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime LeaseUntilMS length", metadb.ErrCorruptValue)
			}
			meta.LeaseUntilMS = int64(binary.BigEndian.Uint64(value))
			haveLeaseUntilMS = true
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	if !haveChannelID || !haveChannelType || !haveChannelEpoch || !haveLeaderEpoch ||
		!haveReplicas || !haveISR || !haveLeader || !haveMinISR || !haveStatus ||
		!haveFeatures || !haveLeaseUntilMS {
		return nil, fmt.Errorf("%w: incomplete runtime metadata command", metadb.ErrCorruptValue)
	}
	return &upsertChannelRuntimeMetaCmd{meta: canonicalizeChannelRuntimeMeta(meta)}, nil
}

func decodeDeleteChannelRuntimeMeta(data []byte) (command, error) {
	var cmd deleteChannelRuntimeMetaCmd
	var haveChannelID, haveChannelType bool
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n
		switch tag {
		case tagRuntimeMetaChannelID:
			cmd.channelID = string(value)
			haveChannelID = true
		case tagRuntimeMetaChannelType:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad runtime ChannelType length", metadb.ErrCorruptValue)
			}
			cmd.channelType = int64(binary.BigEndian.Uint64(value))
			haveChannelType = true
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	if !haveChannelID || !haveChannelType {
		return nil, fmt.Errorf("%w: incomplete runtime metadata delete command", metadb.ErrCorruptValue)
	}
	return &cmd, nil
}

func decodeAddSubscribers(data []byte) (command, error) {
	return decodeSubscribersCommand(data, func(channelID string, channelType int64, uids []string) command {
		return &addSubscribersCmd{
			channelID:   channelID,
			channelType: channelType,
			uids:        uids,
		}
	})
}

func decodeRemoveSubscribers(data []byte) (command, error) {
	return decodeSubscribersCommand(data, func(channelID string, channelType int64, uids []string) command {
		return &removeSubscribersCmd{
			channelID:   channelID,
			channelType: channelType,
			uids:        uids,
		}
	})
}

func decodeSubscribersCommand(data []byte, build func(channelID string, channelType int64, uids []string) command) (command, error) {
	var (
		channelID       string
		channelType     int64
		uids            []string
		haveChannelID   bool
		haveChannelType bool
		haveUIDs        bool
	)

	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n

		switch tag {
		case tagSubscriberChannelID:
			channelID = string(value)
			haveChannelID = true
		case tagSubscriberChannelType:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad subscriber ChannelType length", metadb.ErrCorruptValue)
			}
			channelType = int64(binary.BigEndian.Uint64(value))
			haveChannelType = true
		case tagSubscriberUIDs:
			uids = decodeStringSet(value)
			haveUIDs = true
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}

	if !haveChannelID || !haveChannelType || !haveUIDs {
		return nil, fmt.Errorf("%w: incomplete subscriber command", metadb.ErrCorruptValue)
	}
	return build(channelID, channelType, uids), nil
}

// ---------- TLV helpers ----------

// putStringField writes [tag][len:4][string bytes] and returns the new offset.
func putStringField(buf []byte, off int, tag uint8, s string) int {
	buf[off] = tag
	binary.BigEndian.PutUint32(buf[off+1:], uint32(len(s)))
	off += tlvOverhead
	copy(buf[off:], s)
	return off + len(s)
}

// putInt64Field writes [tag][len:4=00000008][8-byte big-endian] and returns the new offset.
func putInt64Field(buf []byte, off int, tag uint8, v int64) int {
	buf[off] = tag
	binary.BigEndian.PutUint32(buf[off+1:], 8)
	off += tlvOverhead
	binary.BigEndian.PutUint64(buf[off:], uint64(v))
	return off + 8
}

func appendStringTLVField(dst []byte, tag uint8, value string) []byte {
	dst = append(dst, tag, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], uint32(len(value)))
	return append(dst, value...)
}

func appendBytesTLVField(dst []byte, tag uint8, value []byte) []byte {
	dst = append(dst, tag, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], uint32(len(value)))
	return append(dst, value...)
}

func appendInt64TLVField(dst []byte, tag uint8, value int64) []byte {
	dst = append(dst, tag, 0, 0, 0, 8)
	return binary.BigEndian.AppendUint64(dst, uint64(value))
}

func appendUint64TLVField(dst []byte, tag uint8, value uint64) []byte {
	dst = append(dst, tag, 0, 0, 0, 8)
	return binary.BigEndian.AppendUint64(dst, value)
}

// readTLV reads one TLV entry from data and returns (tag, value, bytesConsumed, error).
func readTLV(data []byte) (uint8, []byte, int, error) {
	if len(data) < tlvOverhead {
		return 0, nil, 0, fmt.Errorf("%w: truncated TLV header", metadb.ErrCorruptValue)
	}
	tag := data[0]
	length := int(binary.BigEndian.Uint32(data[1:]))
	end := tlvOverhead + length
	if end > len(data) {
		return 0, nil, 0, fmt.Errorf("%w: truncated TLV value (tag=%d, need=%d, have=%d)",
			metadb.ErrCorruptValue, tag, length, len(data)-tlvOverhead)
	}
	return tag, data[tlvOverhead:end], end, nil
}

func canonicalizeChannelRuntimeMeta(meta metadb.ChannelRuntimeMeta) metadb.ChannelRuntimeMeta {
	meta.Replicas = canonicalizeUint64Set(meta.Replicas)
	meta.ISR = canonicalizeUint64Set(meta.ISR)
	return meta
}

func canonicalizeUint64Set(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]uint64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	n := 1
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[n-1] {
			continue
		}
		sorted[n] = sorted[i]
		n++
	}
	return sorted[:n]
}

func encodeUint64Slice(values []uint64) []byte {
	if len(values) == 0 {
		return nil
	}
	buf := make([]byte, 8*len(values))
	for i, value := range values {
		binary.BigEndian.PutUint64(buf[i*8:], value)
	}
	return buf
}

func decodeUint64Slice(data []byte) ([]uint64, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("%w: malformed runtime uint64 slice", metadb.ErrCorruptValue)
	}
	values := make([]uint64, len(data)/8)
	for i := range values {
		values[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return values, nil
}

func encodeStringSet(values []string) []byte {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	n := 1
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[n-1] {
			continue
		}
		sorted[n] = sorted[i]
		n++
	}
	sorted = sorted[:n]
	return []byte(strings.Join(sorted, "\x00"))
}

func decodeStringSet(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	return strings.Split(string(data), "\x00")
}
