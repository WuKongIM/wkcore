package wkstore

import (
	"encoding/binary"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/controller/wkdb"
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

	cmdTypeUpsertUser    uint8 = 1
	cmdTypeUpsertChannel uint8 = 2
	cmdTypeDeleteChannel uint8 = 3

	// User field tags.
	tagUserUID         uint8 = 1
	tagUserToken       uint8 = 2
	tagUserDeviceFlag  uint8 = 3
	tagUserDeviceLevel uint8 = 4

	// Channel field tags.
	tagChannelID   uint8 = 1
	tagChannelType uint8 = 2
	tagChannelBan  uint8 = 3

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
	apply(wb *wkdb.WriteBatch, slot uint64) error
}

// commandDecoder parses TLV fields after the header into a typed command.
type commandDecoder func(data []byte) (command, error)

// commandDecoders maps command type bytes to their decoders.
// To add a new command type, create a struct implementing command,
// a corresponding encode function, a decoder, and register it here.
var commandDecoders = map[uint8]commandDecoder{
	cmdTypeUpsertUser:    decodeUpsertUser,
	cmdTypeUpsertChannel: decodeUpsertChannel,
	cmdTypeDeleteChannel: decodeDeleteChannel,
}

// --- UpsertUser ---

type upsertUserCmd struct {
	user wkdb.User
}

func (c *upsertUserCmd) apply(wb *wkdb.WriteBatch, slot uint64) error {
	return wb.UpsertUser(slot, c.user)
}

// --- UpsertChannel ---

type upsertChannelCmd struct {
	channel wkdb.Channel
}

func (c *upsertChannelCmd) apply(wb *wkdb.WriteBatch, slot uint64) error {
	return wb.UpsertChannel(slot, c.channel)
}

// --- DeleteChannel ---

type deleteChannelCmd struct {
	channelID   string
	channelType int64
}

func (c *deleteChannelCmd) apply(wb *wkdb.WriteBatch, slot uint64) error {
	return wb.DeleteChannel(slot, c.channelID, c.channelType)
}

// EncodeUpsertUserCommand encodes a User into a binary command.
func EncodeUpsertUserCommand(u wkdb.User) []byte {
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
	buf[off] = cmdTypeUpsertUser
	off++

	off = putStringField(buf, off, tagUserUID, u.UID)
	off = putStringField(buf, off, tagUserToken, u.Token)
	off = putInt64Field(buf, off, tagUserDeviceFlag, u.DeviceFlag)
	_ = putInt64Field(buf, off, tagUserDeviceLevel, u.DeviceLevel)

	return buf
}

// EncodeUpsertChannelCommand encodes a Channel into a binary command.
func EncodeUpsertChannelCommand(ch wkdb.Channel) []byte {
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

// decodeCommand parses a binary-encoded command using the decoder registry.
func decodeCommand(data []byte) (command, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("%w: command too short", wkdb.ErrCorruptValue)
	}

	version := data[0]
	if version != commandVersion {
		return nil, fmt.Errorf("%w: unsupported command version %d", wkdb.ErrCorruptValue, version)
	}

	cmdType := data[1]
	decoder, ok := commandDecoders[cmdType]
	if !ok {
		return nil, fmt.Errorf("%w: unknown command type %d", wkdb.ErrInvalidArgument, cmdType)
	}
	return decoder(data[headerSize:])
}

func decodeUpsertUser(data []byte) (command, error) {
	var u wkdb.User
	off := 0
	for off < len(data) {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n
		switch tag {
		case tagUserUID:
			u.UID = string(value)
		case tagUserToken:
			u.Token = string(value)
		case tagUserDeviceFlag:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad DeviceFlag length", wkdb.ErrCorruptValue)
			}
			u.DeviceFlag = int64(binary.BigEndian.Uint64(value))
		case tagUserDeviceLevel:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad DeviceLevel length", wkdb.ErrCorruptValue)
			}
			u.DeviceLevel = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return &upsertUserCmd{user: u}, nil
}

func decodeUpsertChannel(data []byte) (command, error) {
	var ch wkdb.Channel
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
				return nil, fmt.Errorf("%w: bad ChannelType length", wkdb.ErrCorruptValue)
			}
			ch.ChannelType = int64(binary.BigEndian.Uint64(value))
		case tagChannelBan:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad Ban length", wkdb.ErrCorruptValue)
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
				return nil, fmt.Errorf("%w: bad ChannelType length", wkdb.ErrCorruptValue)
			}
			cmd.channelType = int64(binary.BigEndian.Uint64(value))
		default:
			// Unknown tag — skip for forward compatibility.
		}
	}
	return &cmd, nil
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

// readTLV reads one TLV entry from data and returns (tag, value, bytesConsumed, error).
func readTLV(data []byte) (uint8, []byte, int, error) {
	if len(data) < tlvOverhead {
		return 0, nil, 0, fmt.Errorf("%w: truncated TLV header", wkdb.ErrCorruptValue)
	}
	tag := data[0]
	length := int(binary.BigEndian.Uint32(data[1:]))
	end := tlvOverhead + length
	if end > len(data) {
		return 0, nil, 0, fmt.Errorf("%w: truncated TLV value (tag=%d, need=%d, have=%d)",
			wkdb.ErrCorruptValue, tag, length, len(data)-tlvOverhead)
	}
	return tag, data[tlvOverhead:end], end, nil
}
