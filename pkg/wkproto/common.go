package wkproto

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type Framer = wkpacket.Framer
type FrameType = wkpacket.FrameType
type ReasonCode = wkpacket.ReasonCode
type DeviceFlag = wkpacket.DeviceFlag
type DeviceLevel = wkpacket.DeviceLevel
type Frame = wkpacket.Frame
type Channel = wkpacket.Channel

const (
	UNKNOWN    FrameType = wkpacket.UNKNOWN
	CONNECT    FrameType = wkpacket.CONNECT
	CONNACK    FrameType = wkpacket.CONNACK
	SEND       FrameType = wkpacket.SEND
	SENDACK    FrameType = wkpacket.SENDACK
	RECV       FrameType = wkpacket.RECV
	RECVACK    FrameType = wkpacket.RECVACK
	PING       FrameType = wkpacket.PING
	PONG       FrameType = wkpacket.PONG
	DISCONNECT FrameType = wkpacket.DISCONNECT
	SUB        FrameType = wkpacket.SUB
	SUBACK     FrameType = wkpacket.SUBACK
	EVENT      FrameType = wkpacket.EVENT
)

const (
	ReasonUnknown                ReasonCode = wkpacket.ReasonUnknown
	ReasonSuccess                ReasonCode = wkpacket.ReasonSuccess
	ReasonAuthFail               ReasonCode = wkpacket.ReasonAuthFail
	ReasonSubscriberNotExist     ReasonCode = wkpacket.ReasonSubscriberNotExist
	ReasonInBlacklist            ReasonCode = wkpacket.ReasonInBlacklist
	ReasonChannelNotExist        ReasonCode = wkpacket.ReasonChannelNotExist
	ReasonUserNotOnNode          ReasonCode = wkpacket.ReasonUserNotOnNode
	ReasonSenderOffline          ReasonCode = wkpacket.ReasonSenderOffline
	ReasonMsgKeyError            ReasonCode = wkpacket.ReasonMsgKeyError
	ReasonPayloadDecodeError     ReasonCode = wkpacket.ReasonPayloadDecodeError
	ReasonForwardSendPacketError ReasonCode = wkpacket.ReasonForwardSendPacketError
	ReasonNotAllowSend           ReasonCode = wkpacket.ReasonNotAllowSend
	ReasonConnectKick            ReasonCode = wkpacket.ReasonConnectKick
	ReasonNotInWhitelist         ReasonCode = wkpacket.ReasonNotInWhitelist
	ReasonQueryTokenError        ReasonCode = wkpacket.ReasonQueryTokenError
	ReasonSystemError            ReasonCode = wkpacket.ReasonSystemError
	ReasonChannelIDError         ReasonCode = wkpacket.ReasonChannelIDError
	ReasonNodeMatchError         ReasonCode = wkpacket.ReasonNodeMatchError
	ReasonNodeNotMatch           ReasonCode = wkpacket.ReasonNodeNotMatch
	ReasonBan                    ReasonCode = wkpacket.ReasonBan
	ReasonNotSupportHeader       ReasonCode = wkpacket.ReasonNotSupportHeader
	ReasonClientKeyIsEmpty       ReasonCode = wkpacket.ReasonClientKeyIsEmpty
	ReasonRateLimit              ReasonCode = wkpacket.ReasonRateLimit
	ReasonNotSupportChannelType  ReasonCode = wkpacket.ReasonNotSupportChannelType
	ReasonDisband                ReasonCode = wkpacket.ReasonDisband
	ReasonSendBan                ReasonCode = wkpacket.ReasonSendBan
)

const (
	APP    DeviceFlag = wkpacket.APP
	WEB    DeviceFlag = wkpacket.WEB
	PC     DeviceFlag = wkpacket.PC
	SYSTEM DeviceFlag = wkpacket.SYSTEM
)

const (
	DeviceLevelSlave  DeviceLevel = wkpacket.DeviceLevelSlave
	DeviceLevelMaster DeviceLevel = wkpacket.DeviceLevelMaster
)

const (
	SettingByteSize         = wkpacket.SettingByteSize
	StringFixLenByteSize    = wkpacket.StringFixLenByteSize
	ClientSeqByteSize       = wkpacket.ClientSeqByteSize
	ChannelTypeByteSize     = wkpacket.ChannelTypeByteSize
	VersionByteSize         = wkpacket.VersionByteSize
	DeviceFlagByteSize      = wkpacket.DeviceFlagByteSize
	ClientTimestampByteSize = wkpacket.ClientTimestampByteSize
	TimeDiffByteSize        = wkpacket.TimeDiffByteSize
	ReasonCodeByteSize      = wkpacket.ReasonCodeByteSize
	MessageIDByteSize       = wkpacket.MessageIDByteSize
	MessageSeqByteSize      = wkpacket.MessageSeqByteSize
	TimestampByteSize       = wkpacket.TimestampByteSize
	BigTimestampByteSize    = wkpacket.BigTimestampByteSize
	ActionByteSize          = wkpacket.ActionByteSize
	StreamIdByteSize        = wkpacket.StreamIdByteSize
	StreamFlagByteSize      = wkpacket.StreamFlagByteSize
	ExpireByteSize          = wkpacket.ExpireByteSize
	NodeIdByteSize          = wkpacket.NodeIdByteSize
	ChunkIDByteSize         = wkpacket.ChunkIDByteSize
	EndReasonByteSize       = wkpacket.EndReasonByteSize
)

const (
	ChannelTypePerson          uint8 = wkpacket.ChannelTypePerson
	ChannelTypeGroup           uint8 = wkpacket.ChannelTypeGroup
	ChannelTypeCustomerService uint8 = wkpacket.ChannelTypeCustomerService
	ChannelTypeCommunity       uint8 = wkpacket.ChannelTypeCommunity
	ChannelTypeCommunityTopic  uint8 = wkpacket.ChannelTypeCommunityTopic
	ChannelTypeInfo            uint8 = wkpacket.ChannelTypeInfo
	ChannelTypeData            uint8 = wkpacket.ChannelTypeData
	ChannelTypeTemp            uint8 = wkpacket.ChannelTypeTemp
	ChannelTypeLive            uint8 = wkpacket.ChannelTypeLive
	ChannelTypeVisitors        uint8 = wkpacket.ChannelTypeVisitors
	ChannelTypeAgent           uint8 = wkpacket.ChannelTypeAgent
	ChannelTypeAgentGroup      uint8 = wkpacket.ChannelTypeAgentGroup
)

const (
	LatestVersion    = wkpacket.LatestVersion
	MaxRemaingLength = wkpacket.MaxRemaingLength
	PayloadMaxSize   = wkpacket.PayloadMaxSize
)

func ToFixHeaderUint8(f wkpacket.Frame) uint8 {
	typeAndFlags := encodeBool(f.GetDUP())<<3 | encodeBool(f.GetsyncOnce())<<2 | encodeBool(f.GetRedDot())<<1 | encodeBool(f.GetNoPersist())
	if f.GetFrameType() == wkpacket.CONNACK {
		typeAndFlags = encodeBool(f.GetHasServerVersion())
	}
	return byte(int(f.GetFrameType()<<4) | typeAndFlags)
}

func FramerFromUint8(v uint8) wkpacket.Framer {
	p := wkpacket.Framer{}
	p.NoPersist = (v & 0x01) > 0
	p.RedDot = (v >> 1 & 0x01) > 0
	p.SyncOnce = (v >> 2 & 0x01) > 0
	p.DUP = (v >> 3 & 0x01) > 0
	p.FrameType = wkpacket.FrameType(v >> 4)

	switch p.FrameType {
	case wkpacket.CONNACK:
		p.HasServerVersion = (v & 0x01) > 0
	}

	return p
}
