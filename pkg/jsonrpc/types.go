package jsonrpc

import (
	"encoding/json"
	"fmt"

	// Import the WuKongIMGoProto package
	"strconv" // Added for MessageID parsing

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

// Constants based on the schema enums

type DeviceFlagEnum int

const (
	DeviceApp DeviceFlagEnum = 0
	DeviceWeb DeviceFlagEnum = 1
	DeviceSys DeviceFlagEnum = 2
)

type ReasonCodeEnum int

// Add specific ReasonCode values if available in wkpacket.ReasonCode
// Example:
// const (
//    ReasonCodeSuccess ReasonCodeEnum = 0
//    ReasonCodeAuthFailed ReasonCodeEnum = 1
//    // ... other reason codes
// )

type StreamFlagEnum int

const (
	StreamStart StreamFlagEnum = 0
	StreamIng   StreamFlagEnum = 1
	StreamEnd   StreamFlagEnum = 2
)

type ActionEnum int

const (
	ActionSubscribe   ActionEnum = 0
	ActionUnsubscribe ActionEnum = 1
)

// Shared structures

type Header struct {
	NoPersist bool `json:"noPersist,omitempty"`
	RedDot    bool `json:"redDot,omitempty"`
	SyncOnce  bool `json:"syncOnce,omitempty"`
	Dup       bool `json:"dup,omitempty"`
	End       bool `json:"end,omitempty"`
}

type SettingFlags struct {
	Receipt bool `json:"receipt,omitempty"`
	Signal  bool `json:"signal,omitempty"`
	Stream  bool `json:"stream,omitempty"`
	Topic   bool `json:"topic,omitempty"`
}

type ErrorObject struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"` // Keep generic for flexibility
}

// Base request/response structure components

type BaseRequest struct {
	Jsonrpc string `json:"jsonrpc,omitempty"`
	Method  string `json:"method"`
	ID      string `json:"id,omitempty"`
}

type BaseResponse struct {
	Jsonrpc string       `json:"jsonrpc,omitempty"`
	ID      string       `json:"id,omitempty"`
	Error   *ErrorObject `json:"error,omitempty"`
}

type BaseNotification struct {
	Jsonrpc string `json:"jsonrpc,omitempty"`
	Method  string `json:"method"`
}

// --- Specific Request Payloads (Params) ---

type ConnectParams struct {
	Header          Header         `json:"header,omitempty"`
	Version         int            `json:"version,omitempty"`
	ClientKey       string         `json:"clientKey,omitempty"`
	DeviceID        string         `json:"deviceId,omitempty"`
	DeviceFlag      DeviceFlagEnum `json:"deviceFlag"`
	ClientTimestamp int64          `json:"clientTimestamp,omitempty"`
	UID             string         `json:"uid"`
	Token           string         `json:"token"`
}

type SendParams struct {
	Header      Header       `json:"header,omitempty"`
	Setting     SettingFlags `json:"setting,omitempty"`
	MsgKey      string       `json:"msgKey,omitempty"`
	Expire      uint32       `json:"expire,omitempty"`
	ClientMsgNo string       `json:"clientMsgNo,omitempty"`
	StreamNo    string       `json:"streamNo,omitempty"`
	ChannelID   string       `json:"channelId"`
	ChannelType int          `json:"channelType"`
	Topic       string       `json:"topic,omitempty"`
	Payload     []byte       `json:"payload"`
}

type RecvAckParams struct {
	Header     Header `json:"header,omitempty"`
	MessageID  string `json:"messageId"`
	MessageSeq uint64 `json:"messageSeq"`
}

type SubscribeParams struct {
	SubNo       string `json:"subNo"`
	ChannelID   string `json:"channelId"`
	ChannelType int    `json:"channelType"`
	Param       string `json:"param,omitempty"`
}

type UnsubscribeParams struct {
	SubNo       string `json:"subNo"`
	ChannelID   string `json:"channelId"`
	ChannelType int    `json:"channelType"`
}

type PingParams struct {
	// Empty struct
}

type DisconnectParams struct {
	ReasonCode ReasonCodeEnum `json:"reasonCode"`
	Reason     string         `json:"reason,omitempty"`
}

// --- Specific Result Payloads ---

type ConnectResult struct {
	Header        *Header        `json:"header,omitempty"`
	ServerVersion int            `json:"serverVersion,omitempty"`
	ServerKey     string         `json:"serverKey,omitempty"`
	Salt          string         `json:"salt,omitempty"`
	TimeDiff      int64          `json:"timeDiff,omitempty"`
	ReasonCode    ReasonCodeEnum `json:"reasonCode"`
	NodeID        uint64         `json:"nodeId"`
}

type SendResult struct {
	Header     *Header        `json:"header,omitempty"`
	MessageID  string         `json:"messageId"`
	MessageSeq uint64         `json:"messageSeq"`
	ReasonCode ReasonCodeEnum `json:"reasonCode"`
}

type SubscriptionResult struct {
	Header      *Header        `json:"header,omitempty"`
	SubNo       string         `json:"subNo"`
	ChannelID   string         `json:"channelId"`
	ChannelType int            `json:"channelType"`
	Action      ActionEnum     `json:"action"`
	ReasonCode  ReasonCodeEnum `json:"reasonCode"`
}

// Pong result is null according to schema, handled by BaseResponse structure

// --- Specific Notification Payloads (Params) ---

type RecvNotificationParams struct {
	Header      *Header        `json:"header,omitempty"`
	Setting     *SettingFlags  `json:"setting,omitempty"`
	MsgKey      string         `json:"msgKey,omitempty"`
	Expire      uint32         `json:"expire,omitempty"`
	MessageID   string         `json:"messageId"`
	MessageSeq  uint64         `json:"messageSeq"`
	ClientMsgNo string         `json:"clientMsgNo,omitempty"`
	StreamNo    string         `json:"streamNo,omitempty"`
	StreamID    string         `json:"streamId,omitempty"`
	StreamFlag  StreamFlagEnum `json:"streamFlag,omitempty"`
	Timestamp   int32          `json:"timestamp"`
	ChannelID   string         `json:"channelId"`
	ChannelType int            `json:"channelType"`
	Topic       string         `json:"topic,omitempty"`
	FromUID     string         `json:"fromUid"`
	Payload     []byte         `json:"payload"`
}

// DisconnectNotificationParams are same as DisconnectParams
type DisconnectNotificationParams DisconnectParams

// EventNotificationParams represents the parameters for event notifications
type EventNotificationParams struct {
	Header    *Header `json:"header,omitempty"`
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Timestamp int64   `json:"timestamp"`
	Data      string  `json:"data"`
}

// --- Full Request/Response/Notification Structures ---
// These combine the base and the specific params/result for easier encoding.

// Requests
type ConnectRequest struct {
	BaseRequest
	Params ConnectParams `json:"params"`
}

type SendRequest struct {
	BaseRequest
	Params SendParams `json:"params"`
}

type RecvAckNotification struct {
	BaseNotification
	Params RecvAckParams `json:"params"`
}

type SubscribeRequest struct {
	BaseRequest
	Params SubscribeParams `json:"params"`
}

type UnsubscribeRequest struct {
	BaseRequest
	Params UnsubscribeParams `json:"params"`
}

type PingRequest struct {
	BaseRequest
	// Use pointer for Params to allow omitting the field entirely when nil
	Params *PingParams `json:"params,omitempty"`
}

type DisconnectRequest struct {
	BaseRequest
	Params DisconnectParams `json:"params"`
}

// Responses
type ConnectResponse struct {
	BaseResponse
	Result *ConnectResult `json:"result,omitempty"`
	Error  *ErrorObject   `json:"error,omitempty"`
}

type SendResponse struct {
	BaseResponse
	Result *SendResult  `json:"result,omitempty"`
	Error  *ErrorObject `json:"error,omitempty"`
}

type SubscriptionResponse struct {
	BaseResponse
	Result *SubscriptionResult `json:"result,omitempty"`
	Error  *ErrorObject        `json:"error,omitempty"`
}

type PongResponse struct {
	BaseResponse
	Result json.RawMessage `json:"result,omitempty"`
}

type RecvAckResponse struct {
	BaseResponse
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorObject    `json:"error,omitempty"`
}

// Disconnect Request does not seem to have a defined Response structure either.
// Assuming BaseResponse is sufficient.

// Notifications
type RecvNotification struct {
	BaseNotification
	Params RecvNotificationParams `json:"params"`
}

type DisconnectNotification struct {
	BaseNotification
	Params DisconnectNotificationParams `json:"params"`
}

type EventNotification struct {
	BaseNotification
	Params EventNotificationParams `json:"params"`
}

// --- Conversion Methods ---

// toProtoInternal converts JSON-RPC Header to wkpacket.Framer (internal helper)
func (h Header) toProtoInternal() *wkpacket.Framer {
	protoHeader := &wkpacket.Framer{}
	// Assuming direct mapping for boolean flags.
	protoHeader.NoPersist = h.NoPersist
	protoHeader.RedDot = h.RedDot
	protoHeader.SyncOnce = h.SyncOnce
	protoHeader.DUP = h.Dup
	protoHeader.End = h.End
	return protoHeader
}

// ToProto converts JSON-RPC SettingFlags to wkpacket.Setting
func (sf SettingFlags) ToProto() wkpacket.Setting {
	var setting wkpacket.Setting = 0
	if sf.Receipt {
		setting |= wkpacket.SettingReceiptEnabled
	}
	if sf.Signal {
		setting |= wkpacket.SettingSignal
	}
	if sf.Stream {
		setting |= wkpacket.SettingStream
	}
	if sf.Topic {
		setting |= wkpacket.SettingTopic
	}
	return setting
}

// ToProto converts the Header value to its proto representation.
func (h Header) ToProto() *wkpacket.Framer {
	return h.toProtoInternal()
}

// --- Specific Payload Conversions ---

// ToProto converts JSON-RPC ConnectParams to wkpacket.ConnectPacket.
func (p ConnectParams) ToProto() *wkpacket.ConnectPacket {

	var version uint8 = uint8(p.Version)
	if p.Version == 0 {
		version = wkpacket.LatestVersion
	}

	req := &wkpacket.ConnectPacket{
		Framer:          headerToFramer(p.Header),
		Version:         version,
		ClientKey:       p.ClientKey,
		DeviceID:        p.DeviceID,
		DeviceFlag:      wkpacket.DeviceFlag(p.DeviceFlag),
		ClientTimestamp: p.ClientTimestamp,
		UID:             p.UID,
		Token:           p.Token,
	}
	return req
}

// FromProtoConnectAck converts wkpacket.ConnackPacket to JSON-RPC ConnectResult
func FromProtoConnectAck(ack *wkpacket.ConnackPacket) *ConnectResult {
	if ack == nil {
		return nil
	}
	res := &ConnectResult{
		Header:        fromProtoHeader(ack.Framer),
		ServerVersion: int(ack.ServerVersion),
		ServerKey:     ack.ServerKey,
		Salt:          ack.Salt,
		TimeDiff:      ack.TimeDiff,
		ReasonCode:    ReasonCodeEnum(ack.ReasonCode),
		NodeID:        ack.NodeId,
	}
	return res
}

// ToProto converts JSON-RPC SendParams to wkpacket.SendPacket.
func (p SendParams) ToProto() *wkpacket.SendPacket {
	req := &wkpacket.SendPacket{
		Framer:      headerToFramer(p.Header),
		Setting:     p.Setting.ToProto(),
		ClientMsgNo: p.ClientMsgNo,
		ChannelID:   p.ChannelID,
		ChannelType: uint8(p.ChannelType),
		Payload:     p.Payload,
		MsgKey:      p.MsgKey,
		Expire:      p.Expire,
		StreamNo:    p.StreamNo,
		Topic:       p.Topic,
	}
	return req
}

// FromProtoSendAck converts wkpacket.SendackPacket to JSON-RPC SendResult
func FromProtoSendAck(ack *wkpacket.SendackPacket) *SendResult {
	if ack == nil {
		return nil
	}
	messageID := strconv.FormatInt(ack.MessageID, 10)
	res := &SendResult{
		Header:     fromProtoHeader(ack.Framer),
		MessageID:  messageID,
		MessageSeq: ack.MessageSeq,
		ReasonCode: ReasonCodeEnum(ack.ReasonCode),
	}
	return res
}

// ToProto converts JSON-RPC RecvAckParams to wkpacket.RecvackPacket.
func (p RecvAckParams) ToProto() *wkpacket.RecvackPacket {
	msgID, _ := strconv.ParseInt(p.MessageID, 10, 64)
	req := &wkpacket.RecvackPacket{
		Framer:     headerToFramer(p.Header),
		MessageID:  msgID,
		MessageSeq: p.MessageSeq,
	}
	return req
}

// FromProtoRecvPacket converts wkpacket.RecvPacket to JSON-RPC RecvNotificationParams
func FromProtoRecvPacket(pkt *wkpacket.RecvPacket) RecvNotificationParams {

	params := RecvNotificationParams{
		Header:      fromProtoHeader(pkt.Framer),
		Setting:     fromProtoSetting(pkt.Setting),
		MsgKey:      pkt.MsgKey,
		Expire:      pkt.Expire,
		MessageID:   strconv.FormatInt(pkt.MessageID, 10),
		MessageSeq:  pkt.MessageSeq,
		ClientMsgNo: pkt.ClientMsgNo,
		StreamNo:    pkt.StreamNo,
		StreamID:    strconv.FormatUint(pkt.StreamId, 10),
		StreamFlag:  StreamFlagEnum(pkt.StreamFlag),
		Timestamp:   pkt.Timestamp,
		ChannelID:   pkt.ChannelID,
		ChannelType: int(pkt.ChannelType),
		Topic:       pkt.Topic,
		FromUID:     pkt.FromUID,
		Payload:     pkt.Payload,
	}
	return params
}

// ToProto converts JSON-RPC SubscribeParams to wkpacket.SubPacket.
func (p SubscribeParams) ToProto() *wkpacket.SubPacket {
	req := &wkpacket.SubPacket{
		SubNo:       p.SubNo,
		ChannelID:   p.ChannelID,
		ChannelType: uint8(p.ChannelType),
		Param:       p.Param,
	}
	return req
}

// ToProto converts JSON-RPC DisconnectParams to wkpacket.DisconnectPacket.
func (p DisconnectParams) ToProto() *wkpacket.DisconnectPacket {
	pkt := &wkpacket.DisconnectPacket{
		ReasonCode: wkpacket.ReasonCode(p.ReasonCode),
		Reason:     p.Reason,
	}
	return pkt
}

// FromProtoDisconnectPacket converts wkpacket.DisconnectPacket to JSON-RPC DisconnectNotificationParams
func FromProtoDisconnectPacket(pkt *wkpacket.DisconnectPacket) DisconnectNotificationParams {
	if pkt == nil {
		return DisconnectNotificationParams{}
	}
	params := DisconnectNotificationParams{
		ReasonCode: ReasonCodeEnum(pkt.ReasonCode),
		Reason:     pkt.Reason,
	}
	return params
}

// ToProto converts PingParams to wkpacket.PingPacket.
func (p PingParams) ToProto() *wkpacket.PingPacket {
	return &wkpacket.PingPacket{}
}

// FromProtoPongPacket converts wkpacket.PongPacket to PongResponse fields (mostly base)
// Pong response usually just confirms the ID, result is often null.
func FromProtoPongPacket(pkt *wkpacket.PongPacket) {
	if pkt == nil {
		// return appropriate representation of error or empty/null result
	}
	// Pong has no specific result fields typically.
	// The BaseResponse handles ID and potential errors.
	// Result field in PongResponse is json.RawMessage, likely set to `null`.
}

// --- Reverse Helper Functions (Proto -> JSON-RPC) ---

// fromProtoHeader converts wkpacket.Framer to JSON-RPC Header
func fromProtoHeader(protoHeader wkpacket.Framer) *Header {
	if !protoHeader.NoPersist && !protoHeader.RedDot && !protoHeader.SyncOnce && !protoHeader.DUP && !protoHeader.End {
		return nil
	}
	return &Header{
		NoPersist: protoHeader.NoPersist,
		RedDot:    protoHeader.RedDot,
		SyncOnce:  protoHeader.SyncOnce,
		Dup:       protoHeader.DUP,
		End:       protoHeader.End,
	}
}

func headerToFramer(header Header) wkpacket.Framer {
	return wkpacket.Framer{
		NoPersist: header.NoPersist,
		RedDot:    header.RedDot,
		SyncOnce:  header.SyncOnce,
		DUP:       header.Dup,
		End:       header.End,
	}
}

// fromProtoSetting converts wkpacket.Setting to JSON-RPC SettingFlags
func fromProtoSetting(setting wkpacket.Setting) *SettingFlags {

	if setting == 0 {
		return nil
	}

	flags := &SettingFlags{}
	flags.Receipt = (setting & wkpacket.SettingReceiptEnabled) != 0
	flags.Signal = (setting & wkpacket.SettingSignal) != 0
	flags.Stream = (setting & wkpacket.SettingStream) != 0
	flags.Topic = (setting & wkpacket.SettingTopic) != 0
	return flags
}

// --- Helper function to create standard requests easily ---
// Might need adjustments if wkpacket types are used directly or interfaces change
func NewRequest(method string, id string, params interface{}) interface{} {
	req := BaseRequest{
		Jsonrpc: "2.0",
		Method:  method,
		ID:      id,
	}
	switch p := params.(type) {
	case ConnectParams:
		return ConnectRequest{BaseRequest: req, Params: p}
	case SendParams:
		return SendRequest{BaseRequest: req, Params: p}
	case SubscribeParams:
		return SubscribeRequest{BaseRequest: req, Params: p}
	case UnsubscribeParams:
		return UnsubscribeRequest{BaseRequest: req, Params: p}
	case DisconnectParams:
		return DisconnectRequest{BaseRequest: req, Params: p}
	case PingParams:
		// If PingParams (value) is passed, wrap it in a pointer for PingRequest
		pVal := params.(PingParams)
		return PingRequest{BaseRequest: req, Params: &pVal}
	case *PingParams:
		// If *PingParams (pointer) is passed, use it directly
		return PingRequest{BaseRequest: req, Params: p}
	case nil:
		// If nil is passed specifically for ping, create request with nil Params
		if method == "ping" {
			return PingRequest{BaseRequest: req, Params: nil}
		}
		// Handle nil for other types if necessary, or fall through
		fmt.Printf("Warning: NewRequest called with nil params for non-ping method %s\n", method)
	default:
		fmt.Printf("Warning: NewRequest called with unhandled params type: %T for method %s\n", params, method)
		// Returning BaseRequest is likely incorrect
	}
	// Fallback for default and nil cases (if not handled above)
	return req
}

// Helper function/type for generic response decoding later
type GenericResponse struct {
	BaseResponse
	Result json.RawMessage `json:"result,omitempty"`
}

func NewGenericResponse(id string, result json.RawMessage) GenericResponse {
	return GenericResponse{
		BaseResponse: BaseResponse{
			Jsonrpc: jsonRPCVersion,
			ID:      id,
		},
		Result: result,
	}
}

func NewGenericResponseWithErr(id string, err *ErrorObject) GenericResponse {
	return GenericResponse{
		BaseResponse: BaseResponse{
			Jsonrpc: jsonRPCVersion,
			ID:      id,
			Error:   err,
		},
	}
}

// Add conversions for full Request/Response types if needed, e.g.:

// ToProto converts the full ConnectRequest to its proto representation
func (r ConnectRequest) ToProto() *wkpacket.ConnectPacket {
	return r.Params.ToProto()
}

// ToProto converts the full SendRequest to its proto representation
func (r SendRequest) ToProto() (*wkpacket.SendPacket, error) {
	payloadBytes := r.Params.Payload
	pkt := &wkpacket.SendPacket{
		Framer:      headerToFramer(r.Params.Header),
		Setting:     r.Params.Setting.ToProto(),
		ClientMsgNo: r.Params.ClientMsgNo,
		ChannelID:   r.Params.ChannelID,
		ChannelType: uint8(r.Params.ChannelType),
		Payload:     payloadBytes,
		MsgKey:      r.Params.MsgKey,
		Expire:      r.Params.Expire,
		StreamNo:    r.Params.StreamNo,
		Topic:       r.Params.Topic,
	}
	return pkt, nil
}

// Example: FromProto... for full response
func FromProtoConnackNotification(id string, ack *wkpacket.ConnackPacket) *ConnectResponse {
	resp := &ConnectResponse{
		BaseResponse: BaseResponse{
			Jsonrpc: jsonRPCVersion,
			ID:      id,
		},
	}
	if ack.ReasonCode == wkpacket.ReasonSuccess {
		resp.Result = FromProtoConnectAck(ack)
	} else {
		resp.Error = &ErrorObject{
			Code:    int(ack.ReasonCode),
			Message: wkpacket.ReasonCode(ack.ReasonCode).String(),
		}
	}
	return resp
}

// Example: FromProto... for full notification
func FromProtoRecvNotification(pkt *wkpacket.RecvPacket) RecvNotification {

	return RecvNotification{
		BaseNotification: BaseNotification{
			Jsonrpc: "2.0",
			Method:  MethodRecv,
		},
		Params: FromProtoRecvPacket(pkt),
	}
}

// NewEventNotification creates a new EventNotification
func NewEventNotification(id string, eventType string, timestamp int64, data string, header *Header) EventNotification {
	return EventNotification{
		BaseNotification: BaseNotification{
			Jsonrpc: jsonRPCVersion,
			Method:  MethodEvent,
		},
		Params: EventNotificationParams{
			Header:    header,
			ID:        id,
			Type:      eventType,
			Timestamp: timestamp,
			Data:      data,
		},
	}
}

func FromProtoEventNotification(eventPacket *wkpacket.EventPacket) EventNotification {
	return EventNotification{
		BaseNotification: BaseNotification{
			Jsonrpc: "2.0",
			Method:  MethodEvent,
		},
		Params: EventNotificationParams{
			Header:    fromProtoHeader(eventPacket.Framer),
			ID:        eventPacket.Id,
			Type:      eventPacket.Type,
			Timestamp: eventPacket.Timestamp,
			Data:      string(eventPacket.Data),
		},
	}
}

// Similarly for DisconnectNotification...
