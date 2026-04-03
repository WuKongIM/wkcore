package gateway

import (
	"time"

	gatewaytypes "github.com/WuKongIM/WuKongIM/internal/gateway/types"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
)

type Authenticator = gatewaytypes.Authenticator
type AuthenticatorFunc = gatewaytypes.AuthenticatorFunc
type AuthResult = gatewaytypes.AuthResult

const (
	SessionValueUID             = gatewaytypes.SessionValueUID
	SessionValueDeviceFlag      = gatewaytypes.SessionValueDeviceFlag
	SessionValueDeviceLevel     = gatewaytypes.SessionValueDeviceLevel
	SessionValueProtocolVersion = gatewaytypes.SessionValueProtocolVersion
)

type WKProtoAuthOptions struct {
	TokenAuthOn bool
	NodeID      uint64
	Now         func() time.Time

	IsVisitor   func(uid string) bool
	VerifyToken func(uid string, deviceFlag wkpacket.DeviceFlag, token string) (wkpacket.DeviceLevel, error)
	IsBanned    func(uid string) (bool, error)
}

func NewWKProtoAuthenticator(opts WKProtoAuthOptions) Authenticator {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return AuthenticatorFunc(func(_ *Context, connect *wkpacket.ConnectPacket) (*AuthResult, error) {
		if connect == nil {
			return &AuthResult{
				Connack: &wkpacket.ConnackPacket{ReasonCode: wkpacket.ReasonAuthFail},
			}, nil
		}

		deviceLevel := wkpacket.DeviceLevelSlave
		if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {
			if connect.Token == "" || opts.VerifyToken == nil {
				return &AuthResult{
					Connack: &wkpacket.ConnackPacket{ReasonCode: wkpacket.ReasonAuthFail},
				}, nil
			}

			level, err := opts.VerifyToken(connect.UID, connect.DeviceFlag, connect.Token)
			if err != nil {
				return &AuthResult{
					Connack: &wkpacket.ConnackPacket{ReasonCode: wkpacket.ReasonAuthFail},
				}, nil
			}
			deviceLevel = level
		}

		if opts.IsBanned != nil {
			banned, err := opts.IsBanned(connect.UID)
			if err != nil || banned {
				reason := wkpacket.ReasonBan
				if err != nil {
					reason = wkpacket.ReasonAuthFail
				}
				return &AuthResult{
					Connack: &wkpacket.ConnackPacket{ReasonCode: reason},
				}, nil
			}
		}

		serverVersion := connect.Version
		if serverVersion == 0 || serverVersion > wkpacket.LatestVersion {
			serverVersion = wkpacket.LatestVersion
		}

		connack := &wkpacket.ConnackPacket{
			ReasonCode:    wkpacket.ReasonSuccess,
			TimeDiff:      nowFn().UnixMilli() - connect.ClientTimestamp,
			ServerVersion: serverVersion,
			NodeId:        opts.NodeID,
		}
		connack.HasServerVersion = connect.Version > 3

		return &AuthResult{
			Connack: connack,
			SessionValues: map[string]any{
				SessionValueUID:             connect.UID,
				SessionValueDeviceFlag:      connect.DeviceFlag,
				SessionValueDeviceLevel:     deviceLevel,
				SessionValueProtocolVersion: serverVersion,
			},
		}, nil
	})
}

func isVisitor(fn func(uid string) bool, uid string) bool {
	if fn == nil {
		return false
	}
	return fn(uid)
}
