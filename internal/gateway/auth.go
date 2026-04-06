package gateway

import (
	"time"

	gatewaytypes "github.com/WuKongIM/WuKongIM/internal/gateway/types"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type Authenticator = gatewaytypes.Authenticator
type AuthenticatorFunc = gatewaytypes.AuthenticatorFunc
type AuthResult = gatewaytypes.AuthResult

const (
	SessionValueUID             = gatewaytypes.SessionValueUID
	SessionValueDeviceID        = gatewaytypes.SessionValueDeviceID
	SessionValueDeviceFlag      = gatewaytypes.SessionValueDeviceFlag
	SessionValueDeviceLevel     = gatewaytypes.SessionValueDeviceLevel
	SessionValueProtocolVersion = gatewaytypes.SessionValueProtocolVersion
)

type WKProtoAuthOptions struct {
	TokenAuthOn bool
	NodeID      uint64
	Now         func() time.Time

	IsVisitor   func(uid string) bool
	VerifyToken func(uid string, deviceFlag wkframe.DeviceFlag, token string) (wkframe.DeviceLevel, error)
	IsBanned    func(uid string) (bool, error)
}

func NewWKProtoAuthenticator(opts WKProtoAuthOptions) Authenticator {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return AuthenticatorFunc(func(_ *Context, connect *wkframe.ConnectPacket) (*AuthResult, error) {
		if connect == nil {
			return &AuthResult{
				Connack: &wkframe.ConnackPacket{ReasonCode: wkframe.ReasonAuthFail},
			}, nil
		}

		deviceLevel := wkframe.DeviceLevelSlave
		if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {
			if connect.Token == "" || opts.VerifyToken == nil {
				return &AuthResult{
					Connack: &wkframe.ConnackPacket{ReasonCode: wkframe.ReasonAuthFail},
				}, nil
			}

			level, err := opts.VerifyToken(connect.UID, connect.DeviceFlag, connect.Token)
			if err != nil {
				return &AuthResult{
					Connack: &wkframe.ConnackPacket{ReasonCode: wkframe.ReasonAuthFail},
				}, nil
			}
			deviceLevel = level
		}

		if opts.IsBanned != nil {
			banned, err := opts.IsBanned(connect.UID)
			if err != nil || banned {
				reason := wkframe.ReasonBan
				if err != nil {
					reason = wkframe.ReasonAuthFail
				}
				return &AuthResult{
					Connack: &wkframe.ConnackPacket{ReasonCode: reason},
				}, nil
			}
		}

		serverVersion := connect.Version
		if serverVersion == 0 || serverVersion > wkframe.LatestVersion {
			serverVersion = wkframe.LatestVersion
		}

		connack := &wkframe.ConnackPacket{
			ReasonCode:    wkframe.ReasonSuccess,
			TimeDiff:      nowFn().UnixMilli() - connect.ClientTimestamp,
			ServerVersion: serverVersion,
			NodeId:        opts.NodeID,
		}
		connack.HasServerVersion = connect.Version > 3

		return &AuthResult{
			Connack: connack,
			SessionValues: map[string]any{
				SessionValueUID:             connect.UID,
				SessionValueDeviceID:        connect.DeviceID,
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
