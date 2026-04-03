package gateway

import (
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
)

func onlineConnFromContext(ctx *coregateway.Context, now time.Time) (online.OnlineConn, error) {
	if ctx == nil || ctx.Session == nil {
		return online.OnlineConn{}, ErrUnauthenticatedSession
	}

	uid, _ := ctx.Session.Value(coregateway.SessionValueUID).(string)
	if uid == "" {
		return online.OnlineConn{}, ErrUnauthenticatedSession
	}

	conn := online.OnlineConn{
		SessionID:   ctx.Session.ID(),
		UID:         uid,
		DeviceFlag:  deviceFlagFromValue(ctx.Session.Value(coregateway.SessionValueDeviceFlag)),
		DeviceLevel: deviceLevelFromValue(ctx.Session.Value(coregateway.SessionValueDeviceLevel)),
		Listener:    ctx.Listener,
		ConnectedAt: now,
		Session:     ctx.Session,
	}
	if conn.Listener == "" {
		conn.Listener = ctx.Session.Listener()
	}

	return conn, nil
}

func deviceFlagFromValue(value any) wkpacket.DeviceFlag {
	switch v := value.(type) {
	case wkpacket.DeviceFlag:
		return v
	case uint8:
		return wkpacket.DeviceFlag(v)
	case int:
		return wkpacket.DeviceFlag(v)
	case int32:
		return wkpacket.DeviceFlag(v)
	case int64:
		return wkpacket.DeviceFlag(v)
	default:
		return 0
	}
}

func deviceLevelFromValue(value any) wkpacket.DeviceLevel {
	switch v := value.(type) {
	case wkpacket.DeviceLevel:
		return v
	case uint8:
		return wkpacket.DeviceLevel(v)
	case int:
		return wkpacket.DeviceLevel(v)
	case int32:
		return wkpacket.DeviceLevel(v)
	case int64:
		return wkpacket.DeviceLevel(v)
	default:
		return 0
	}
}
