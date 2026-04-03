package gateway

import (
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
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

func deviceFlagFromValue(value any) wkframe.DeviceFlag {
	switch v := value.(type) {
	case wkframe.DeviceFlag:
		return v
	case uint8:
		return wkframe.DeviceFlag(v)
	case int:
		return wkframe.DeviceFlag(v)
	case int32:
		return wkframe.DeviceFlag(v)
	case int64:
		return wkframe.DeviceFlag(v)
	default:
		return 0
	}
}

func deviceLevelFromValue(value any) wkframe.DeviceLevel {
	switch v := value.(type) {
	case wkframe.DeviceLevel:
		return v
	case uint8:
		return wkframe.DeviceLevel(v)
	case int:
		return wkframe.DeviceLevel(v)
	case int32:
		return wkframe.DeviceLevel(v)
	case int64:
		return wkframe.DeviceLevel(v)
	default:
		return 0
	}
}
