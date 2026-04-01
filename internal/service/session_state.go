package service

import (
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type SessionMeta struct {
	SessionID   uint64
	UID         string
	DeviceFlag  wkpacket.DeviceFlag
	DeviceLevel wkpacket.DeviceLevel
	Listener    string
	ConnectedAt time.Time
	Session     session.Session
}

func sessionMetaFromContext(ctx *gateway.Context, now time.Time) (SessionMeta, error) {
	if ctx == nil || ctx.Session == nil {
		return SessionMeta{}, ErrUnauthenticatedSession
	}

	uid, _ := ctx.Session.Value(gateway.SessionValueUID).(string)
	if uid == "" {
		return SessionMeta{}, ErrUnauthenticatedSession
	}

	meta := SessionMeta{
		SessionID:   ctx.Session.ID(),
		UID:         uid,
		DeviceFlag:  deviceFlagFromValue(ctx.Session.Value(gateway.SessionValueDeviceFlag)),
		DeviceLevel: deviceLevelFromValue(ctx.Session.Value(gateway.SessionValueDeviceLevel)),
		Listener:    ctx.Listener,
		ConnectedAt: now,
		Session:     ctx.Session,
	}

	if meta.Listener == "" {
		meta.Listener = ctx.Session.Listener()
	}

	return meta, nil
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
