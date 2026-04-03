package online

import (
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

var ErrInvalidConnection = errors.New("runtime/online: invalid connection")

type Conn struct {
	SessionID   uint64
	UID         string
	DeviceFlag  wkframe.DeviceFlag
	DeviceLevel wkframe.DeviceLevel
	Listener    string
	ConnectedAt time.Time
	Session     session.Session
}

type OnlineConn = Conn

type Registry interface {
	Register(conn OnlineConn) error
	Unregister(sessionID uint64)
	Connection(sessionID uint64) (OnlineConn, bool)
	ConnectionsByUID(uid string) []OnlineConn
}

type Delivery interface {
	Deliver(recipients []OnlineConn, frame wkframe.Frame) error
}
