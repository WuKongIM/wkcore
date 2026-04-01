package online

import (
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

var ErrInvalidConnection = errors.New("runtime/online: invalid connection")

type Conn struct {
	SessionID   uint64
	UID         string
	DeviceFlag  wkpacket.DeviceFlag
	DeviceLevel wkpacket.DeviceLevel
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
	Deliver(recipients []OnlineConn, frame wkpacket.Frame) error
}
