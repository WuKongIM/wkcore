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
	DeviceID    string
	DeviceFlag  wkframe.DeviceFlag
	DeviceLevel wkframe.DeviceLevel
	GroupID     uint64
	State       LocalRouteState
	Listener    string
	ConnectedAt time.Time
	Session     session.Session
}

type OnlineConn = Conn
type Session = session.Session

type LocalRouteState uint8

const (
	LocalRouteStateActive LocalRouteState = iota
	LocalRouteStateClosing
)

type GroupSnapshot struct {
	GroupID uint64
	Count   int
	Digest  uint64
}

type Registry interface {
	Register(conn OnlineConn) error
	Unregister(sessionID uint64)
	MarkClosing(sessionID uint64) (OnlineConn, bool)
	Connection(sessionID uint64) (OnlineConn, bool)
	ConnectionsByUID(uid string) []OnlineConn
	ActiveConnectionsByGroup(groupID uint64) []OnlineConn
	ActiveGroups() []GroupSnapshot
}

type Delivery interface {
	Deliver(recipients []OnlineConn, frame wkframe.Frame) error
}
