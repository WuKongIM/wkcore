package online

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestRegistryRegisterRejectsInvalidConnection(t *testing.T) {
	reg := NewRegistry()

	err := reg.Register(OnlineConn{})
	require.ErrorIs(t, err, ErrInvalidConnection)
}

func TestRegistryRegisterStoresDeviceIDGroupAndActiveState(t *testing.T) {
	reg := NewRegistry()
	conn := OnlineConn{
		SessionID:   11,
		UID:         "u1",
		DeviceID:    "d1",
		GroupID:     7,
		State:       LocalRouteStateActive,
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
		Session:     session.New(session.Config{ID: 11, Listener: "tcp"}),
	}

	require.NoError(t, reg.Register(conn))

	got, ok := reg.Connection(11)
	require.True(t, ok)
	require.Equal(t, conn, got)

	connections := reg.ConnectionsByUID("u1")
	require.Len(t, connections, 1)
	require.Equal(t, conn, connections[0])

	groups := reg.ActiveGroups()
	require.Len(t, groups, 1)
	require.Equal(t, uint64(7), groups[0].GroupID)
	require.Equal(t, 1, groups[0].Count)
	require.NotZero(t, groups[0].Digest)
}

func TestRegistryRegisterLookupAndUnregister(t *testing.T) {
	reg := NewRegistry()
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	conn1 := OnlineConn{
		SessionID:   1,
		UID:         "u1",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: fixedNow,
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}
	conn2 := OnlineConn{
		SessionID:   2,
		UID:         "u1",
		DeviceFlag:  wkframe.WEB,
		DeviceLevel: wkframe.DeviceLevelSlave,
		Listener:    "ws",
		ConnectedAt: fixedNow.Add(time.Minute),
		Session:     session.New(session.Config{ID: 2, Listener: "ws"}),
	}

	require.NoError(t, reg.Register(conn1))
	require.NoError(t, reg.Register(conn2))

	got, ok := reg.Connection(1)
	require.True(t, ok)
	require.Equal(t, conn1, got)

	connections := reg.ConnectionsByUID("u1")
	require.Len(t, connections, 2)
	require.ElementsMatch(t, []string{"tcp", "ws"}, listenersOf(connections))
	require.Contains(t, connectionIDsOf(connections), uint64(1))
	require.Contains(t, connectionIDsOf(connections), uint64(2))
	require.Contains(t, deviceFlagsOf(connections), wkframe.APP)
	require.Contains(t, deviceFlagsOf(connections), wkframe.DeviceFlag(wkframe.WEB))

	connections[0].Listener = "mutated"
	connectionsAgain := reg.ConnectionsByUID("u1")
	require.ElementsMatch(t, []string{"tcp", "ws"}, listenersOf(connectionsAgain))

	reg.Unregister(1)
	_, ok = reg.Connection(1)
	require.False(t, ok)
	require.Len(t, reg.ConnectionsByUID("u1"), 1)
}

func TestRegistryRegisterOverwritesSessionAndCleansOldUIDBucket(t *testing.T) {
	reg := NewRegistry()
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	first := OnlineConn{
		SessionID:   1,
		UID:         "u1",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: fixedNow,
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}
	second := OnlineConn{
		SessionID:   1,
		UID:         "u2",
		DeviceFlag:  wkframe.WEB,
		DeviceLevel: wkframe.DeviceLevelSlave,
		Listener:    "tcp",
		ConnectedAt: fixedNow.Add(time.Minute),
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}

	require.NoError(t, reg.Register(first))
	require.NoError(t, reg.Register(second))

	got, ok := reg.Connection(1)
	require.True(t, ok)
	require.Equal(t, second, got)
	require.Empty(t, reg.ConnectionsByUID("u1"))
	require.Len(t, reg.ConnectionsByUID("u2"), 1)
	require.Equal(t, second, reg.ConnectionsByUID("u2")[0])
}

func TestRegistryUnregisterIsIdempotent(t *testing.T) {
	reg := NewRegistry()
	conn := OnlineConn{
		SessionID:   1,
		UID:         "u1",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}

	require.NoError(t, reg.Register(conn))

	reg.Unregister(1)
	reg.Unregister(1)

	_, ok := reg.Connection(1)
	require.False(t, ok)
	require.Empty(t, reg.ConnectionsByUID("u1"))
}

func TestRegistryMarkClosingRemovesRouteFromUIDDeliveryAndBucketDigest(t *testing.T) {
	reg := NewRegistry()
	require.NoError(t, reg.Register(OnlineConn{
		SessionID:   11,
		UID:         "u1",
		DeviceID:    "d1",
		GroupID:     1,
		State:       LocalRouteStateActive,
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Session:     session.New(session.Config{ID: 11, Listener: "tcp"}),
	}))

	before := reg.ActiveGroups()
	conn, ok := reg.MarkClosing(11)
	require.True(t, ok)
	require.Equal(t, LocalRouteStateClosing, conn.State)
	require.Empty(t, reg.ConnectionsByUID("u1"))
	require.NotEmpty(t, before)
	after := reg.ActiveGroups()
	require.Len(t, after, 1)
	require.Equal(t, uint64(1), after[0].GroupID)
	require.Zero(t, after[0].Count)
	require.NotEqual(t, before[0].Digest, after[0].Digest)
}

func TestRegistryActiveConnectionsByGroupReturnsOnlyActiveRoutes(t *testing.T) {
	reg := NewRegistry()

	active := OnlineConn{
		SessionID:   11,
		UID:         "u1",
		DeviceID:    "d1",
		GroupID:     3,
		State:       LocalRouteStateActive,
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Session:     session.New(session.Config{ID: 11, Listener: "tcp"}),
	}
	closing := OnlineConn{
		SessionID:   12,
		UID:         "u1",
		DeviceID:    "d2",
		GroupID:     3,
		State:       LocalRouteStateClosing,
		DeviceFlag:  wkframe.WEB,
		DeviceLevel: wkframe.DeviceLevelSlave,
		Session:     session.New(session.Config{ID: 12, Listener: "ws"}),
	}

	require.NoError(t, reg.Register(active))
	require.NoError(t, reg.Register(closing))

	conns := reg.ActiveConnectionsByGroup(3)
	require.Len(t, conns, 1)
	require.Equal(t, active, conns[0])
}

func listenersOf(conns []OnlineConn) []string {
	listeners := make([]string, 0, len(conns))
	for _, conn := range conns {
		listeners = append(listeners, conn.Listener)
	}
	return listeners
}

func connectionIDsOf(conns []OnlineConn) []uint64 {
	ids := make([]uint64, 0, len(conns))
	for _, conn := range conns {
		ids = append(ids, conn.SessionID)
	}
	return ids
}

func deviceFlagsOf(conns []OnlineConn) []wkframe.DeviceFlag {
	flags := make([]wkframe.DeviceFlag, 0, len(conns))
	for _, conn := range conns {
		flags = append(flags, conn.DeviceFlag)
	}
	return flags
}
