package online

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestRegistryRegisterRejectsInvalidConnection(t *testing.T) {
	reg := NewRegistry()

	err := reg.Register(OnlineConn{})
	require.ErrorIs(t, err, ErrInvalidConnection)
}

func TestRegistryRegisterLookupAndUnregister(t *testing.T) {
	reg := NewRegistry()
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	conn1 := OnlineConn{
		SessionID:   1,
		UID:         "u1",
		DeviceFlag:  wkpacket.APP,
		DeviceLevel: wkpacket.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: fixedNow,
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}
	conn2 := OnlineConn{
		SessionID:   2,
		UID:         "u1",
		DeviceFlag:  wkpacket.WEB,
		DeviceLevel: wkpacket.DeviceLevelSlave,
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
	require.Contains(t, deviceFlagsOf(connections), wkpacket.APP)
	require.Contains(t, deviceFlagsOf(connections), wkpacket.DeviceFlag(wkpacket.WEB))

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
		DeviceFlag:  wkpacket.APP,
		DeviceLevel: wkpacket.DeviceLevelMaster,
		Listener:    "tcp",
		ConnectedAt: fixedNow,
		Session:     session.New(session.Config{ID: 1, Listener: "tcp"}),
	}
	second := OnlineConn{
		SessionID:   1,
		UID:         "u2",
		DeviceFlag:  wkpacket.WEB,
		DeviceLevel: wkpacket.DeviceLevelSlave,
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
		DeviceFlag:  wkpacket.APP,
		DeviceLevel: wkpacket.DeviceLevelMaster,
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

func deviceFlagsOf(conns []OnlineConn) []wkpacket.DeviceFlag {
	flags := make([]wkpacket.DeviceFlag, 0, len(conns))
	for _, conn := range conns {
		flags = append(flags, conn.DeviceFlag)
	}
	return flags
}
