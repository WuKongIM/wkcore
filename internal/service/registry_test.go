package service

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestSessionMetaFromContextReadsGatewayAuthValues(t *testing.T) {
	sess := session.New(session.Config{
		ID:       1,
		Listener: "tcp",
	})
	sess.SetValue(gateway.SessionValueUID, "u1")
	sess.SetValue(gateway.SessionValueDeviceFlag, wkpacket.APP)
	sess.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)

	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	meta, err := sessionMetaFromContext(&gateway.Context{
		Session:  sess,
		Listener: "tcp",
	}, fixedNow)
	require.NoError(t, err)
	require.Equal(t, uint64(1), meta.SessionID)
	require.Equal(t, "u1", meta.UID)
	require.Equal(t, wkpacket.APP, meta.DeviceFlag)
	require.Equal(t, wkpacket.DeviceLevelMaster, meta.DeviceLevel)
	require.Equal(t, "tcp", meta.Listener)
	require.Equal(t, fixedNow, meta.ConnectedAt)
	require.Same(t, sess, meta.Session)
}

func TestSessionMetaFromContextRequiresUID(t *testing.T) {
	sess := session.New(session.Config{
		ID:       1,
		Listener: "tcp",
	})

	_, err := sessionMetaFromContext(&gateway.Context{
		Session:  sess,
		Listener: "tcp",
	}, time.Now())
	require.ErrorIs(t, err, ErrUnauthenticatedSession)
}

func TestRegistryRegisterLookupAndUnregister(t *testing.T) {
	reg := NewRegistry()
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	sess1 := session.New(session.Config{ID: 1, Listener: "tcp"})
	sess1.SetValue(gateway.SessionValueUID, "u1")
	sess1.SetValue(gateway.SessionValueDeviceFlag, wkpacket.APP)
	sess1.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)
	meta1, err := sessionMetaFromContext(&gateway.Context{Session: sess1, Listener: "tcp"}, fixedNow)
	require.NoError(t, err)

	sess2 := session.New(session.Config{ID: 2, Listener: "ws"})
	sess2.SetValue(gateway.SessionValueUID, "u1")
	sess2.SetValue(gateway.SessionValueDeviceFlag, wkpacket.WEB)
	sess2.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelSlave)
	meta2, err := sessionMetaFromContext(&gateway.Context{Session: sess2, Listener: "ws"}, fixedNow.Add(time.Minute))
	require.NoError(t, err)

	require.NoError(t, reg.Register(meta1))
	require.NoError(t, reg.Register(meta2))

	got, ok := reg.Session(1)
	require.True(t, ok)
	require.Equal(t, meta1, got)

	sessions := reg.SessionsByUID("u1")
	require.Len(t, sessions, 2)
	require.ElementsMatch(t, []string{"tcp", "ws"}, listenersOf(sessions))
	require.Contains(t, sessionIDsOf(sessions), uint64(1))
	require.Contains(t, sessionIDsOf(sessions), uint64(2))
	require.Contains(t, deviceFlagsOf(sessions), wkpacket.APP)
	require.Contains(t, deviceFlagsOf(sessions), wkpacket.DeviceFlag(wkpacket.WEB))

	sessions[0].Listener = "mutated"
	sessionsAgain := reg.SessionsByUID("u1")
	require.ElementsMatch(t, []string{"tcp", "ws"}, listenersOf(sessionsAgain))

	reg.Unregister(1)
	_, ok = reg.Session(1)
	require.False(t, ok)
	require.Len(t, reg.SessionsByUID("u1"), 1)
}

func TestRegistryUnregisterIsIdempotent(t *testing.T) {
	reg := NewRegistry()
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	sess := session.New(session.Config{ID: 1, Listener: "tcp"})
	sess.SetValue(gateway.SessionValueUID, "u1")
	sess.SetValue(gateway.SessionValueDeviceFlag, wkpacket.APP)
	sess.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)
	meta, err := sessionMetaFromContext(&gateway.Context{Session: sess, Listener: "tcp"}, fixedNow)
	require.NoError(t, err)
	require.NoError(t, reg.Register(meta))

	reg.Unregister(1)
	reg.Unregister(1)

	_, ok := reg.Session(1)
	require.False(t, ok)
	require.Empty(t, reg.SessionsByUID("u1"))
}

func listenersOf(metas []SessionMeta) []string {
	listeners := make([]string, 0, len(metas))
	for _, meta := range metas {
		listeners = append(listeners, meta.Listener)
	}
	return listeners
}

func sessionIDsOf(metas []SessionMeta) []uint64 {
	ids := make([]uint64, 0, len(metas))
	for _, meta := range metas {
		ids = append(ids, meta.SessionID)
	}
	return ids
}

func deviceFlagsOf(metas []SessionMeta) []wkpacket.DeviceFlag {
	flags := make([]wkpacket.DeviceFlag, 0, len(metas))
	for _, meta := range metas {
		flags = append(flags, meta.DeviceFlag)
	}
	return flags
}
