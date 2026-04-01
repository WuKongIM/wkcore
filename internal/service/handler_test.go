package service

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestServiceOnSessionOpenRegistersAuthenticatedSession(t *testing.T) {
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	svc := New(Options{Now: func() time.Time { return fixedNow }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, svc.OnSessionOpen(ctx))

	sessions := svc.registry.SessionsByUID("u1")
	require.Len(t, sessions, 1)
	require.Equal(t, uint64(1), sessions[0].SessionID)
	require.Equal(t, fixedNow, sessions[0].ConnectedAt)
}

func TestServiceOnSessionCloseUnregistersSession(t *testing.T) {
	svc := New(Options{Now: func() time.Time { return time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC) }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, svc.OnSessionOpen(ctx))
	svc.OnSessionClose(ctx)

	require.Empty(t, svc.registry.SessionsByUID("u1"))
	_, ok := svc.registry.Session(1)
	require.False(t, ok)
}

func TestServiceOnSessionErrorDoesNotMutateRegistry(t *testing.T) {
	svc := New(Options{Now: func() time.Time { return time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC) }})
	ctx := newAuthedContext(t, 1, "u1")

	require.NoError(t, svc.OnSessionOpen(ctx))
	before := svc.registry.SessionsByUID("u1")

	svc.OnSessionError(ctx, errors.New("boom"))

	after := svc.registry.SessionsByUID("u1")
	require.Equal(t, before, after)
}

func TestNewServiceUsesDefaultNowWhenUnset(t *testing.T) {
	before := time.Now()
	svc := New(Options{})
	after := time.Now()

	require.NotNil(t, svc.opts.Now)

	got := svc.opts.Now()
	require.False(t, got.IsZero())
	require.True(t, !got.Before(before.Add(-time.Second)) && !got.After(after.Add(time.Second)))
}

func newAuthedContext(t *testing.T, sessionID uint64, uid string) *gateway.Context {
	t.Helper()

	sess := session.New(session.Config{
		ID:       sessionID,
		Listener: "tcp",
	})
	sess.SetValue(gateway.SessionValueUID, uid)
	sess.SetValue(gateway.SessionValueDeviceFlag, wkpacket.APP)
	sess.SetValue(gateway.SessionValueDeviceLevel, wkpacket.DeviceLevelMaster)

	return &gateway.Context{
		Session:  sess,
		Listener: "tcp",
	}
}
