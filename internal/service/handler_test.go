package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkstore"
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
	require.NoError(t, svc.OnSessionClose(ctx))

	require.Empty(t, svc.registry.SessionsByUID("u1"))
	_, ok := svc.registry.Session(1)
	require.False(t, ok)
}

func TestServiceOnSessionCloseIsNilSafe(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var svc *Service

		require.NotPanics(t, func() {
			require.NoError(t, svc.OnSessionClose(newAuthedContext(t, 1, "u1")))
		})
	})

	t.Run("nil context", func(t *testing.T) {
		svc := New(Options{})

		require.NotPanics(t, func() {
			require.NoError(t, svc.OnSessionClose(nil))
		})
	})

	t.Run("nil session", func(t *testing.T) {
		svc := New(Options{})

		require.NotPanics(t, func() {
			require.NoError(t, svc.OnSessionClose(&gateway.Context{}))
		})
	})
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

func TestServiceOnSessionOpenRejectsUnauthenticatedContext(t *testing.T) {
	svc := New(Options{Now: func() time.Time { return time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC) }})

	err := svc.OnSessionOpen(&gateway.Context{
		Session: session.New(session.Config{
			ID:       1,
			Listener: "tcp",
		}),
	})

	require.ErrorIs(t, err, ErrUnauthenticatedSession)
	require.Empty(t, svc.registry.SessionsByUID(""))
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

func TestNewServiceUsesInjectedRegistry(t *testing.T) {
	fixedNow := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	reg := &capturingRegistry{}
	svc := New(Options{
		Now:      func() time.Time { return fixedNow },
		Registry: reg,
	})

	ctx := newAuthedContext(t, 1, "u1")
	require.NoError(t, svc.OnSessionOpen(ctx))

	require.Len(t, reg.registered, 1)
	require.Equal(t, "u1", reg.registered[0].UID)
	require.Equal(t, fixedNow, reg.registered[0].ConnectedAt)
}

func TestNewServicePreservesInjectedBusinessPorts(t *testing.T) {
	users := &fakeIdentityStore{}
	channels := &fakeChannelStore{}
	cluster := &fakeClusterPort{}

	svc := New(Options{
		IdentityStore: users,
		ChannelStore:  channels,
		ClusterPort:   cluster,
	})

	var identityPort IdentityStore = users
	_, _ = identityPort.GetUser(context.Background(), "u1")

	var channelPort ChannelStore = channels
	_, _ = channelPort.GetChannel(context.Background(), "c1", 1)

	require.Same(t, users, svc.users)
	require.Same(t, channels, svc.channels)
	require.Same(t, cluster, svc.cluster)
}

func TestNewServiceKeepsDefaultsWhenPortsUnset(t *testing.T) {
	svc := New(Options{})

	require.Nil(t, svc.users)
	require.Nil(t, svc.channels)
	require.Nil(t, svc.cluster)
}

func TestWKStoreSatisfiesServiceBusinessPorts(t *testing.T) {
	var _ IdentityStore = (*wkstore.Store)(nil)
	var _ ChannelStore = (*wkstore.Store)(nil)
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

type capturingRegistry struct {
	registered   []SessionMeta
	bySessionID  map[uint64]SessionMeta
	byUID        map[string][]SessionMeta
	unregistered []uint64
}

func (r *capturingRegistry) Register(meta SessionMeta) error {
	r.registered = append(r.registered, meta)
	if r.bySessionID == nil {
		r.bySessionID = make(map[uint64]SessionMeta)
	}
	if r.byUID == nil {
		r.byUID = make(map[string][]SessionMeta)
	}
	r.bySessionID[meta.SessionID] = meta
	r.byUID[meta.UID] = append(r.byUID[meta.UID], meta)
	return nil
}

func (r *capturingRegistry) Unregister(sessionID uint64) {
	r.unregistered = append(r.unregistered, sessionID)
	delete(r.bySessionID, sessionID)
}

func (r *capturingRegistry) Session(sessionID uint64) (SessionMeta, bool) {
	meta, ok := r.bySessionID[sessionID]
	return meta, ok
}

func (r *capturingRegistry) SessionsByUID(uid string) []SessionMeta {
	return append([]SessionMeta(nil), r.byUID[uid]...)
}

type fakeIdentityStore struct{}

func (*fakeIdentityStore) GetUser(context.Context, string) (wkdb.User, error) {
	return wkdb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (wkdb.Channel, error) {
	return wkdb.Channel{}, nil
}

type fakeClusterPort struct{}
