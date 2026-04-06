package presence

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestWorkerSendsOneHeartbeatPerNonEmptyGroupBucket(t *testing.T) {
	onlineReg := online.NewRegistry()
	mustRegisterTestConn(t, onlineReg, 11, "u1", "d1", 1)
	mustRegisterTestConn(t, onlineReg, 12, "u2", "d2", 1)
	mustRegisterTestConn(t, onlineReg, 21, "u3", "d3", 2)

	authority := &fakeAuthorityClient{}
	app := New(Options{
		LocalNodeID:     1,
		GatewayBootID:   101,
		Online:          onlineReg,
		AuthorityClient: authority,
		Now:             func() time.Time { return time.Unix(200, 0) },
	})

	require.NoError(t, app.HeartbeatOnce(context.Background()))
	require.Len(t, authority.heartbeatCalls, 2)
	require.Equal(t, uint64(1), authority.heartbeatCalls[0].Lease.GroupID)
	require.Equal(t, 2, authority.heartbeatCalls[0].Lease.RouteCount)
	require.Equal(t, uint64(2), authority.heartbeatCalls[1].Lease.GroupID)
	require.Equal(t, 1, authority.heartbeatCalls[1].Lease.RouteCount)
}

func TestWorkerReplayUsesOnlyActiveRoutes(t *testing.T) {
	onlineReg := online.NewRegistry()
	mustRegisterTestConn(t, onlineReg, 11, "u1", "d1", 1)
	mustRegisterTestConn(t, onlineReg, 12, "u2", "d2", 1)
	mustRegisterTestConn(t, onlineReg, 13, "u3", "d3", 1)
	_, ok := onlineReg.MarkClosing(13)
	require.True(t, ok)

	authority := &fakeAuthorityClient{
		heartbeatBySlot: map[uint64]HeartbeatAuthoritativeResult{
			1: {Mismatch: true},
		},
	}
	app := New(Options{
		LocalNodeID:     1,
		GatewayBootID:   101,
		Online:          onlineReg,
		AuthorityClient: authority,
		Now:             func() time.Time { return time.Unix(200, 0) },
	})

	require.NoError(t, app.HeartbeatOnce(context.Background()))
	require.Len(t, authority.replayCalls, 1)
	require.Len(t, authority.replayCalls[0].Routes, 2)
	require.ElementsMatch(t, []uint64{11, 12}, []uint64{
		authority.replayCalls[0].Routes[0].SessionID,
		authority.replayCalls[0].Routes[1].SessionID,
	})
}

func mustRegisterTestConn(t *testing.T, reg online.Registry, sessionID uint64, uid, deviceID string, groupID uint64) {
	t.Helper()

	require.NoError(t, reg.Register(online.OnlineConn{
		SessionID:   sessionID,
		UID:         uid,
		DeviceID:    deviceID,
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		GroupID:     groupID,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		ConnectedAt: time.Unix(200, 0),
		Session:     session.New(session.Config{ID: sessionID, Listener: "tcp"}),
	}))
}
