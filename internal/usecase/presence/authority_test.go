package presence

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestAuthorityRegisterMasterDifferentDeviceReturnsKickThenCloseActions(t *testing.T) {
	app := New(Options{})

	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 100, "old-device", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)

	result, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 2, 20, 200, "new-device", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, "kick_then_close", result.Actions[0].Kind)
	require.Equal(t, uint64(100), result.Actions[0].SessionID)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpoints, 1)
	require.Equal(t, uint64(200), endpoints[0].SessionID)
}

func TestAuthorityRegisterDuplicateSameRouteIsIdempotent(t *testing.T) {
	app := New(Options{})

	route := testRoute("u1", 1, 10, 100, "device-a", uint8(wkframe.DeviceLevelMaster))
	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   route,
	})
	require.NoError(t, err)

	result, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   route,
	})
	require.NoError(t, err)
	require.Empty(t, result.Actions)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpoints, 1)
	require.Equal(t, uint64(100), endpoints[0].SessionID)
}

func TestAuthorityRegisterMasterSameDeviceReturnsCloseActions(t *testing.T) {
	app := New(Options{})

	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 100, "same-device", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)

	result, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 11, 101, "same-device", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, "close", result.Actions[0].Kind)
	require.Equal(t, uint64(100), result.Actions[0].SessionID)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpoints, 1)
	require.Equal(t, uint64(101), endpoints[0].SessionID)
}

func TestAuthorityRegisterSlaveOnlyReplacesSameDeviceID(t *testing.T) {
	app := New(Options{})

	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 100, "device-a", uint8(wkframe.DeviceLevelSlave)),
	})
	require.NoError(t, err)
	_, err = app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 101, "device-b", uint8(wkframe.DeviceLevelSlave)),
	})
	require.NoError(t, err)

	result, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 2, 20, 200, "device-a", uint8(wkframe.DeviceLevelSlave)),
	})
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, "close", result.Actions[0].Kind)
	require.Equal(t, uint64(100), result.Actions[0].SessionID)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpoints, 2)
	require.ElementsMatch(t, []uint64{101, 200}, []uint64{endpoints[0].SessionID, endpoints[1].SessionID})
}

func TestAuthorityHeartbeatDetectsDigestMismatchWhenCountMatches(t *testing.T) {
	app := New(Options{})

	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 100, "device-a", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)

	result, err := app.HeartbeatAuthoritative(context.Background(), HeartbeatAuthoritativeCommand{
		Lease: GatewayLease{
			GroupID:       1,
			GatewayNodeID: 1,
			GatewayBootID: 10,
			RouteCount:    1,
			RouteDigest:   999,
		},
	})
	require.NoError(t, err)
	require.True(t, result.Mismatch)
	require.Equal(t, 1, result.RouteCount)
	require.NotEqual(t, uint64(999), result.RouteDigest)
}

func TestAuthorityReplayReplacesOwnerSetWithActiveRoutesOnly(t *testing.T) {
	app := New(Options{})

	err := app.ReplayAuthoritative(context.Background(), ReplayAuthoritativeCommand{
		Lease: GatewayLease{
			GroupID:       1,
			GatewayNodeID: 9,
			GatewayBootID: 99,
		},
		Routes: []Route{
			testRoute("u1", 9, 99, 100, "device-a", uint8(wkframe.DeviceLevelMaster)),
			testRoute("u2", 9, 99, 200, "device-b", uint8(wkframe.DeviceLevelMaster)),
		},
	})
	require.NoError(t, err)

	err = app.ReplayAuthoritative(context.Background(), ReplayAuthoritativeCommand{
		Lease: GatewayLease{
			GroupID:       1,
			GatewayNodeID: 9,
			GatewayBootID: 99,
		},
		Routes: []Route{
			testRoute("u1", 9, 99, 100, "device-a", uint8(wkframe.DeviceLevelMaster)),
		},
	})
	require.NoError(t, err)

	endpointsU1 := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpointsU1, 1)
	require.Equal(t, uint64(100), endpointsU1[0].SessionID)

	endpointsU2 := app.EndpointsByUID(context.Background(), "u2")
	require.Empty(t, endpointsU2)
}

func TestAuthorityEndpointsByUIDReturnsCurrentRoutes(t *testing.T) {
	app := New(Options{})

	_, err := app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   testRoute("u1", 1, 10, 100, "device-a", uint8(wkframe.DeviceLevelMaster)),
	})
	require.NoError(t, err)
	_, err = app.RegisterAuthoritative(context.Background(), RegisterAuthoritativeCommand{
		GroupID: 1,
		Route: Route{
			UID:         "u1",
			NodeID:      2,
			BootID:      20,
			SessionID:   200,
			DeviceID:    "device-b",
			DeviceFlag:  uint8(wkframe.WEB),
			DeviceLevel: uint8(wkframe.DeviceLevelMaster),
		},
	})
	require.NoError(t, err)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Len(t, endpoints, 2)
	require.ElementsMatch(t, []uint64{100, 200}, []uint64{endpoints[0].SessionID, endpoints[1].SessionID})
}

func TestAuthorityEndpointsByUIDEvictsExpiredLeaseRoutes(t *testing.T) {
	now := time.Unix(200, 0)
	app := New(Options{
		Now: func() time.Time { return now },
	})

	err := app.ReplayAuthoritative(context.Background(), ReplayAuthoritativeCommand{
		Lease: GatewayLease{
			GroupID:        1,
			GatewayNodeID:  9,
			GatewayBootID:  99,
			LeaseUntilUnix: now.Add(-time.Second).Unix(),
		},
		Routes: []Route{
			testRoute("u1", 9, 99, 100, "device-a", uint8(wkframe.DeviceLevelMaster)),
		},
	})
	require.NoError(t, err)

	endpoints := app.EndpointsByUID(context.Background(), "u1")
	require.Empty(t, endpoints)
}

func testRoute(uid string, nodeID, bootID, sessionID uint64, deviceID string, level uint8) Route {
	return Route{
		UID:         uid,
		NodeID:      nodeID,
		BootID:      bootID,
		SessionID:   sessionID,
		DeviceID:    deviceID,
		DeviceFlag:  uint8(wkframe.APP),
		DeviceLevel: level,
	}
}
