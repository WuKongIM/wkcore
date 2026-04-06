package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActorBuffersOutOfOrderEnvelopeAndDispatchesInSequence(t *testing.T) {
	runtime, _, pusher := newTestManager()

	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(2, 2)))
	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(1, 1)))

	require.Equal(t, []uint64{1, 2}, pusher.pushedSeqs())
}

func TestActorBindsAckIndexOnlyForAcceptedRoutes(t *testing.T) {
	runtime, _, pusher := newTestManager()
	accepted := testRoute("u2", 1, 11, 2)
	retryable := testRoute("u2", 1, 11, 3)
	dropped := testRoute("u2", 1, 11, 4)
	runtime.resolver.(*stubResolver).routesByChannel[testChannelID] = []RouteKey{
		accepted,
		retryable,
		dropped,
	}
	pusher.responses = []pushResponse{{
		result: PushResult{
			Accepted:  []RouteKey{accepted},
			Retryable: []RouteKey{retryable},
			Dropped:   []RouteKey{dropped},
		},
	}}

	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(101, 1)))

	binding, ok := runtime.ackIdx.Lookup(accepted.SessionID, 101)
	require.True(t, ok)
	require.Equal(t, accepted, binding.Route)
	_, ok = runtime.ackIdx.Lookup(retryable.SessionID, 101)
	require.False(t, ok)
	_, ok = runtime.ackIdx.Lookup(dropped.SessionID, 101)
	require.False(t, ok)
}

func TestActorRetryTickRetriesPendingRoutesUntilAcked(t *testing.T) {
	runtime, clock, pusher := newTestManager()
	pusher.responses = []pushResponse{
		{result: PushResult{Accepted: []RouteKey{testRoute("u2", 1, 11, 2)}}},
		{result: PushResult{Accepted: []RouteKey{testRoute("u2", 1, 11, 2)}}},
	}

	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(101, 1)))
	require.Equal(t, 1, pusher.attemptsFor(101))

	clock.Advance(time.Second)
	require.NoError(t, runtime.ProcessRetryTicks(context.Background()))
	require.Equal(t, 2, pusher.attemptsFor(101))

	require.NoError(t, runtime.AckRoute(context.Background(), RouteAck{
		UID:        "u2",
		SessionID:  2,
		MessageID:  101,
		MessageSeq: 1,
	}))

	clock.Advance(time.Second)
	require.NoError(t, runtime.ProcessRetryTicks(context.Background()))
	require.Equal(t, 2, pusher.attemptsFor(101))
}
