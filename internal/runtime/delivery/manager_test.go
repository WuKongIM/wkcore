package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestManagerMaterializesOneVirtualActorPerChannel(t *testing.T) {
	runtime, _, _ := newTestManager()

	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(1, 1)))
	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(2, 2)))

	require.Equal(t, 1, runtime.actorCount())
}

func TestManagerEvictsIdleActors(t *testing.T) {
	runtime, clock, _ := newTestManager()
	runtime.resolver.(*stubResolver).routesByChannel[testChannelID] = nil

	require.NoError(t, runtime.Submit(context.Background(), testEnvelope(1, 1)))
	require.Equal(t, 1, runtime.actorCount())

	clock.Advance(2 * time.Minute)
	runtime.SweepIdle()

	require.Zero(t, runtime.actorCount())
}

func newTestManager() (*Manager, *testClock, *recordingPusher) {
	clock := &testClock{now: time.Date(2026, 4, 6, 12, 0, 0, 0, time.UTC)}
	pusher := &recordingPusher{}
	runtime := NewManager(Config{
		Resolver: &stubResolver{
			routesByChannel: map[string][]RouteKey{
				testChannelID: {
					testRoute("u2", 1, 11, 2),
				},
			},
		},
		Push:             pusher,
		Clock:            clock,
		ShardCount:       1,
		IdleTimeout:      time.Minute,
		RetryDelays:      []time.Duration{time.Second},
		MaxRetryAttempts: 4,
	})
	return runtime, clock, pusher
}

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time {
	return c.now
}

func (c *testClock) Advance(delta time.Duration) {
	c.now = c.now.Add(delta)
}

type stubResolver struct {
	routesByChannel map[string][]RouteKey
}

func (r *stubResolver) ResolveRoutes(_ context.Context, key ChannelKey, _ CommittedEnvelope) ([]RouteKey, error) {
	routes := r.routesByChannel[key.ChannelID]
	return append([]RouteKey(nil), routes...), nil
}

type pushResponse struct {
	result PushResult
	err    error
}

type pushCall struct {
	envelope CommittedEnvelope
	routes   []RouteKey
}

type recordingPusher struct {
	calls     []pushCall
	responses []pushResponse
}

func (p *recordingPusher) Push(_ context.Context, cmd PushCommand) (PushResult, error) {
	copiedEnv := cmd.Envelope
	copiedEnv.Payload = append([]byte(nil), cmd.Envelope.Payload...)
	copiedRoutes := append([]RouteKey(nil), cmd.Routes...)
	p.calls = append(p.calls, pushCall{
		envelope: copiedEnv,
		routes:   copiedRoutes,
	})
	if len(p.responses) == 0 {
		return PushResult{Accepted: copiedRoutes}, nil
	}
	resp := p.responses[0]
	p.responses = p.responses[1:]
	return resp.result, resp.err
}

func (p *recordingPusher) pushedSeqs() []uint64 {
	out := make([]uint64, 0, len(p.calls))
	for _, call := range p.calls {
		out = append(out, call.envelope.MessageSeq)
	}
	return out
}

func (p *recordingPusher) attemptsFor(messageID uint64) int {
	count := 0
	for _, call := range p.calls {
		if call.envelope.MessageID == messageID {
			count++
		}
	}
	return count
}

const testChannelID = "u1@u2"

func testEnvelope(messageID, messageSeq uint64) CommittedEnvelope {
	return CommittedEnvelope{
		ChannelID:   testChannelID,
		ChannelType: wkframe.ChannelTypePerson,
		MessageID:   messageID,
		MessageSeq:  messageSeq,
		SenderUID:   "u1",
		ClientMsgNo: "m1",
		Payload:     []byte("hi"),
		ClientSeq:   9,
	}
}

func testRoute(uid string, nodeID, bootID, sessionID uint64) RouteKey {
	return RouteKey{
		UID:       uid,
		NodeID:    nodeID,
		BootID:    bootID,
		SessionID: sessionID,
	}
}
