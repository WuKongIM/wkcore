package delivery

import "context"

type actor struct {
	shard           *shard
	key             ChannelKey
	nextDispatchSeq uint64
	reorder         map[uint64]CommittedEnvelope
	inflight        map[uint64]*InflightMessage
	lastActive      int64
}

func newActor(shard *shard, key ChannelKey) *actor {
	return &actor{
		shard:           shard,
		key:             key,
		nextDispatchSeq: 1,
		reorder:         make(map[uint64]CommittedEnvelope),
		inflight:        make(map[uint64]*InflightMessage),
		lastActive:      shard.manager.clock.Now().UnixNano(),
	}
}

func (a *actor) handleStartDispatch(ctx context.Context, env CommittedEnvelope) error {
	a.touch()
	switch {
	case env.MessageSeq < a.nextDispatchSeq:
		return nil
	case env.MessageSeq > a.nextDispatchSeq:
		a.reorder[env.MessageSeq] = cloneEnvelope(env)
		return nil
	default:
	}
	if err := a.dispatch(ctx, env); err != nil {
		return err
	}
	a.nextDispatchSeq++
	return a.flushReady(ctx)
}

func (a *actor) handleRouteAck(_ context.Context, event RouteAcked) error {
	a.touch()
	msg := a.inflight[event.MessageID]
	if msg == nil {
		return nil
	}
	a.finishRoute(msg, event.Route)
	return nil
}

func (a *actor) handleRouteOffline(_ context.Context, event RouteOffline) error {
	a.touch()
	msg := a.inflight[event.MessageID]
	if msg == nil {
		return nil
	}
	a.finishRoute(msg, event.Route)
	return nil
}

func (a *actor) handleRetryTick(ctx context.Context, event RetryTick) error {
	a.touch()
	msg := a.inflight[event.Entry.MessageID]
	if msg == nil {
		return nil
	}
	state, ok := msg.Routes[event.Entry.Route]
	if !ok {
		return nil
	}
	if event.Entry.Attempt <= state.Attempt {
		return nil
	}
	return a.applyPush(ctx, msg, []RouteKey{event.Entry.Route}, event.Entry.Attempt)
}

func (a *actor) dispatch(ctx context.Context, env CommittedEnvelope) error {
	routes, err := a.shard.manager.resolver.ResolveRoutes(ctx, a.key, env)
	if err != nil || len(routes) == 0 {
		return nil
	}
	msg := &InflightMessage{
		MessageID:  env.MessageID,
		MessageSeq: env.MessageSeq,
		Envelope:   cloneEnvelope(env),
		Routes:     make(map[RouteKey]*RouteDeliveryState),
	}
	a.inflight[env.MessageID] = msg
	if err := a.applyPush(ctx, msg, routes, 1); err != nil {
		return err
	}
	if msg.PendingRouteCnt == 0 {
		delete(a.inflight, env.MessageID)
	}
	return nil
}

func (a *actor) applyPush(ctx context.Context, msg *InflightMessage, routes []RouteKey, attempt int) error {
	result, err := a.shard.manager.push.Push(ctx, PushCommand{
		Envelope: cloneEnvelope(msg.Envelope),
		Routes:   append([]RouteKey(nil), routes...),
		Attempt:  attempt,
	})
	if err != nil {
		result = PushResult{Retryable: append([]RouteKey(nil), routes...)}
	}
	for _, route := range result.Accepted {
		state := a.ensureRouteState(msg, route)
		state.Attempt = attempt
		state.Accepted = true
		a.shard.manager.ackIdx.Bind(AckBinding{
			SessionID:   route.SessionID,
			MessageID:   msg.MessageID,
			ChannelID:   a.key.ChannelID,
			ChannelType: a.key.ChannelType,
			Route:       route,
		})
		a.scheduleRetry(msg, route, attempt)
	}
	for _, route := range result.Retryable {
		state := a.ensureRouteState(msg, route)
		state.Attempt = attempt
		a.scheduleRetry(msg, route, attempt)
	}
	for _, route := range result.Dropped {
		a.finishRoute(msg, route)
	}
	return nil
}

func (a *actor) ensureRouteState(msg *InflightMessage, route RouteKey) *RouteDeliveryState {
	state := msg.Routes[route]
	if state != nil {
		return state
	}
	state = &RouteDeliveryState{}
	msg.Routes[route] = state
	msg.PendingRouteCnt++
	return state
}

func (a *actor) finishRoute(msg *InflightMessage, route RouteKey) {
	if _, ok := msg.Routes[route]; !ok {
		return
	}
	delete(msg.Routes, route)
	if msg.PendingRouteCnt > 0 {
		msg.PendingRouteCnt--
	}
	a.shard.manager.ackIdx.Remove(route.SessionID, msg.MessageID)
	if msg.PendingRouteCnt == 0 {
		delete(a.inflight, msg.MessageID)
	}
}

func (a *actor) scheduleRetry(msg *InflightMessage, route RouteKey, attempt int) {
	delay, ok := a.shard.nextRetryDelay(attempt + 1)
	if !ok {
		return
	}
	a.shard.wheel.Schedule(RetryEntry{
		When:        a.shard.manager.clock.Now().Add(delay),
		ChannelID:   a.key.ChannelID,
		ChannelType: a.key.ChannelType,
		MessageID:   msg.MessageID,
		Route:       route,
		Attempt:     attempt + 1,
	})
}

func (a *actor) flushReady(ctx context.Context) error {
	for {
		env, ok := a.reorder[a.nextDispatchSeq]
		if !ok {
			return nil
		}
		delete(a.reorder, a.nextDispatchSeq)
		if err := a.dispatch(ctx, env); err != nil {
			return err
		}
		a.nextDispatchSeq++
	}
}

func (a *actor) isIdle(nowUnixNano int64, idleTimeout int64) bool {
	if len(a.reorder) > 0 || len(a.inflight) > 0 {
		return false
	}
	return nowUnixNano-a.lastActive >= idleTimeout
}

func (a *actor) touch() {
	a.lastActive = a.shard.manager.clock.Now().UnixNano()
}

func cloneEnvelope(env CommittedEnvelope) CommittedEnvelope {
	copied := env
	copied.Payload = append([]byte(nil), env.Payload...)
	return copied
}
