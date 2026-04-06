package delivery

import (
	"context"
	"sync"
	"time"
)

type shard struct {
	mu      sync.Mutex
	manager *Manager
	actors  map[ChannelKey]*actor
	wheel   *RetryWheel
}

func newShard(manager *Manager) *shard {
	return &shard{
		manager: manager,
		actors:  make(map[ChannelKey]*actor),
		wheel:   NewRetryWheel(),
	}
}

func (s *shard) submit(ctx context.Context, env CommittedEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ChannelKey{ChannelID: env.ChannelID, ChannelType: env.ChannelType}
	act := s.actorFor(key)
	return act.handleStartDispatch(ctx, env)
}

func (s *shard) routeAcked(ctx context.Context, binding AckBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	act := s.actors[ChannelKey{ChannelID: binding.ChannelID, ChannelType: binding.ChannelType}]
	if act == nil {
		return nil
	}
	return act.handleRouteAck(ctx, RouteAcked{
		MessageID: binding.MessageID,
		Route:     binding.Route,
	})
}

func (s *shard) routeOffline(ctx context.Context, binding AckBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	act := s.actors[ChannelKey{ChannelID: binding.ChannelID, ChannelType: binding.ChannelType}]
	if act == nil {
		return nil
	}
	return act.handleRouteOffline(ctx, RouteOffline{
		MessageID: binding.MessageID,
		Route:     binding.Route,
	})
}

func (s *shard) processRetryTicks(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	due := s.wheel.PopDue(s.manager.clock.Now())
	for _, entry := range due {
		act := s.actors[ChannelKey{ChannelID: entry.ChannelID, ChannelType: entry.ChannelType}]
		if act == nil {
			continue
		}
		if err := act.handleRetryTick(ctx, RetryTick{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (s *shard) sweepIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowUnixNano := s.manager.clock.Now().UnixNano()
	for key, act := range s.actors {
		if act.isIdle(nowUnixNano, s.manager.idleTimeout.Nanoseconds()) {
			delete(s.actors, key)
		}
	}
}

func (s *shard) actorFor(key ChannelKey) *actor {
	act := s.actors[key]
	if act != nil {
		return act
	}
	act = newActor(s, key)
	s.actors[key] = act
	return act
}

func (s *shard) nextRetryDelay(attempt int) (time.Duration, bool) {
	if attempt <= 1 {
		return 0, false
	}
	if s.manager.maxRetryAttempts > 0 && attempt > s.manager.maxRetryAttempts {
		return 0, false
	}
	if len(s.manager.retryDelays) == 0 {
		return 0, false
	}
	index := attempt - 2
	if index >= len(s.manager.retryDelays) {
		index = len(s.manager.retryDelays) - 1
	}
	return s.manager.retryDelays[index], true
}
