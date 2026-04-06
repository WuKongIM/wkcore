package app

import (
	"context"
	"sync"
	"time"
)

const defaultPresenceHeartbeatInterval = 10 * time.Second

type presenceHeartbeater interface {
	HeartbeatOnce(ctx context.Context) error
}

type presenceWorker struct {
	heartbeater presenceHeartbeater
	interval    time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newPresenceWorker(heartbeater presenceHeartbeater, interval time.Duration) *presenceWorker {
	if interval <= 0 {
		interval = defaultPresenceHeartbeatInterval
	}
	return &presenceWorker{
		heartbeater: heartbeater,
		interval:    interval,
	}
}

func (w *presenceWorker) Start() error {
	if w == nil || w.heartbeater == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := w.interval
	heartbeater := w.heartbeater

	w.cancel = cancel
	w.done = done

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(done)

		for {
			select {
			case <-ticker.C:
				_ = heartbeater.HeartbeatOnce(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (w *presenceWorker) Stop() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	w.cancel = nil
	w.done = nil
	w.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()
	if done != nil {
		<-done
	}
	return nil
}
