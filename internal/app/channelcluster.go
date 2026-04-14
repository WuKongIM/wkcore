package app

import (
	"context"
	"errors"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	channeltransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
)

type appChannelCluster struct {
	service channel.MetaRollbackService
	runtime channel.Runtime
	closers []func() error

	closeOnce sync.Once
	closeErr  error

	applyMu    sync.Mutex
	applyLocks map[channel.ChannelKey]*appChannelApplyLock
}

type appChannelApplyLock struct {
	mu   sync.Mutex
	refs int
}

func newAppChannelCluster(
	store *channelstore.Engine,
	rt channelruntime.Runtime,
	transport *channeltransport.Transport,
	messageIDs channel.MessageIDGenerator,
) (*appChannelCluster, error) {
	if store == nil || rt == nil || transport == nil || messageIDs == nil {
		return nil, channel.ErrInvalidConfig
	}
	service, err := channelhandler.New(channelhandler.Config{
		Runtime:    rt,
		Store:      store,
		MessageIDs: messageIDs,
	})
	if err != nil {
		return nil, err
	}
	transport.BindFetchService(rt)
	return &appChannelCluster{
		service: service,
		runtime: appChannelRuntimeControl{runtime: rt},
		closers: []func() error{
			transport.Close,
			rt.Close,
		},
	}, nil
}

func (c *appChannelCluster) ApplyMeta(meta channel.Meta) error {
	if c == nil || c.service == nil || c.runtime == nil {
		return channel.ErrInvalidConfig
	}
	key := meta.Key
	if key == "" {
		key = channelhandler.KeyFromChannelID(meta.ID)
		meta.Key = key
	}
	unlock := c.lockApplyMeta(key)
	defer unlock()

	previous, ok := c.service.MetaSnapshot(key)
	if err := c.service.ApplyMeta(meta); err != nil {
		return err
	}
	var runtimeErr error
	if meta.Status == channel.StatusDeleted {
		runtimeErr = c.runtime.RemoveChannel(key)
	} else {
		runtimeErr = c.runtime.UpsertMeta(meta)
	}
	if runtimeErr == nil {
		return nil
	}
	c.service.RestoreMeta(key, previous, ok)
	return runtimeErr
}

func (c *appChannelCluster) lockApplyMeta(key channel.ChannelKey) func() {
	c.applyMu.Lock()
	if c.applyLocks == nil {
		c.applyLocks = make(map[channel.ChannelKey]*appChannelApplyLock)
	}
	lock, ok := c.applyLocks[key]
	if !ok {
		lock = &appChannelApplyLock{}
		c.applyLocks[key] = lock
	}
	lock.refs++
	c.applyMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()

		c.applyMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.applyLocks, key)
		}
		c.applyMu.Unlock()
	}
}

func (c *appChannelCluster) Append(ctx context.Context, req channel.AppendRequest) (channel.AppendResult, error) {
	if c == nil || c.service == nil {
		return channel.AppendResult{}, channel.ErrInvalidConfig
	}
	return c.service.Append(ctx, req)
}

func (c *appChannelCluster) Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error) {
	if c == nil || c.service == nil {
		return channel.FetchResult{}, channel.ErrInvalidConfig
	}
	return c.service.Fetch(ctx, req)
}

func (c *appChannelCluster) Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	if c == nil || c.service == nil {
		return channel.ChannelRuntimeStatus{}, channel.ErrInvalidConfig
	}
	return c.service.Status(id)
}

func (c *appChannelCluster) MetaSnapshot(key channel.ChannelKey) (channel.Meta, bool) {
	if c == nil || c.service == nil {
		return channel.Meta{}, false
	}
	return c.service.MetaSnapshot(key)
}

func (c *appChannelCluster) RestoreMeta(key channel.ChannelKey, meta channel.Meta, ok bool) {
	if c == nil || c.service == nil {
		return
	}
	c.service.RestoreMeta(key, meta, ok)
}

func (c *appChannelCluster) RemoveLocal(key channel.ChannelKey) error {
	if c == nil {
		return nil
	}
	var err error
	if c.runtime != nil {
		err = c.runtime.RemoveChannel(key)
		if errors.Is(err, channel.ErrChannelNotFound) {
			err = nil
		}
	}
	if c.service != nil {
		c.service.RestoreMeta(key, channel.Meta{}, false)
	}
	return err
}

func (c *appChannelCluster) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		for _, closeFn := range c.closers {
			if closeFn == nil {
				continue
			}
			c.closeErr = errors.Join(c.closeErr, closeFn())
		}
	})
	return c.closeErr
}

type appChannelRuntimeControl struct {
	runtime channelruntime.Runtime
}

func (c appChannelRuntimeControl) UpsertMeta(meta channel.Meta) error {
	if c.runtime == nil {
		return channel.ErrInvalidConfig
	}
	if err := c.runtime.EnsureChannel(meta); err != nil {
		if errors.Is(err, channelruntime.ErrChannelExists) {
			return c.runtime.ApplyMeta(meta)
		}
		return err
	}
	return nil
}

func (c appChannelRuntimeControl) RemoveChannel(key channel.ChannelKey) error {
	if c.runtime == nil {
		return channel.ErrInvalidConfig
	}
	if err := c.runtime.RemoveChannel(key); err != nil && !errors.Is(err, channel.ErrChannelNotFound) {
		return err
	}
	return nil
}

func (c appChannelRuntimeControl) Close() error {
	if c.runtime == nil {
		return nil
	}
	return c.runtime.Close()
}
