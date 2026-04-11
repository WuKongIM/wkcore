package store

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/cockroachdb/pebble/v2"
)

type Engine struct {
	mu          sync.Mutex
	db          *pebble.DB
	stores      map[channel.ChannelKey]*ChannelStore
	coordinator *commitCoordinator
}

func Open(path string) (*Engine, error) {
	pdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Engine{
		db:     pdb,
		stores: make(map[channel.ChannelKey]*ChannelStore),
	}, nil
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	if e.db == nil {
		e.mu.Unlock()
		return nil
	}
	pdb := e.db
	coordinator := e.coordinator
	e.db = nil
	e.stores = nil
	e.coordinator = nil
	e.mu.Unlock()
	if coordinator != nil {
		coordinator.close()
	}
	return pdb.Close()
}

func (e *Engine) ForChannel(key channel.ChannelKey, id channel.ChannelID) *ChannelStore {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.stores == nil {
		e.stores = make(map[channel.ChannelKey]*ChannelStore)
	}
	if st, ok := e.stores[key]; ok {
		return st
	}
	st := &ChannelStore{
		engine: e,
		key:    key,
		id:     id,
	}
	e.stores[key] = st
	return st
}

func (e *Engine) commitCoordinator() *commitCoordinator {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil {
		return nil
	}
	if e.coordinator == nil {
		e.coordinator = newCommitCoordinator(e.db)
	}
	return e.coordinator
}
