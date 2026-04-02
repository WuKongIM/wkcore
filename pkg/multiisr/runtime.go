package multiisr

import (
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type runtime struct {
	cfg Config

	mu         sync.RWMutex
	groups     map[uint64]*group
	tombstones map[uint64]map[uint64]tombstone
}

func New(cfg Config) (Runtime, error) {
	if cfg.LocalNode == 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.ReplicaFactory == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.GenerationStore == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Transport == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.PeerSessions == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Limits.MaxGroups < 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Tombstones.TombstoneTTL <= 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r := &runtime{
		cfg:        cfg,
		groups:     make(map[uint64]*group),
		tombstones: make(map[uint64]map[uint64]tombstone),
	}
	cfg.Transport.RegisterHandler(r.handleEnvelope)
	return r, nil
}

func (r *runtime) EnsureGroup(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if _, ok := r.groupLocked(meta.GroupID); ok {
		return nil
	}
	if r.cfg.Limits.MaxGroups > 0 && len(r.groups) >= r.cfg.Limits.MaxGroups {
		return ErrTooManyGroups
	}

	generation, err := r.allocateGeneration(meta.GroupID)
	if err != nil {
		return err
	}
	replica, err := r.cfg.ReplicaFactory.New(GroupConfig{
		GroupID:    meta.GroupID,
		Generation: generation,
		Meta:       meta,
	})
	if err != nil {
		return err
	}
	if err := replica.ApplyMeta(meta); err != nil {
		return err
	}

	r.putGroupLocked(&group{
		id:         meta.GroupID,
		generation: generation,
		replica:    replica,
	})
	return nil
}

func (r *runtime) RemoveGroup(groupID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.groupLocked(groupID)
	if !ok {
		return ErrGroupNotFound
	}
	delete(r.groups, groupID)
	if err := g.replica.Tombstone(); err != nil {
		return err
	}
	r.tombstoneGroupLocked(g)
	return nil
}

func (r *runtime) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.RLock()
	g, ok := r.groups[meta.GroupID]
	r.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}
	return g.replica.ApplyMeta(meta)
}

func (r *runtime) Group(groupID uint64) (GroupHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())
	g, ok := r.groupLocked(groupID)
	if !ok {
		return nil, false
	}
	return g, true
}

func (r *runtime) handleEnvelope(env Envelope) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if g, ok := r.groups[env.GroupID]; ok && g.generation == env.Generation {
		return
	}
	if generations, ok := r.tombstones[env.GroupID]; ok {
		if _, ok := generations[env.Generation]; ok {
			return
		}
	}
}
