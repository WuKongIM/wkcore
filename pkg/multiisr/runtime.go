package multiisr

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type runtime struct {
	cfg Config
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
	return &runtime{cfg: cfg}, nil
}

func (r *runtime) EnsureGroup(meta isr.GroupMeta) error {
	return errNotImplemented
}

func (r *runtime) RemoveGroup(groupID uint64) error {
	return errNotImplemented
}

func (r *runtime) ApplyMeta(meta isr.GroupMeta) error {
	return errNotImplemented
}

func (r *runtime) Group(groupID uint64) (GroupHandle, bool) {
	return nil, false
}
