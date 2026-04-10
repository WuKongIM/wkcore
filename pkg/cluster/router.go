package cluster

import (
	"hash/crc32"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type Router struct {
	slotCount uint32
	runtime   *multiraft.Runtime
	localNode multiraft.NodeID
}

func NewRouter(slotCount uint32, localNode multiraft.NodeID, runtime *multiraft.Runtime) *Router {
	return &Router{
		slotCount: slotCount,
		runtime:   runtime,
		localNode: localNode,
	}
}

// SlotForKey maps a key to a raft slot via CRC32 hashing.
// NOTE: The mapping is deterministic for a given slotCount but will change
// if slotCount changes (no consistent hashing). All nodes must use the same
// slotCount for correct routing.
func (r *Router) SlotForKey(key string) multiraft.SlotID {
	return multiraft.SlotID(crc32.ChecksumIEEE([]byte(key))%r.slotCount + 1)
}

func (r *Router) LeaderOf(slotID multiraft.SlotID) (multiraft.NodeID, error) {
	if r == nil || r.runtime == nil {
		return 0, ErrNotStarted
	}
	status, err := r.runtime.Status(slotID)
	if err != nil {
		return 0, err
	}
	if status.LeaderID == 0 {
		return 0, ErrNoLeader
	}
	return status.LeaderID, nil
}

func (r *Router) IsLocal(nodeID multiraft.NodeID) bool {
	return nodeID == r.localNode
}
