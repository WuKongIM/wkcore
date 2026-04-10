package cluster

import (
	"hash/crc32"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type Router struct {
	groupCount uint32
	runtime    *multiraft.Runtime
	localNode  multiraft.NodeID
}

func NewRouter(groupCount uint32, localNode multiraft.NodeID, runtime *multiraft.Runtime) *Router {
	return &Router{
		groupCount: groupCount,
		runtime:    runtime,
		localNode:  localNode,
	}
}

// SlotForKey maps a key to a raft group via CRC32 hashing.
// NOTE: The mapping is deterministic for a given groupCount but will change
// if groupCount changes (no consistent hashing). All nodes must use the same
// groupCount for correct routing.
func (r *Router) SlotForKey(key string) multiraft.GroupID {
	return multiraft.GroupID(crc32.ChecksumIEEE([]byte(key))%r.groupCount + 1)
}

func (r *Router) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error) {
	if r == nil || r.runtime == nil {
		return 0, ErrNotStarted
	}
	status, err := r.runtime.Status(groupID)
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
