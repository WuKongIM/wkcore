package wkcluster

import (
	"hash/crc32"

	"github.com/WuKongIM/wraft/multiraft"
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

func (r *Router) SlotForChannel(channelID string) multiraft.GroupID {
	return multiraft.GroupID(crc32.ChecksumIEEE([]byte(channelID))%r.groupCount + 1)
}

func (r *Router) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error) {
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
