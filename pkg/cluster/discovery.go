package cluster

import "github.com/WuKongIM/WuKongIM/pkg/group/multiraft"

type NodeInfo struct {
	NodeID multiraft.NodeID
	Addr   string
}

type Discovery interface {
	GetNodes() []NodeInfo
	Resolve(nodeID uint64) (string, error)
	Stop()
}
