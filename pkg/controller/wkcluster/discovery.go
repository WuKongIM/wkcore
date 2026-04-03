package wkcluster

import "github.com/WuKongIM/WuKongIM/pkg/consensus/multiraft"

type NodeInfo struct {
	NodeID multiraft.NodeID
	Addr   string
}

type Discovery interface {
	GetNodes() []NodeInfo
	Resolve(nodeID uint64) (string, error)
	Stop()
}
