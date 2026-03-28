package wkcluster

import "github.com/WuKongIM/wraft/multiraft"

type NodeInfo struct {
	NodeID multiraft.NodeID
	Addr   string
}

type Discovery interface {
	GetNodes() []NodeInfo
	Resolve(nodeID multiraft.NodeID) (string, error)
	Stop()
}
