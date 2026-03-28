package wkcluster

import "github.com/WuKongIM/wraft/multiraft"

type NodeInfo struct {
	NodeID multiraft.NodeID
	Addr   string
}

type NodeEvent struct {
	Type string // "join" | "leave"
	Node NodeInfo
}

type Discovery interface {
	GetNodes() []NodeInfo
	Resolve(nodeID multiraft.NodeID) (string, error)
	Watch() <-chan NodeEvent
	Stop()
}
