package wkcluster

import "github.com/WuKongIM/wraft/multiraft"

type StaticDiscovery struct {
	nodes map[multiraft.NodeID]NodeInfo
}

func NewStaticDiscovery(configs []NodeConfig) *StaticDiscovery {
	nodes := make(map[multiraft.NodeID]NodeInfo, len(configs))
	for _, c := range configs {
		nodes[c.NodeID] = NodeInfo{NodeID: c.NodeID, Addr: c.Addr}
	}
	return &StaticDiscovery{nodes: nodes}
}

func (s *StaticDiscovery) GetNodes() []NodeInfo {
	out := make([]NodeInfo, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	return out
}

func (s *StaticDiscovery) Resolve(nodeID multiraft.NodeID) (string, error) {
	n, ok := s.nodes[nodeID]
	if !ok {
		return "", ErrNodeNotFound
	}
	return n.Addr, nil
}

func (s *StaticDiscovery) Stop() {}
