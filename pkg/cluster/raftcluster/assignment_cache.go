package raftcluster

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type assignmentCache struct {
	mu           sync.RWMutex
	assignments  []controllermeta.GroupAssignment
	peersByGroup map[multiraft.GroupID][]multiraft.NodeID
}

func newAssignmentCache() *assignmentCache {
	return &assignmentCache{
		peersByGroup: make(map[multiraft.GroupID][]multiraft.NodeID),
	}
}

func (c *assignmentCache) SetAssignments(assignments []controllermeta.GroupAssignment) {
	if c == nil {
		return
	}

	peersByGroup := make(map[multiraft.GroupID][]multiraft.NodeID, len(assignments))
	cloned := make([]controllermeta.GroupAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		cloned = append(cloned, controllermeta.GroupAssignment{
			GroupID:        assignment.GroupID,
			DesiredPeers:   append([]uint64(nil), assignment.DesiredPeers...),
			ConfigEpoch:    assignment.ConfigEpoch,
			BalanceVersion: assignment.BalanceVersion,
		})

		peers := make([]multiraft.NodeID, 0, len(assignment.DesiredPeers))
		for _, peer := range assignment.DesiredPeers {
			peers = append(peers, multiraft.NodeID(peer))
		}
		peersByGroup[multiraft.GroupID(assignment.GroupID)] = peers
	}

	c.mu.Lock()
	c.assignments = cloned
	c.peersByGroup = peersByGroup
	c.mu.Unlock()
}

func (c *assignmentCache) PeersForGroup(groupID multiraft.GroupID) ([]multiraft.NodeID, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	peers, ok := c.peersByGroup[groupID]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]multiraft.NodeID(nil), peers...), true
}

func (c *assignmentCache) Snapshot() []controllermeta.GroupAssignment {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]controllermeta.GroupAssignment, 0, len(c.assignments))
	for _, assignment := range c.assignments {
		out = append(out, controllermeta.GroupAssignment{
			GroupID:        assignment.GroupID,
			DesiredPeers:   append([]uint64(nil), assignment.DesiredPeers...),
			ConfigEpoch:    assignment.ConfigEpoch,
			BalanceVersion: assignment.BalanceVersion,
		})
	}
	return out
}
