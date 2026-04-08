package raftcluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

const (
	rpcServiceController          uint8             = 2
	controllerRPCShardKey         multiraft.GroupID = multiraft.GroupID(^uint32(0))
	controllerRPCHeartbeat        string            = "heartbeat"
	controllerRPCListAssignments  string            = "list_assignments"
	controllerRPCListRuntimeViews string            = "list_runtime_views"
)

type controllerRPCRequest struct {
	Kind   string                       `json:"kind"`
	Report *groupcontroller.AgentReport `json:"report,omitempty"`
}

type controllerRPCResponse struct {
	NotLeader    bool                              `json:"not_leader,omitempty"`
	LeaderID     uint64                            `json:"leader_id,omitempty"`
	Assignments  []controllermeta.GroupAssignment  `json:"assignments,omitempty"`
	RuntimeViews []controllermeta.GroupRuntimeView `json:"runtime_views,omitempty"`
}

type controllerAPI interface {
	Report(ctx context.Context, report groupcontroller.AgentReport) error
	RefreshAssignments(ctx context.Context) ([]controllermeta.GroupAssignment, error)
	ListRuntimeViews(ctx context.Context) ([]controllermeta.GroupRuntimeView, error)
}

type controllerClient struct {
	cluster *Cluster
	cache   *assignmentCache
	peers   []multiraft.NodeID

	mu     sync.RWMutex
	leader multiraft.NodeID
}

func newControllerClient(cluster *Cluster, peers []NodeConfig, cache *assignmentCache) *controllerClient {
	ids := make([]multiraft.NodeID, 0, len(peers))
	for _, peer := range peers {
		ids = append(ids, peer.NodeID)
	}
	return &controllerClient{
		cluster: cluster,
		cache:   cache,
		peers:   ids,
	}
}

func (c *controllerClient) Report(ctx context.Context, report groupcontroller.AgentReport) error {
	_, err := c.call(ctx, controllerRPCRequest{
		Kind:   controllerRPCHeartbeat,
		Report: &report,
	})
	return err
}

func (c *controllerClient) RefreshAssignments(ctx context.Context) ([]controllermeta.GroupAssignment, error) {
	resp, err := c.call(ctx, controllerRPCRequest{Kind: controllerRPCListAssignments})
	if err != nil {
		return nil, err
	}
	if c.cache != nil {
		c.cache.SetAssignments(resp.Assignments)
	}
	return resp.Assignments, nil
}

func (c *controllerClient) ListRuntimeViews(ctx context.Context) ([]controllermeta.GroupRuntimeView, error) {
	resp, err := c.call(ctx, controllerRPCRequest{Kind: controllerRPCListRuntimeViews})
	if err != nil {
		return nil, err
	}
	return resp.RuntimeViews, nil
}

func (c *controllerClient) call(ctx context.Context, req controllerRPCRequest) (controllerRPCResponse, error) {
	if c == nil || c.cluster == nil {
		return controllerRPCResponse{}, ErrNotStarted
	}

	body, err := json.Marshal(req)
	if err != nil {
		return controllerRPCResponse{}, err
	}

	targets := c.targets()
	if len(targets) == 0 {
		return controllerRPCResponse{}, ErrNoLeader
	}

	tried := make(map[multiraft.NodeID]struct{}, len(targets))
	var lastErr error
	for len(targets) > 0 {
		target := targets[0]
		targets = targets[1:]
		if _, seen := tried[target]; seen {
			continue
		}
		tried[target] = struct{}{}

		respBody, err := c.cluster.RPCService(ctx, target, controllerRPCShardKey, rpcServiceController, body)
		if err != nil {
			if c.cachedLeader() == target {
				c.clearLeader()
			}
			lastErr = err
			continue
		}

		var resp controllerRPCResponse
		if err := json.Unmarshal(respBody, &resp); err != nil {
			return controllerRPCResponse{}, err
		}
		if resp.NotLeader {
			if resp.LeaderID != 0 {
				leaderID := multiraft.NodeID(resp.LeaderID)
				c.setLeader(leaderID)
				if _, seen := tried[leaderID]; !seen {
					targets = append([]multiraft.NodeID{leaderID}, targets...)
				}
			} else {
				c.clearLeader()
			}
			lastErr = ErrNotLeader
			continue
		}

		c.setLeader(target)
		return resp, nil
	}

	if lastErr == nil {
		lastErr = ErrNoLeader
	}
	return controllerRPCResponse{}, lastErr
}

func (c *controllerClient) targets() []multiraft.NodeID {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	leader := c.leader
	c.mu.RUnlock()

	targets := make([]multiraft.NodeID, 0, len(c.peers)+1)
	if leader != 0 {
		targets = append(targets, leader)
	}
	for _, peer := range c.peers {
		if peer == leader {
			continue
		}
		targets = append(targets, peer)
	}
	return targets
}

func (c *controllerClient) cachedLeader() multiraft.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

func (c *controllerClient) setLeader(nodeID multiraft.NodeID) {
	c.mu.Lock()
	c.leader = nodeID
	c.mu.Unlock()
}

func (c *controllerClient) clearLeader() {
	c.mu.Lock()
	c.leader = 0
	c.mu.Unlock()
}

func isControllerRedirect(err error) bool {
	return errors.Is(err, ErrNotLeader)
}
