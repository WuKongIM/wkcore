package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const (
	rpcServiceController          uint8            = 14
	controllerRPCShardKey         multiraft.SlotID = multiraft.SlotID(^uint32(0))
	controllerRPCHeartbeat        string           = "heartbeat"
	controllerRPCListAssignments  string           = "list_assignments"
	controllerRPCListNodes        string           = "list_nodes"
	controllerRPCListRuntimeViews string           = "list_runtime_views"
	controllerRPCOperator         string           = "operator"
	controllerRPCGetTask          string           = "get_task"
	controllerRPCForceReconcile   string           = "force_reconcile"
	controllerRPCTaskResult       string           = "task_result"
)

type controllerRPCRequest struct {
	Kind    string                          `json:"kind"`
	SlotID  uint32                          `json:"slot_id,omitempty"`
	Report  *slotcontroller.AgentReport     `json:"report,omitempty"`
	Op      *slotcontroller.OperatorRequest `json:"op,omitempty"`
	Advance *controllerTaskAdvance          `json:"advance,omitempty"`
}

type controllerTaskAdvance struct {
	SlotID  uint32    `json:"slot_id"`
	Attempt uint32    `json:"attempt,omitempty"`
	Now     time.Time `json:"now"`
	Err     string    `json:"err,omitempty"`
}

type controllerRPCResponse struct {
	NotLeader    bool                             `json:"not_leader,omitempty"`
	NotFound     bool                             `json:"not_found,omitempty"`
	LeaderID     uint64                           `json:"leader_id,omitempty"`
	Nodes        []controllermeta.ClusterNode     `json:"nodes,omitempty"`
	Assignments  []controllermeta.SlotAssignment  `json:"assignments,omitempty"`
	RuntimeViews []controllermeta.SlotRuntimeView `json:"runtime_views,omitempty"`
	Task         *controllermeta.ReconcileTask    `json:"task,omitempty"`
}

type controllerAPI interface {
	Report(ctx context.Context, report slotcontroller.AgentReport) error
	ListNodes(ctx context.Context) ([]controllermeta.ClusterNode, error)
	RefreshAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error)
	ListRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error)
	Operator(ctx context.Context, op slotcontroller.OperatorRequest) error
	GetTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error)
	ForceReconcile(ctx context.Context, slotID uint32) error
	ReportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error
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

func (c *controllerClient) Report(ctx context.Context, report slotcontroller.AgentReport) error {
	_, err := c.call(ctx, controllerRPCRequest{
		Kind:   controllerRPCHeartbeat,
		Report: &report,
	})
	return err
}

func (c *controllerClient) ListNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	resp, err := c.call(ctx, controllerRPCRequest{Kind: controllerRPCListNodes})
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

func (c *controllerClient) RefreshAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error) {
	resp, err := c.call(ctx, controllerRPCRequest{Kind: controllerRPCListAssignments})
	if err != nil {
		return nil, err
	}
	if c.cache != nil {
		c.cache.SetAssignments(resp.Assignments)
	}
	return resp.Assignments, nil
}

func (c *controllerClient) ListRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	resp, err := c.call(ctx, controllerRPCRequest{Kind: controllerRPCListRuntimeViews})
	if err != nil {
		return nil, err
	}
	return resp.RuntimeViews, nil
}

func (c *controllerClient) Operator(ctx context.Context, op slotcontroller.OperatorRequest) error {
	_, err := c.call(ctx, controllerRPCRequest{
		Kind: controllerRPCOperator,
		Op:   &op,
	})
	return err
}

func (c *controllerClient) GetTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error) {
	resp, err := c.call(ctx, controllerRPCRequest{
		Kind:   controllerRPCGetTask,
		SlotID: slotID,
	})
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	if resp.NotFound || resp.Task == nil {
		return controllermeta.ReconcileTask{}, controllermeta.ErrNotFound
	}
	return *resp.Task, nil
}

func (c *controllerClient) ForceReconcile(ctx context.Context, slotID uint32) error {
	_, err := c.call(ctx, controllerRPCRequest{
		Kind:   controllerRPCForceReconcile,
		SlotID: slotID,
	})
	return err
}

func (c *controllerClient) ReportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error {
	advance := &controllerTaskAdvance{
		SlotID:  task.SlotID,
		Attempt: task.Attempt,
		Now:     time.Now(),
	}
	if taskErr != nil {
		advance.Err = taskErr.Error()
	}
	_, err := c.call(ctx, controllerRPCRequest{
		Kind:    controllerRPCTaskResult,
		Advance: advance,
	})
	return err
}

func (c *controllerClient) call(ctx context.Context, req controllerRPCRequest) (controllerRPCResponse, error) {
	if c == nil || c.cluster == nil {
		return controllerRPCResponse{}, ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
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

		// Give each peer probe its own budget so a slow stale leader does not
		// consume the entire controller retry window before we reach the current
		// leader.
		rpcCtx, cancel := withControllerTimeout(ctx)
		respBody, err := c.cluster.RPCService(rpcCtx, target, controllerRPCShardKey, rpcServiceController, body)
		cancel()
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
