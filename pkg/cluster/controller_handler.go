package cluster

import (
	"context"
	"errors"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
)

type controllerHandler struct {
	cluster *Cluster
}

func (h *controllerHandler) Handle(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeControllerRequest(body)
	if err != nil {
		return nil, err
	}
	if h == nil || h.cluster == nil || h.cluster.controller == nil || h.cluster.controllerMeta == nil {
		return nil, ErrNotStarted
	}

	c := h.cluster
	marshalRedirect := func() ([]byte, error) {
		return encodeControllerResponse(req.Kind, controllerRPCResponse{
			NotLeader: true,
			LeaderID:  c.controller.LeaderID(),
		})
	}

	switch req.Kind {
	case controllerRPCHeartbeat:
		if req.Report == nil {
			return nil, ErrInvalidConfig
		}
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		proposeCtx, cancel := c.withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind:   slotcontroller.CommandKindNodeHeartbeat,
			Report: req.Report,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{})
	case controllerRPCTaskResult:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if req.Advance == nil {
			return nil, ErrInvalidConfig
		}
		advance := &slotcontroller.TaskAdvance{
			SlotID:  req.Advance.SlotID,
			Attempt: req.Advance.Attempt,
			Now:     req.Advance.Now,
		}
		if req.Advance.Err != "" {
			advance.Err = errors.New(req.Advance.Err)
		}
		proposeCtx, cancel := c.withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind:    slotcontroller.CommandKindTaskResult,
			Advance: advance,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{})
	case controllerRPCListAssignments:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		assignments, err := c.controllerMeta.ListAssignments(ctx)
		if err != nil {
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{Assignments: assignments})
	case controllerRPCListNodes:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		nodes, err := c.controllerMeta.ListNodes(ctx)
		if err != nil {
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{Nodes: nodes})
	case controllerRPCListRuntimeViews:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		views, err := c.controllerMeta.ListRuntimeViews(ctx)
		if err != nil {
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{RuntimeViews: views})
	case controllerRPCOperator:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if req.Op == nil {
			return nil, ErrInvalidConfig
		}
		proposeCtx, cancel := c.withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind: slotcontroller.CommandKindOperatorRequest,
			Op:   req.Op,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{})
	case controllerRPCGetTask:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		task, err := c.controllerMeta.GetTask(ctx, req.SlotID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			return encodeControllerResponse(req.Kind, controllerRPCResponse{NotFound: true})
		}
		if err != nil {
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{Task: &task})
	case controllerRPCForceReconcile:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if err := c.forceReconcileOnLeader(ctx, req.SlotID); err != nil {
			if errors.Is(err, ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{})
	default:
		return nil, ErrInvalidConfig
	}
}
