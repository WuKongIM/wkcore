package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

const (
	rpcStatusOK        = "ok"
	rpcStatusNotLeader = "not_leader"
	rpcStatusNoLeader  = "no_leader"
	rpcStatusNoGroup   = "no_group"

	presenceOpRegister    = "register"
	presenceOpUnregister  = "unregister"
	presenceOpHeartbeat   = "heartbeat"
	presenceOpReplay      = "replay"
	presenceOpEndpoints   = "endpoints"
	presenceOpApplyAction = "apply_action"
)

type presenceRPCRequest struct {
	Op      string                 `json:"op"`
	GroupID uint64                 `json:"group_id,omitempty"`
	UID     string                 `json:"uid,omitempty"`
	Route   *presence.Route        `json:"route,omitempty"`
	Routes  []presence.Route       `json:"routes,omitempty"`
	Action  *presence.RouteAction  `json:"action,omitempty"`
	Lease   *presence.GatewayLease `json:"lease,omitempty"`
}

type presenceRPCResponse struct {
	Status    string                                 `json:"status"`
	LeaderID  uint64                                 `json:"leader_id,omitempty"`
	Register  *presence.RegisterAuthoritativeResult  `json:"register,omitempty"`
	Heartbeat *presence.HeartbeatAuthoritativeResult `json:"heartbeat,omitempty"`
	Endpoints []presence.Route                       `json:"endpoints,omitempty"`
}

func (r presenceRPCResponse) rpcStatus() string {
	return r.Status
}

func (r presenceRPCResponse) rpcLeaderID() uint64 {
	return r.LeaderID
}

func (a *Adapter) handlePresenceRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req presenceRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	switch req.Op {
	case presenceOpRegister:
		return a.handleRegister(ctx, req)
	case presenceOpUnregister:
		return a.handleUnregister(ctx, req)
	case presenceOpHeartbeat:
		return a.handleHeartbeat(ctx, req)
	case presenceOpReplay:
		return a.handleReplay(ctx, req)
	case presenceOpEndpoints:
		return a.handleEndpoints(ctx, req)
	case presenceOpApplyAction:
		return a.handleApplyAction(ctx, req)
	default:
		return nil, fmt.Errorf("access/node: unknown presence op %q", req.Op)
	}
}

func (a *Adapter) handleRegister(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	if body, handled, err := a.handleAuthoritativeRPC(multiraft.GroupID(req.GroupID)); handled || err != nil {
		return body, err
	}
	result, err := a.presence.RegisterAuthoritative(ctx, presence.RegisterAuthoritativeCommand{
		GroupID: req.GroupID,
		Route:   derefRoute(req.Route),
	})
	if err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{
		Status:   rpcStatusOK,
		Register: &result,
	})
}

func (a *Adapter) handleUnregister(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	if body, handled, err := a.handleAuthoritativeRPC(multiraft.GroupID(req.GroupID)); handled || err != nil {
		return body, err
	}
	err := a.presence.UnregisterAuthoritative(ctx, presence.UnregisterAuthoritativeCommand{
		GroupID: req.GroupID,
		Route:   derefRoute(req.Route),
	})
	if err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{Status: rpcStatusOK})
}

func (a *Adapter) handleHeartbeat(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	lease := derefLease(req.Lease)
	if body, handled, err := a.handleAuthoritativeRPC(multiraft.GroupID(lease.GroupID)); handled || err != nil {
		return body, err
	}
	result, err := a.presence.HeartbeatAuthoritative(ctx, presence.HeartbeatAuthoritativeCommand{
		Lease: lease,
	})
	if err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{
		Status:    rpcStatusOK,
		Heartbeat: &result,
	})
}

func (a *Adapter) handleReplay(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	lease := derefLease(req.Lease)
	if body, handled, err := a.handleAuthoritativeRPC(multiraft.GroupID(lease.GroupID)); handled || err != nil {
		return body, err
	}
	err := a.presence.ReplayAuthoritative(ctx, presence.ReplayAuthoritativeCommand{
		Lease:  lease,
		Routes: append([]presence.Route(nil), req.Routes...),
	})
	if err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{Status: rpcStatusOK})
}

func (a *Adapter) handleEndpoints(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	if body, handled, err := a.handleAuthoritativeRPC(multiraft.GroupID(req.GroupID)); handled || err != nil {
		return body, err
	}
	endpoints, err := a.presence.EndpointsByUID(ctx, req.UID)
	if err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{
		Status:    rpcStatusOK,
		Endpoints: endpoints,
	})
}

func (a *Adapter) handleApplyAction(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
	if err := a.presence.ApplyRouteAction(ctx, derefAction(req.Action)); err != nil {
		return nil, err
	}
	return encodePresenceResponse(presenceRPCResponse{Status: rpcStatusOK})
}

func (a *Adapter) handleAuthoritativeRPC(groupID multiraft.GroupID) ([]byte, bool, error) {
	if a.cluster == nil || groupID == 0 {
		return nil, false, nil
	}
	leaderID, err := a.cluster.LeaderOf(groupID)
	switch {
	case errors.Is(err, raftcluster.ErrGroupNotFound):
		body, encodeErr := encodePresenceResponse(presenceRPCResponse{Status: rpcStatusNoGroup})
		return body, true, encodeErr
	case err != nil:
		body, encodeErr := encodePresenceResponse(presenceRPCResponse{Status: rpcStatusNoLeader})
		return body, true, encodeErr
	case !a.cluster.IsLocal(leaderID):
		body, encodeErr := encodePresenceResponse(presenceRPCResponse{
			Status:   rpcStatusNotLeader,
			LeaderID: uint64(leaderID),
		})
		return body, true, encodeErr
	default:
		return nil, false, nil
	}
}

func encodePresenceResponse(resp presenceRPCResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func derefRoute(route *presence.Route) presence.Route {
	if route == nil {
		return presence.Route{}
	}
	return *route
}

func derefAction(action *presence.RouteAction) presence.RouteAction {
	if action == nil {
		return presence.RouteAction{}
	}
	return *action
}

func derefLease(lease *presence.GatewayLease) presence.GatewayLease {
	if lease == nil {
		return presence.GatewayLease{}
	}
	return *lease
}
