package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

type authoritativeRPCResponse interface {
	rpcStatus() string
	rpcLeaderID() uint64
}

type DeliveryCommand struct {
	NodeID     uint64
	GroupID    uint64
	UID        string
	BootID     uint64
	SessionIDs []uint64
	Frame      wkframe.Frame
}

func (c *Client) RegisterAuthoritative(ctx context.Context, cmd presence.RegisterAuthoritativeCommand) (presence.RegisterAuthoritativeResult, error) {
	resp, err := c.callPresenceAuthoritative(ctx, multiraft.GroupID(cmd.GroupID), presenceRPCRequest{
		Op:      presenceOpRegister,
		GroupID: cmd.GroupID,
		Route:   &cmd.Route,
	})
	if err != nil {
		return presence.RegisterAuthoritativeResult{}, err
	}
	if resp.Register == nil {
		return presence.RegisterAuthoritativeResult{}, nil
	}
	return *resp.Register, nil
}

func (c *Client) UnregisterAuthoritative(ctx context.Context, cmd presence.UnregisterAuthoritativeCommand) error {
	_, err := c.callPresenceAuthoritative(ctx, multiraft.GroupID(cmd.GroupID), presenceRPCRequest{
		Op:      presenceOpUnregister,
		GroupID: cmd.GroupID,
		Route:   &cmd.Route,
	})
	return err
}

func (c *Client) HeartbeatAuthoritative(ctx context.Context, cmd presence.HeartbeatAuthoritativeCommand) (presence.HeartbeatAuthoritativeResult, error) {
	resp, err := c.callPresenceAuthoritative(ctx, multiraft.GroupID(cmd.Lease.GroupID), presenceRPCRequest{
		Op:    presenceOpHeartbeat,
		Lease: &cmd.Lease,
	})
	if err != nil {
		return presence.HeartbeatAuthoritativeResult{}, err
	}
	if resp.Heartbeat == nil {
		return presence.HeartbeatAuthoritativeResult{}, nil
	}
	return *resp.Heartbeat, nil
}

func (c *Client) ReplayAuthoritative(ctx context.Context, cmd presence.ReplayAuthoritativeCommand) error {
	_, err := c.callPresenceAuthoritative(ctx, multiraft.GroupID(cmd.Lease.GroupID), presenceRPCRequest{
		Op:     presenceOpReplay,
		Lease:  &cmd.Lease,
		Routes: cmd.Routes,
	})
	return err
}

func (c *Client) EndpointsByUID(ctx context.Context, uid string) []presence.Route {
	groupID := c.cluster.SlotForKey(uid)
	resp, err := c.callPresenceAuthoritative(ctx, groupID, presenceRPCRequest{
		Op:      presenceOpEndpoints,
		GroupID: uint64(groupID),
		UID:     uid,
	})
	if err != nil {
		return nil
	}
	return resp.Endpoints
}

func (c *Client) ApplyRouteAction(ctx context.Context, action presence.RouteAction) error {
	resp, err := c.callPresenceDirect(ctx, multiraft.NodeID(action.NodeID), 0, presenceRPCRequest{
		Op:     presenceOpApplyAction,
		Action: &action,
	})
	if err != nil {
		return err
	}
	if resp.Status != rpcStatusOK {
		return fmt.Errorf("access/node: unexpected presence status %q", resp.Status)
	}
	return nil
}

func (c *Client) Deliver(ctx context.Context, cmd DeliveryCommand) error {
	frameBytes, err := c.codec.EncodeFrame(cmd.Frame, wkframe.LatestVersion)
	if err != nil {
		return err
	}
	body, err := json.Marshal(deliveryRequest{
		UID:        cmd.UID,
		GroupID:    cmd.GroupID,
		BootID:     cmd.BootID,
		SessionIDs: append([]uint64(nil), cmd.SessionIDs...),
		Frame:      frameBytes,
	})
	if err != nil {
		return err
	}
	respBody, err := c.cluster.RPCService(ctx, multiraft.NodeID(cmd.NodeID), multiraft.GroupID(cmd.GroupID), deliveryRPCServiceID, body)
	if err != nil {
		return err
	}
	resp, err := decodeDeliveryResponse(respBody)
	if err != nil {
		return err
	}
	if resp.Status != rpcStatusOK {
		return fmt.Errorf("access/node: unexpected delivery status %q", resp.Status)
	}
	return nil
}

func (c *Client) callPresenceAuthoritative(ctx context.Context, groupID multiraft.GroupID, req presenceRPCRequest) (presenceRPCResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return presenceRPCResponse{}, err
	}
	return callAuthoritativeRPC(ctx, c, groupID, presenceRPCServiceID, body, decodePresenceResponse)
}

func (c *Client) callPresenceDirect(ctx context.Context, nodeID multiraft.NodeID, groupID multiraft.GroupID, req presenceRPCRequest) (presenceRPCResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return presenceRPCResponse{}, err
	}
	respBody, err := c.cluster.RPCService(ctx, nodeID, groupID, presenceRPCServiceID, body)
	if err != nil {
		return presenceRPCResponse{}, err
	}
	return decodePresenceResponse(respBody)
}

func callAuthoritativeRPC[T authoritativeRPCResponse](
	ctx context.Context,
	c *Client,
	groupID multiraft.GroupID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
) (T, error) {
	var zero T

	if c.cluster == nil {
		return zero, fmt.Errorf("access/node: cluster not configured")
	}

	peers := c.cluster.PeersForGroup(groupID)
	if len(peers) == 0 {
		return zero, raftcluster.ErrGroupNotFound
	}

	tried := make(map[multiraft.NodeID]struct{}, len(peers))
	candidates := append([]multiraft.NodeID(nil), peers...)
	var lastErr error

	for len(candidates) > 0 {
		peer := candidates[0]
		candidates = candidates[1:]
		if _, ok := tried[peer]; ok {
			continue
		}
		tried[peer] = struct{}{}

		respBody, err := c.cluster.RPCService(ctx, peer, groupID, serviceID, payload)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := decode(respBody)
		if err != nil {
			lastErr = err
			continue
		}

		switch resp.rpcStatus() {
		case rpcStatusOK:
			return resp, nil
		case rpcStatusNotLeader:
			if leaderID := multiraft.NodeID(resp.rpcLeaderID()); leaderID != 0 {
				if _, ok := tried[leaderID]; !ok {
					candidates = append([]multiraft.NodeID{leaderID}, candidates...)
				}
				continue
			}
		case rpcStatusNoLeader:
			lastErr = raftcluster.ErrNoLeader
			continue
		case rpcStatusNoGroup:
			lastErr = raftcluster.ErrGroupNotFound
			continue
		default:
			lastErr = fmt.Errorf("access/node: unexpected rpc status %q", resp.rpcStatus())
			continue
		}
	}

	if lastErr != nil {
		return zero, lastErr
	}
	return zero, raftcluster.ErrNoLeader
}

func decodePresenceResponse(body []byte) (presenceRPCResponse, error) {
	var resp presenceRPCResponse
	err := json.Unmarshal(body, &resp)
	return resp, err
}

func decodeDeliveryResponse(body []byte) (deliveryResponse, error) {
	var resp deliveryResponse
	err := json.Unmarshal(body, &resp)
	return resp, err
}

var (
	_ presence.Authoritative    = (*Client)(nil)
	_ presence.ActionDispatcher = (*Client)(nil)
)
