package app

import (
	"context"

	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

type presenceAuthorityClient struct {
	cluster     *raftcluster.Cluster
	local       *presence.App
	remote      *accessnode.Client
	localNodeID uint64
}

func (c *presenceAuthorityClient) RegisterAuthoritative(ctx context.Context, cmd presence.RegisterAuthoritativeCommand) (presence.RegisterAuthoritativeResult, error) {
	if c.shouldUseLocalLeader(cmd.GroupID) {
		return c.local.RegisterAuthoritative(ctx, cmd)
	}
	return c.remote.RegisterAuthoritative(ctx, cmd)
}

func (c *presenceAuthorityClient) UnregisterAuthoritative(ctx context.Context, cmd presence.UnregisterAuthoritativeCommand) error {
	if c.shouldUseLocalLeader(cmd.GroupID) {
		return c.local.UnregisterAuthoritative(ctx, cmd)
	}
	return c.remote.UnregisterAuthoritative(ctx, cmd)
}

func (c *presenceAuthorityClient) HeartbeatAuthoritative(ctx context.Context, cmd presence.HeartbeatAuthoritativeCommand) (presence.HeartbeatAuthoritativeResult, error) {
	if c.shouldUseLocalLeader(cmd.Lease.GroupID) {
		return c.local.HeartbeatAuthoritative(ctx, cmd)
	}
	return c.remote.HeartbeatAuthoritative(ctx, cmd)
}

func (c *presenceAuthorityClient) ReplayAuthoritative(ctx context.Context, cmd presence.ReplayAuthoritativeCommand) error {
	if c.shouldUseLocalLeader(cmd.Lease.GroupID) {
		return c.local.ReplayAuthoritative(ctx, cmd)
	}
	return c.remote.ReplayAuthoritative(ctx, cmd)
}

func (c *presenceAuthorityClient) EndpointsByUID(ctx context.Context, uid string) ([]presence.Route, error) {
	if c.cluster == nil || c.remote == nil {
		if c.local == nil {
			return nil, nil
		}
		return c.local.EndpointsByUID(ctx, uid)
	}
	groupID := c.cluster.SlotForKey(uid)
	if leaderID, err := c.cluster.LeaderOf(groupID); err == nil && c.cluster.IsLocal(leaderID) {
		return c.local.EndpointsByUID(ctx, uid)
	}
	return c.remote.EndpointsByUID(ctx, uid)
}

func (c *presenceAuthorityClient) ApplyRouteAction(ctx context.Context, action presence.RouteAction) error {
	if c.local != nil && (action.NodeID == 0 || action.NodeID == c.localNodeID) {
		return c.local.ApplyRouteAction(ctx, action)
	}
	return c.remote.ApplyRouteAction(ctx, action)
}

func (c *presenceAuthorityClient) shouldUseLocalLeader(groupID uint64) bool {
	if c.local == nil {
		return false
	}
	if c.cluster == nil {
		return true
	}
	leaderID, err := c.cluster.LeaderOf(multiraft.GroupID(groupID))
	return err == nil && c.cluster.IsLocal(leaderID)
}
