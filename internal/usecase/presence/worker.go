package presence

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
)

func (a *App) HeartbeatOnce(ctx context.Context) error {
	groups := a.online.ActiveGroups()
	for _, group := range groups {
		if err := a.heartbeatGroup(ctx, group); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) heartbeatGroup(ctx context.Context, group online.GroupSnapshot) error {
	lease := GatewayLease{
		GroupID:        group.GroupID,
		GatewayNodeID:  a.localNodeID,
		GatewayBootID:  a.gatewayBootID,
		RouteCount:     group.Count,
		RouteDigest:    group.Digest,
		LeaseUntilUnix: a.now().Add(a.leaseTTL).Unix(),
	}

	result, err := a.authority.HeartbeatAuthoritative(ctx, HeartbeatAuthoritativeCommand{
		Lease: lease,
	})
	if err != nil {
		return err
	}
	if !result.Mismatch {
		return nil
	}

	routes := make([]Route, 0)
	for _, conn := range a.online.ActiveConnectionsByGroup(group.GroupID) {
		routes = append(routes, a.routeFromConn(conn))
	}
	return a.authority.ReplayAuthoritative(ctx, ReplayAuthoritativeCommand{
		Lease:  lease,
		Routes: routes,
	})
}
