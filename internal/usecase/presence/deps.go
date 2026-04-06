package presence

import "context"

type authoritativeDirectory interface {
	register(groupID uint64, route Route) []RouteAction
	unregister(groupID uint64, route Route)
	heartbeat(lease GatewayLease) HeartbeatAuthoritativeResult
	replay(lease GatewayLease, routes []Route)
	endpointsByUID(uid string) []Route
}

type Authoritative interface {
	RegisterAuthoritative(ctx context.Context, cmd RegisterAuthoritativeCommand) (RegisterAuthoritativeResult, error)
	UnregisterAuthoritative(ctx context.Context, cmd UnregisterAuthoritativeCommand) error
	HeartbeatAuthoritative(ctx context.Context, cmd HeartbeatAuthoritativeCommand) (HeartbeatAuthoritativeResult, error)
	ReplayAuthoritative(ctx context.Context, cmd ReplayAuthoritativeCommand) error
	EndpointsByUID(ctx context.Context, uid string) []Route
}
