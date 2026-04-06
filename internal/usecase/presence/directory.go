package presence

import (
	"hash"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type routeKey struct {
	nodeID    uint64
	bootID    uint64
	sessionID uint64
}

type leaseKey struct {
	groupID       uint64
	gatewayNodeID uint64
	gatewayBootID uint64
}

type directory struct {
	mu       sync.RWMutex
	byUID    map[string]map[routeKey]Route
	leases   map[leaseKey]GatewayLease
	ownerSet map[leaseKey]map[routeKey]Route
}

func newDirectory() *directory {
	return &directory{
		byUID:    make(map[string]map[routeKey]Route),
		leases:   make(map[leaseKey]GatewayLease),
		ownerSet: make(map[leaseKey]map[routeKey]Route),
	}
}

func (d *directory) register(groupID uint64, route Route) []RouteAction {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := leaseKey{groupID: groupID, gatewayNodeID: route.NodeID, gatewayBootID: route.BootID}
	if d.byUID[route.UID] == nil {
		d.byUID[route.UID] = make(map[routeKey]Route)
	}
	if d.ownerSet[k] == nil {
		d.ownerSet[k] = make(map[routeKey]Route)
	}

	actions := make([]RouteAction, 0)
	for existingKey, existing := range d.byUID[route.UID] {
		if !conflicts(route, existing) {
			continue
		}
		delete(d.byUID[route.UID], existingKey)
		deleteOwnerRoute(d.ownerSet, groupID, existing)
		actions = append(actions, actionForReplacement(route, existing))
	}

	rk := makeRouteKey(route)
	d.byUID[route.UID][rk] = route
	d.ownerSet[k][rk] = route
	d.leases[k] = GatewayLease{
		GroupID:        groupID,
		GatewayNodeID:  route.NodeID,
		GatewayBootID:  route.BootID,
		RouteCount:     len(d.ownerSet[k]),
		RouteDigest:    digestRoutes(d.ownerSet[k]),
		LeaseUntilUnix: d.leases[k].LeaseUntilUnix,
	}

	return actions
}

func (d *directory) unregister(groupID uint64, route Route) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rk := makeRouteKey(route)
	if routes := d.byUID[route.UID]; routes != nil {
		delete(routes, rk)
		if len(routes) == 0 {
			delete(d.byUID, route.UID)
		}
	}
	deleteOwnerRoute(d.ownerSet, groupID, route)
}

func (d *directory) heartbeat(lease GatewayLease) HeartbeatAuthoritativeResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := makeLeaseKey(lease)
	current := d.ownerSet[k]
	count := len(current)
	digest := digestRoutes(current)
	mismatch := count != lease.RouteCount || digest != lease.RouteDigest

	lease.RouteCount = count
	lease.RouteDigest = digest
	d.leases[k] = lease

	return HeartbeatAuthoritativeResult{
		RouteCount:  count,
		RouteDigest: digest,
		Mismatch:    mismatch,
	}
}

func (d *directory) replay(lease GatewayLease, routes []Route) {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := makeLeaseKey(lease)
	if prev := d.ownerSet[k]; prev != nil {
		for routeKey, route := range prev {
			if byUID := d.byUID[route.UID]; byUID != nil {
				delete(byUID, routeKey)
				if len(byUID) == 0 {
					delete(d.byUID, route.UID)
				}
			}
		}
	}

	nextOwnerSet := make(map[routeKey]Route, len(routes))
	for _, route := range routes {
		rk := makeRouteKey(route)
		nextOwnerSet[rk] = route
		if d.byUID[route.UID] == nil {
			d.byUID[route.UID] = make(map[routeKey]Route)
		}
		d.byUID[route.UID][rk] = route
	}

	d.ownerSet[k] = nextOwnerSet
	lease.RouteCount = len(nextOwnerSet)
	lease.RouteDigest = digestRoutes(nextOwnerSet)
	d.leases[k] = lease
}

func (d *directory) endpointsByUID(uid string) []Route {
	d.mu.RLock()
	defer d.mu.RUnlock()

	routes := d.byUID[uid]
	if len(routes) == 0 {
		return nil
	}

	out := make([]Route, 0, len(routes))
	for _, route := range routes {
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SessionID != out[j].SessionID {
			return out[i].SessionID < out[j].SessionID
		}
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].BootID < out[j].BootID
	})
	return out
}

func conflicts(incoming, existing Route) bool {
	if incoming.UID != existing.UID || incoming.DeviceFlag != existing.DeviceFlag {
		return false
	}
	switch incoming.DeviceLevel {
	case uint8(wkframe.DeviceLevelMaster):
		return true
	case uint8(wkframe.DeviceLevelSlave):
		return incoming.DeviceID == existing.DeviceID
	default:
		return false
	}
}

func actionForReplacement(incoming, existing Route) RouteAction {
	kind := "close"
	if incoming.DeviceLevel == uint8(wkframe.DeviceLevelMaster) && incoming.DeviceID != existing.DeviceID {
		kind = "kick_then_close"
	}
	return RouteAction{
		UID:       existing.UID,
		NodeID:    existing.NodeID,
		BootID:    existing.BootID,
		SessionID: existing.SessionID,
		Kind:      kind,
	}
}

func makeRouteKey(route Route) routeKey {
	return routeKey{
		nodeID:    route.NodeID,
		bootID:    route.BootID,
		sessionID: route.SessionID,
	}
}

func makeLeaseKey(lease GatewayLease) leaseKey {
	return leaseKey{
		groupID:       lease.GroupID,
		gatewayNodeID: lease.GatewayNodeID,
		gatewayBootID: lease.GatewayBootID,
	}
}

func deleteOwnerRoute(ownerSet map[leaseKey]map[routeKey]Route, groupID uint64, route Route) {
	k := leaseKey{groupID: groupID, gatewayNodeID: route.NodeID, gatewayBootID: route.BootID}
	routes := ownerSet[k]
	if routes == nil {
		return
	}
	delete(routes, makeRouteKey(route))
	if len(routes) == 0 {
		delete(ownerSet, k)
		return
	}
}

func digestRoutes(routes map[routeKey]Route) uint64 {
	if len(routes) == 0 {
		return 0
	}

	keys := make([]routeKey, 0, len(routes))
	for key := range routes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].nodeID != keys[j].nodeID {
			return keys[i].nodeID < keys[j].nodeID
		}
		if keys[i].bootID != keys[j].bootID {
			return keys[i].bootID < keys[j].bootID
		}
		return keys[i].sessionID < keys[j].sessionID
	})

	h := fnv.New64a()
	for _, key := range keys {
		route := routes[key]
		writeUint64(h, route.NodeID)
		writeUint64(h, route.BootID)
		writeUint64(h, route.SessionID)
		_, _ = h.Write([]byte(route.UID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(route.DeviceID))
		_, _ = h.Write([]byte{route.DeviceFlag, route.DeviceLevel, 0})
		_, _ = h.Write([]byte(route.Listener))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

func writeUint64(h hash.Hash64, value uint64) {
	var buf [8]byte
	buf[0] = byte(value)
	buf[1] = byte(value >> 8)
	buf[2] = byte(value >> 16)
	buf[3] = byte(value >> 24)
	buf[4] = byte(value >> 32)
	buf[5] = byte(value >> 40)
	buf[6] = byte(value >> 48)
	buf[7] = byte(value >> 56)
	_, _ = h.Write(buf[:])
}
