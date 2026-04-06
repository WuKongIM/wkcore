package online

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"reflect"
	"sort"
	"sync"
)

type MemoryRegistry struct {
	mu        sync.RWMutex
	bySession map[uint64]OnlineConn
	byUID     map[string]map[uint64]OnlineConn
	byGroup   map[uint64]map[uint64]OnlineConn
}

func NewRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		bySession: make(map[uint64]OnlineConn),
		byUID:     make(map[string]map[uint64]OnlineConn),
		byGroup:   make(map[uint64]map[uint64]OnlineConn),
	}
}

func (r *MemoryRegistry) Register(conn OnlineConn) error {
	if r == nil {
		return nil
	}
	if conn.SessionID == 0 || conn.UID == "" || isNilSession(conn.Session) {
		return ErrInvalidConnection
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.bySession == nil {
		r.bySession = make(map[uint64]OnlineConn)
	}
	if r.byUID == nil {
		r.byUID = make(map[string]map[uint64]OnlineConn)
	}
	if r.byGroup == nil {
		r.byGroup = make(map[uint64]map[uint64]OnlineConn)
	}

	if existing, ok := r.bySession[conn.SessionID]; ok {
		r.removeActiveIndexes(existing)
	}

	if conn.State == 0 {
		conn.State = LocalRouteStateActive
	}
	r.bySession[conn.SessionID] = conn
	if _, ok := r.byGroup[conn.GroupID]; !ok {
		r.byGroup[conn.GroupID] = make(map[uint64]OnlineConn)
	}
	if conn.State != LocalRouteStateClosing {
		r.addActiveIndexes(conn)
	}
	return nil
}

func (r *MemoryRegistry) Unregister(sessionID uint64) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.bySession[sessionID]
	if !ok {
		return
	}

	delete(r.bySession, sessionID)
	r.removeActiveIndexes(conn)
}

func (r *MemoryRegistry) MarkClosing(sessionID uint64) (OnlineConn, bool) {
	if r == nil {
		return OnlineConn{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.bySession[sessionID]
	if !ok {
		return OnlineConn{}, false
	}

	if conn.State != LocalRouteStateClosing {
		r.removeActiveIndexes(conn)
		conn.State = LocalRouteStateClosing
		r.bySession[sessionID] = conn
	}
	return conn, true
}

func (r *MemoryRegistry) Connection(sessionID uint64) (OnlineConn, bool) {
	if r == nil {
		return OnlineConn{}, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	conn, ok := r.bySession[sessionID]
	return conn, ok
}

func (r *MemoryRegistry) ConnectionsByUID(uid string) []OnlineConn {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	conns := r.byUID[uid]
	if len(conns) == 0 {
		return nil
	}

	out := make([]OnlineConn, 0, len(conns))
	for _, conn := range conns {
		out = append(out, conn)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SessionID < out[j].SessionID
	})
	return out
}

func (r *MemoryRegistry) ActiveConnectionsByGroup(groupID uint64) []OnlineConn {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	conns := r.byGroup[groupID]
	if len(conns) == 0 {
		return nil
	}

	out := make([]OnlineConn, 0, len(conns))
	for _, conn := range conns {
		if conn.State != LocalRouteStateClosing {
			out = append(out, conn)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SessionID < out[j].SessionID
	})
	return out
}

func (r *MemoryRegistry) ActiveGroups() []GroupSnapshot {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]GroupSnapshot, 0, len(r.byGroup))
	for groupID, conns := range r.byGroup {
		snapshot := GroupSnapshot{GroupID: groupID}
		ordered := make([]OnlineConn, 0, len(conns))
		for _, conn := range conns {
			if conn.State != LocalRouteStateClosing {
				ordered = append(ordered, conn)
			}
		}
		sort.Slice(ordered, func(i, j int) bool {
			return ordered[i].SessionID < ordered[j].SessionID
		})
		snapshot.Count = len(ordered)
		snapshot.Digest = digestConnections(ordered)
		out = append(out, snapshot)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GroupID < out[j].GroupID
	})
	return out
}

func (r *MemoryRegistry) addActiveIndexes(conn OnlineConn) {
	if _, ok := r.byUID[conn.UID]; !ok {
		r.byUID[conn.UID] = make(map[uint64]OnlineConn)
	}
	r.byUID[conn.UID][conn.SessionID] = conn

	if _, ok := r.byGroup[conn.GroupID]; !ok {
		r.byGroup[conn.GroupID] = make(map[uint64]OnlineConn)
	}
	r.byGroup[conn.GroupID][conn.SessionID] = conn
}

func (r *MemoryRegistry) removeActiveIndexes(conn OnlineConn) {
	if sessions, ok := r.byUID[conn.UID]; ok {
		delete(sessions, conn.SessionID)
		if len(sessions) == 0 {
			delete(r.byUID, conn.UID)
		}
	}
	if sessions, ok := r.byGroup[conn.GroupID]; ok {
		delete(sessions, conn.SessionID)
	}
}

func digestConnections(conns []OnlineConn) uint64 {
	h := fnv.New64a()
	for _, conn := range conns {
		writeUint64(h, conn.SessionID)
		writeString(h, conn.UID)
		writeString(h, conn.DeviceID)
		writeUint64(h, uint64(conn.DeviceFlag))
		writeUint64(h, uint64(conn.DeviceLevel))
		writeString(h, conn.Listener)
	}
	return h.Sum64()
}

func writeUint64(h hash.Hash64, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
}

func writeString(h hash.Hash64, s string) {
	_, _ = h.Write([]byte(s))
	_, _ = h.Write([]byte{0})
}

func isNilSession(sess any) bool {
	if sess == nil {
		return true
	}

	rv := reflect.ValueOf(sess)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
