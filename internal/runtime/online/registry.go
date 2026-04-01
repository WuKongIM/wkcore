package online

import (
	"reflect"
	"sort"
	"sync"
)

type MemoryRegistry struct {
	mu        sync.RWMutex
	bySession map[uint64]OnlineConn
	byUID     map[string]map[uint64]OnlineConn
}

func NewRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		bySession: make(map[uint64]OnlineConn),
		byUID:     make(map[string]map[uint64]OnlineConn),
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

	if existing, ok := r.bySession[conn.SessionID]; ok {
		if sessions, ok := r.byUID[existing.UID]; ok {
			delete(sessions, existing.SessionID)
			if len(sessions) == 0 {
				delete(r.byUID, existing.UID)
			}
		}
	}

	r.bySession[conn.SessionID] = conn
	if _, ok := r.byUID[conn.UID]; !ok {
		r.byUID[conn.UID] = make(map[uint64]OnlineConn)
	}
	r.byUID[conn.UID][conn.SessionID] = conn
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

	if sessions, ok := r.byUID[conn.UID]; ok {
		delete(sessions, sessionID)
		if len(sessions) == 0 {
			delete(r.byUID, conn.UID)
		}
	}
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
