package service

import (
	"reflect"
	"sort"
	"sync"
)

type SessionRegistry interface {
	Register(meta SessionMeta) error
	Unregister(sessionID uint64)
	Session(sessionID uint64) (SessionMeta, bool)
	SessionsByUID(uid string) []SessionMeta
}

type Registry struct {
	mu        sync.RWMutex
	bySession map[uint64]SessionMeta
	byUID     map[string]map[uint64]SessionMeta
}

func NewRegistry() *Registry {
	return &Registry{
		bySession: make(map[uint64]SessionMeta),
		byUID:     make(map[string]map[uint64]SessionMeta),
	}
}

func (r *Registry) Register(meta SessionMeta) error {
	if r == nil {
		return nil
	}
	if meta.SessionID == 0 || meta.UID == "" || isNilSession(meta.Session) {
		return ErrUnauthenticatedSession
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.bySession == nil {
		r.bySession = make(map[uint64]SessionMeta)
	}
	if r.byUID == nil {
		r.byUID = make(map[string]map[uint64]SessionMeta)
	}

	if existing, ok := r.bySession[meta.SessionID]; ok {
		if sessions, ok := r.byUID[existing.UID]; ok {
			delete(sessions, existing.SessionID)
			if len(sessions) == 0 {
				delete(r.byUID, existing.UID)
			}
		}
	}

	r.bySession[meta.SessionID] = meta
	if _, ok := r.byUID[meta.UID]; !ok {
		r.byUID[meta.UID] = make(map[uint64]SessionMeta)
	}
	r.byUID[meta.UID][meta.SessionID] = meta
	return nil
}

func (r *Registry) Unregister(sessionID uint64) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	meta, ok := r.bySession[sessionID]
	if !ok {
		return
	}

	delete(r.bySession, sessionID)

	if sessions, ok := r.byUID[meta.UID]; ok {
		delete(sessions, sessionID)
		if len(sessions) == 0 {
			delete(r.byUID, meta.UID)
		}
	}
}

func (r *Registry) Session(sessionID uint64) (SessionMeta, bool) {
	if r == nil {
		return SessionMeta{}, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, ok := r.bySession[sessionID]
	return meta, ok
}

func (r *Registry) SessionsByUID(uid string) []SessionMeta {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	sessions := r.byUID[uid]
	if len(sessions) == 0 {
		return nil
	}

	metas := make([]SessionMeta, 0, len(sessions))
	for _, meta := range sessions {
		metas = append(metas, meta)
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].SessionID < metas[j].SessionID
	})
	return metas
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
