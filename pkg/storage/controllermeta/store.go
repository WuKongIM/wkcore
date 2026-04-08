package controllermeta

import (
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

type Store struct {
	db *pebble.DB
	mu sync.RWMutex
}

func Open(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) GetNode(ctx context.Context, nodeID uint64) (ClusterNode, error) {
	if nodeID == 0 {
		return ClusterNode{}, ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return ClusterNode{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, err := s.getValueLocked(encodeNodeKey(nodeID))
	if err != nil {
		return ClusterNode{}, err
	}
	return decodeClusterNode(encodeNodeKey(nodeID), value)
}

func (s *Store) DeleteNode(ctx context.Context, nodeID uint64) error {
	if nodeID == 0 {
		return ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteValueLocked(encodeNodeKey(nodeID))
}

func (s *Store) ListNodes(ctx context.Context) ([]ClusterNode, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listNodesLocked(ctx)
}

func (s *Store) UpsertNode(ctx context.Context, node ClusterNode) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if node.NodeID == 0 || node.Addr == "" || node.CapacityWeight < 0 || !validNodeStatus(node.Status) {
		return ErrInvalidArgument
	}
	node = normalizeClusterNode(node)

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeValueLocked(encodeNodeKey(node.NodeID), encodeClusterNode(node))
}

func (s *Store) GetAssignment(ctx context.Context, groupID uint32) (GroupAssignment, error) {
	if groupID == 0 {
		return GroupAssignment{}, ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return GroupAssignment{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := encodeGroupKey(recordPrefixAssignment, groupID)
	value, err := s.getValueLocked(key)
	if err != nil {
		return GroupAssignment{}, err
	}
	return decodeGroupAssignment(key, value)
}

func (s *Store) DeleteAssignment(ctx context.Context, groupID uint32) error {
	if groupID == 0 {
		return ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteValueLocked(encodeGroupKey(recordPrefixAssignment, groupID))
}

func (s *Store) ListAssignments(ctx context.Context) ([]GroupAssignment, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listAssignmentsLocked(ctx)
}

func (s *Store) UpsertAssignment(ctx context.Context, assignment GroupAssignment) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if assignment.GroupID == 0 {
		return ErrInvalidArgument
	}
	assignment = normalizeGroupAssignment(assignment)
	if err := validateRequiredPeerSet(assignment.DesiredPeers, ErrInvalidArgument); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeValueLocked(
		encodeGroupKey(recordPrefixAssignment, assignment.GroupID),
		encodeGroupAssignment(assignment),
	)
}

func (s *Store) GetRuntimeView(ctx context.Context, groupID uint32) (GroupRuntimeView, error) {
	if groupID == 0 {
		return GroupRuntimeView{}, ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return GroupRuntimeView{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := encodeGroupKey(recordPrefixRuntimeView, groupID)
	value, err := s.getValueLocked(key)
	if err != nil {
		return GroupRuntimeView{}, err
	}
	return decodeGroupRuntimeView(key, value)
}

func (s *Store) GetControllerMembership(ctx context.Context) (ControllerMembership, error) {
	if err := s.checkContext(ctx); err != nil {
		return ControllerMembership{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, err := s.getValueLocked(membershipKey())
	if err != nil {
		return ControllerMembership{}, err
	}
	return decodeControllerMembership(value)
}

func (s *Store) DeleteControllerMembership(ctx context.Context) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteValueLocked(membershipKey())
}

func (s *Store) ListRuntimeViews(ctx context.Context) ([]GroupRuntimeView, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listRuntimeViewsLocked(ctx)
}

func (s *Store) DeleteRuntimeView(ctx context.Context, groupID uint32) error {
	if groupID == 0 {
		return ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteValueLocked(encodeGroupKey(recordPrefixRuntimeView, groupID))
}

func (s *Store) UpsertControllerMembership(ctx context.Context, membership ControllerMembership) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	membership.Peers = normalizeUint64Set(membership.Peers)
	if err := validateRequiredPeerSet(membership.Peers, ErrInvalidArgument); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeValueLocked(membershipKey(), encodeControllerMembership(membership))
}

func (s *Store) UpsertRuntimeView(ctx context.Context, view GroupRuntimeView) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if view.GroupID == 0 {
		return ErrInvalidArgument
	}
	view = normalizeGroupRuntimeView(view)
	if err := validateRequiredPeerSet(view.CurrentPeers, ErrInvalidArgument); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeValueLocked(
		encodeGroupKey(recordPrefixRuntimeView, view.GroupID),
		encodeGroupRuntimeView(view),
	)
}

func (s *Store) GetTask(ctx context.Context, groupID uint32) (ReconcileTask, error) {
	if groupID == 0 {
		return ReconcileTask{}, ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return ReconcileTask{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := encodeGroupKey(recordPrefixTask, groupID)
	value, err := s.getValueLocked(key)
	if err != nil {
		return ReconcileTask{}, err
	}
	return decodeReconcileTask(key, value)
}

func (s *Store) DeleteTask(ctx context.Context, groupID uint32) error {
	if groupID == 0 {
		return ErrInvalidArgument
	}
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteValueLocked(encodeGroupKey(recordPrefixTask, groupID))
}

func (s *Store) ListTasks(ctx context.Context) ([]ReconcileTask, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listTasksLocked(ctx)
}

func (s *Store) UpsertTask(ctx context.Context, task ReconcileTask) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if task.GroupID == 0 || !validTaskKind(task.Kind) || !validTaskStep(task.Step) {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeValueLocked(
		encodeGroupKey(recordPrefixTask, task.GroupID),
		encodeReconcileTask(task),
	)
}

func (s *Store) listNodesLocked(ctx context.Context) ([]ClusterNode, error) {
	return listRecords(ctx, s.db, recordPrefixNode, decodeClusterNode)
}

func (s *Store) listAssignmentsLocked(ctx context.Context) ([]GroupAssignment, error) {
	return listRecords(ctx, s.db, recordPrefixAssignment, decodeGroupAssignment)
}

func (s *Store) listRuntimeViewsLocked(ctx context.Context) ([]GroupRuntimeView, error) {
	return listRecords(ctx, s.db, recordPrefixRuntimeView, decodeGroupRuntimeView)
}

func (s *Store) listTasksLocked(ctx context.Context) ([]ReconcileTask, error) {
	return listRecords(ctx, s.db, recordPrefixTask, decodeReconcileTask)
}

func listRecords[T any](ctx context.Context, db *pebble.DB, prefix byte, decode func(key, value []byte) (T, error)) ([]T, error) {
	lowerBound, upperBound := prefixBounds(prefix)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	out := make([]T, 0, 16)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		record, err := decode(iter.Key(), value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) getValueLocked(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()

	return append([]byte(nil), value...), nil
}

func (s *Store) deleteValueLocked(key []byte) error {
	_, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	closer.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(key, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) writeValueLocked(key, value []byte) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) checkContext(ctx context.Context) error {
	return checkContext(ctx)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
