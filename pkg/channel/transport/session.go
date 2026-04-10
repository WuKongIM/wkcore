package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
)

const defaultFetchBatchFlushWindow = 200 * time.Microsecond

const maxQueuedFetchRequestsPerPendingRPC = 32

type sessionManager struct {
	adapter *Adapter

	mu       sync.Mutex
	sessions map[isr.NodeID]*peerSession
}

type peerSession struct {
	adapter *Adapter
	peer    isr.NodeID

	mu              sync.Mutex
	pendingRequests int
	pendingBytes    int64
	closed          bool

	batchFlushScheduled bool
	batchQueuedBytes    int64
	batchFetchQueue     []queuedFetchRequest
}

type queuedFetchRequest struct {
	env   isrnode.Envelope
	bytes int64
}

var _ isrnode.PeerSessionManager = (*sessionManager)(nil)
var _ isrnode.PeerSession = (*peerSession)(nil)

func newSessionManager(adapter *Adapter) *sessionManager {
	return &sessionManager{
		adapter:  adapter,
		sessions: make(map[isr.NodeID]*peerSession),
	}
}

func (m *sessionManager) Session(peer isr.NodeID) isrnode.PeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, ok := m.sessions[peer]; ok {
		return session
	}
	session := &peerSession{adapter: m.adapter, peer: peer}
	m.sessions[peer] = session
	return session
}

func (s *peerSession) Send(env isrnode.Envelope) error {
	switch env.Kind {
	case isrnode.MessageKindFetchRequest:
		return s.sendFetchRequest(env)
	case isrnode.MessageKindProgressAck:
		return s.sendProgressAck(env)
	default:
		return fmt.Errorf("isrnodetransport: unsupported envelope kind %d", env.Kind)
	}
}

func (s *peerSession) TryBatch(env isrnode.Envelope) bool {
	if env.Kind != isrnode.MessageKindFetchRequest || env.FetchRequest == nil {
		return false
	}
	body, err := encodeFetchRequest(*env.FetchRequest)
	if err != nil {
		return false
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.batchFetchQueue = append(s.batchFetchQueue, queuedFetchRequest{
		env:   env,
		bytes: int64(len(body)),
	})
	s.batchQueuedBytes += int64(len(body))
	shouldSchedule := !s.batchFlushScheduled
	if shouldSchedule {
		s.batchFlushScheduled = true
	}
	s.mu.Unlock()

	if shouldSchedule {
		time.AfterFunc(defaultFetchBatchFlushWindow, func() {
			_ = s.Flush()
		})
	}
	return true
}

func (s *peerSession) Flush() error {
	batch := s.drainBatchQueue()
	if len(batch) == 0 {
		return nil
	}
	return s.flushBatch(batch)
}

func (s *peerSession) sendFetchRequest(env isrnode.Envelope) error {
	if env.FetchRequest == nil {
		return fmt.Errorf("isrnodetransport: missing fetch request payload")
	}

	body, err := encodeFetchRequest(*env.FetchRequest)
	if err != nil {
		return err
	}
	pendingBytes := int64(len(body))
	s.trackPending(pendingBytes)
	pendingReleased := false
	defer func() {
		if !pendingReleased {
			s.releasePending(pendingBytes)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), s.adapter.rpcTimeout)
	defer cancel()

	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey(env.GroupKey), RPCServiceFetch, body)
	if err != nil {
		return err
	}
	resp, err := decodeFetchResponse(respBody)
	if err != nil {
		return err
	}
	s.releasePending(pendingBytes)
	pendingReleased = true

	s.deliverFetchResponse(env.RequestID, resp)
	return nil
}

func (s *peerSession) sendFetchBatch(batch []queuedFetchRequest) (isrnode.FetchBatchResponseEnvelope, error) {
	req := isrnode.FetchBatchRequestEnvelope{
		Items: make([]isrnode.FetchBatchRequestItem, 0, len(batch)),
	}
	for _, item := range batch {
		env := item.env
		if env.FetchRequest == nil {
			return isrnode.FetchBatchResponseEnvelope{}, fmt.Errorf("isrnodetransport: missing fetch request payload")
		}
		req.Items = append(req.Items, isrnode.FetchBatchRequestItem{
			RequestID: env.RequestID,
			Request:   *env.FetchRequest,
		})
	}

	body, err := encodeFetchBatchRequest(req)
	if err != nil {
		return isrnode.FetchBatchResponseEnvelope{}, err
	}

	pendingBytes := int64(len(body))
	s.trackPending(pendingBytes)
	pendingReleased := false
	defer func() {
		if !pendingReleased {
			s.releasePending(pendingBytes)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), s.adapter.rpcTimeout)
	defer cancel()

	shardKey := fetchRPCShardKey(batch[0].env.GroupKey)
	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), shardKey, RPCServiceFetchBatch, body)
	if err != nil {
		return isrnode.FetchBatchResponseEnvelope{}, err
	}
	resp, err := decodeFetchBatchResponse(respBody)
	if err != nil {
		return isrnode.FetchBatchResponseEnvelope{}, err
	}
	s.releasePending(pendingBytes)
	pendingReleased = true
	return resp, nil
}

func (s *peerSession) flushBatch(batch []queuedFetchRequest) error {
	batchResp, err := s.sendFetchBatch(batch)
	if err != nil {
		var firstErr error
		for _, item := range batch {
			env := item.env
			if sendErr := s.sendFetchRequest(env); sendErr != nil {
				if firstErr == nil {
					firstErr = sendErr
				}
				s.deliverFetchFailure(env, sendErr)
			}
		}
		return firstErr
	}

	originalByRequestID := make(map[uint64]queuedFetchRequest, len(batch))
	for _, item := range batch {
		originalByRequestID[item.env.RequestID] = item
	}
	delivered := make(map[uint64]struct{}, len(batchResp.Items))

	var firstErr error
	for _, item := range batchResp.Items {
		queued, ok := originalByRequestID[item.RequestID]
		if !ok {
			continue
		}
		env := queued.env
		delivered[item.RequestID] = struct{}{}

		if item.Error != "" || item.Response == nil {
			if sendErr := s.sendFetchRequest(env); sendErr != nil {
				if firstErr == nil {
					firstErr = sendErr
				}
				s.deliverFetchFailure(env, sendErr)
			}
			continue
		}
		s.deliverFetchResponse(env.RequestID, *item.Response)
	}

	for requestID, queued := range originalByRequestID {
		if _, ok := delivered[requestID]; ok {
			continue
		}
		env := queued.env
		if sendErr := s.sendFetchRequest(env); sendErr != nil {
			if firstErr == nil {
				firstErr = sendErr
			}
			s.deliverFetchFailure(env, sendErr)
		}
	}
	return firstErr
}

func (s *peerSession) sendProgressAck(env isrnode.Envelope) error {
	if env.ProgressAck == nil {
		return fmt.Errorf("isrnodetransport: missing progress ack payload")
	}

	body, err := encodeProgressAck(*env.ProgressAck)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.adapter.rpcTimeout)
	defer cancel()
	_, err = s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey(env.GroupKey), RPCServiceProgressAck, body)
	return err
}

func (s *peerSession) deliverFetchResponse(requestID uint64, resp isrnode.FetchResponseEnvelope) {
	s.adapter.deliver(isrnode.Envelope{
		Peer:          s.peer,
		GroupKey:      resp.GroupKey,
		Epoch:         resp.Epoch,
		Generation:    resp.Generation,
		RequestID:     requestID,
		Kind:          isrnode.MessageKindFetchResponse,
		FetchResponse: &resp,
	})
}

func (s *peerSession) deliverFetchFailure(env isrnode.Envelope, err error) {
	failed := isrnode.Envelope{
		Peer:       s.peer,
		GroupKey:   env.GroupKey,
		Epoch:      env.Epoch,
		Generation: env.Generation,
		RequestID:  env.RequestID,
		Kind:       isrnode.MessageKindFetchFailure,
	}
	if err != nil {
		failed.Payload = []byte(err.Error())
	}
	s.adapter.deliver(failed)
}

func (s *peerSession) drainBatchQueue() []queuedFetchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.batchFetchQueue) == 0 {
		s.batchFlushScheduled = false
		return nil
	}
	batch := append([]queuedFetchRequest(nil), s.batchFetchQueue...)
	s.batchFetchQueue = s.batchFetchQueue[:0]
	s.batchQueuedBytes = 0
	s.batchFlushScheduled = false
	return batch
}

func (s *peerSession) Backpressure() isrnode.BackpressureState {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := isrnode.BackpressureState{
		PendingRequests: s.pendingRequests + len(s.batchFetchQueue),
		PendingBytes:    s.pendingBytes + s.batchQueuedBytes,
	}
	if s.adapter.maxPending > 0 && s.pendingRequests >= s.adapter.maxPending {
		state.Level = isrnode.BackpressureHard
	}
	if len(s.batchFetchQueue) >= s.maxQueuedFetchRequests() {
		state.Level = isrnode.BackpressureHard
	}
	return state
}

func (s *peerSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.batchFetchQueue = nil
	s.batchQueuedBytes = 0
	s.batchFlushScheduled = false
	s.mu.Unlock()
	return nil
}

func (s *peerSession) maxQueuedFetchRequests() int {
	maxPending := s.adapter.maxPending
	if maxPending <= 0 {
		maxPending = 1
	}
	return maxPending * maxQueuedFetchRequestsPerPendingRPC
}

func (s *peerSession) trackPending(bytes int64) {
	s.mu.Lock()
	s.pendingRequests++
	s.pendingBytes += bytes
	s.mu.Unlock()
}

func (s *peerSession) releasePending(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pendingRequests > 0 {
		s.pendingRequests--
	}
	s.pendingBytes -= bytes
	if s.pendingBytes < 0 {
		s.pendingBytes = 0
	}
}

func fetchRPCShardKey(groupKey isr.GroupKey) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)

	hash := uint64(fnvOffset64)
	for i := 0; i < len(groupKey); i++ {
		hash ^= uint64(groupKey[i])
		hash *= fnvPrime64
	}
	return hash
}
