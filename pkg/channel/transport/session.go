package transport

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
)

const defaultFetchBatchFlushWindow = 200 * time.Microsecond

const eagerFetchBatchCountThreshold = 8

const eagerFetchBatchBytesThreshold = 32 * 1024

const fetchBatchFallbackChunkSize = 4

const maxQueuedFetchRequestsPerPendingRPC = 32

var fetchBatchDebugEnabled = os.Getenv("WK_SEND_STRESS_BATCH_DEBUG") == "1"

var fetchBatchDebugLogs atomic.Int32

var scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {
	time.AfterFunc(delay, fn)
}

var runEagerFetchBatchFlush = func(fn func()) {
	go fn()
}

type sessionManager struct {
	adapter *Transport

	mu       sync.Mutex
	sessions map[channel.NodeID]*peerSession
}

type peerSession struct {
	adapter *Transport
	peer    channel.NodeID

	mu              sync.Mutex
	pendingRequests int
	pendingBytes    int64
	closed          bool

	batchFlushScheduled    bool
	batchEagerFlushPending bool
	batchQueuedBytes       int64
	batchFetchQueue        []queuedFetchRequest
}

type queuedFetchRequest struct {
	env   runtime.Envelope
	bytes int64
}

var _ runtime.PeerSessionManager = (*sessionManager)(nil)
var _ runtime.PeerSession = (*peerSession)(nil)

func newSessionManager(adapter *Transport) *sessionManager {
	return &sessionManager{
		adapter:  adapter,
		sessions: make(map[channel.NodeID]*peerSession),
	}
}

func (m *sessionManager) Session(peer channel.NodeID) runtime.PeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, ok := m.sessions[peer]; ok {
		return session
	}
	session := &peerSession{adapter: m.adapter, peer: peer}
	m.sessions[peer] = session
	return session
}

func (s *peerSession) Send(env runtime.Envelope) error {
	switch env.Kind {
	case runtime.MessageKindFetchRequest:
		return s.sendFetchRequest(env)
	case runtime.MessageKindProgressAck:
		return s.sendProgressAck(env)
	case runtime.MessageKindReconcileProbeRequest:
		return s.sendReconcileProbe(env)
	default:
		return fmt.Errorf("channeltransport: unsupported envelope kind %d", env.Kind)
	}
}

func (s *peerSession) TryBatch(env runtime.Envelope) bool {
	if env.Kind != runtime.MessageKindFetchRequest || env.FetchRequest == nil {
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
	shouldEagerFlush := (len(s.batchFetchQueue) >= eagerFetchBatchCountThreshold || s.batchQueuedBytes >= eagerFetchBatchBytesThreshold) && !s.batchEagerFlushPending
	if shouldEagerFlush {
		s.batchEagerFlushPending = true
	}
	s.mu.Unlock()

	if shouldSchedule {
		scheduleFetchBatchFlush(defaultFetchBatchFlushWindow, func() {
			_ = s.Flush()
		})
	}
	if shouldEagerFlush {
		runEagerFetchBatchFlush(func() {
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

func (s *peerSession) sendFetchRequest(env runtime.Envelope) error {
	if env.FetchRequest == nil {
		return fmt.Errorf("channeltransport: missing fetch request payload")
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

	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey(env.ChannelKey), RPCServiceFetch, body)
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

func (s *peerSession) sendFetchBatch(batch []queuedFetchRequest) (runtime.FetchBatchResponseEnvelope, error) {
	req := runtime.FetchBatchRequestEnvelope{
		Items: make([]runtime.FetchBatchRequestItem, 0, len(batch)),
	}
	for _, item := range batch {
		env := item.env
		if env.FetchRequest == nil {
			return runtime.FetchBatchResponseEnvelope{}, fmt.Errorf("channeltransport: missing fetch request payload")
		}
		req.Items = append(req.Items, runtime.FetchBatchRequestItem{
			RequestID: env.RequestID,
			Request:   *env.FetchRequest,
		})
	}

	body, err := encodeFetchBatchRequest(req)
	if err != nil {
		return runtime.FetchBatchResponseEnvelope{}, err
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

	shardKey := fetchRPCShardKey(batch[0].env.ChannelKey)
	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), shardKey, RPCServiceFetchBatch, body)
	if err != nil {
		return runtime.FetchBatchResponseEnvelope{}, err
	}
	resp, err := decodeFetchBatchResponse(respBody)
	if err != nil {
		return runtime.FetchBatchResponseEnvelope{}, err
	}
	s.releasePending(pendingBytes)
	pendingReleased = true
	return resp, nil
}

func (s *peerSession) flushBatch(batch []queuedFetchRequest) error {
	if len(batch) == 0 {
		return nil
	}

	batchResp, err := s.sendFetchBatch(batch)
	if err != nil {
		if fetchBatchDebugEnabled && fetchBatchDebugLogs.Add(1) <= 12 {
			log.Printf("channeltransport: fetch batch fallback peer=%d batch=%d err=%v", s.peer, len(batch), err)
		}
		return s.retryBatchChunks(batch)
	}
	return s.deliverBatchResponse(batch, batchResp)
}

func (s *peerSession) deliverBatchResponse(batch []queuedFetchRequest, batchResp runtime.FetchBatchResponseEnvelope) error {
	originalByRequestID := make(map[uint64]queuedFetchRequest, len(batch))
	for _, item := range batch {
		originalByRequestID[item.env.RequestID] = item
	}
	delivered := make(map[uint64]struct{}, len(batchResp.Items))

	retryBatch := make([]queuedFetchRequest, 0)
	for _, item := range batchResp.Items {
		queued, ok := originalByRequestID[item.RequestID]
		if !ok {
			continue
		}
		env := queued.env
		delivered[item.RequestID] = struct{}{}

		if item.Error != "" || item.Response == nil {
			retryBatch = append(retryBatch, queued)
			continue
		}
		s.deliverFetchResponse(env.RequestID, *item.Response)
	}

	for requestID, queued := range originalByRequestID {
		if _, ok := delivered[requestID]; ok {
			continue
		}
		retryBatch = append(retryBatch, queued)
	}
	if len(retryBatch) == 0 {
		return nil
	}
	return s.retryBatchChunks(retryBatch)
}

func (s *peerSession) retryBatchChunks(batch []queuedFetchRequest) error {
	var firstErr error
	for start := 0; start < len(batch); start += fetchBatchFallbackChunkSize {
		end := start + fetchBatchFallbackChunkSize
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[start:end]
		var err error
		if len(chunk) == 1 {
			err = s.sendSingleFetch(chunk[0])
		} else {
			err = s.flushBatchChunk(chunk)
		}
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *peerSession) flushBatchChunk(chunk []queuedFetchRequest) error {
	batchResp, err := s.sendFetchBatch(chunk)
	if err != nil {
		if fetchBatchDebugEnabled && fetchBatchDebugLogs.Add(1) <= 12 {
			log.Printf("channeltransport: fetch chunk fallback peer=%d batch=%d err=%v", s.peer, len(chunk), err)
		}
		return s.retrySingles(chunk)
	}
	return s.deliverChunkResponse(chunk, batchResp)
}

func (s *peerSession) deliverChunkResponse(chunk []queuedFetchRequest, batchResp runtime.FetchBatchResponseEnvelope) error {
	originalByRequestID := make(map[uint64]queuedFetchRequest, len(chunk))
	for _, item := range chunk {
		originalByRequestID[item.env.RequestID] = item
	}
	delivered := make(map[uint64]struct{}, len(batchResp.Items))
	retryBatch := make([]queuedFetchRequest, 0)

	for _, item := range batchResp.Items {
		queued, ok := originalByRequestID[item.RequestID]
		if !ok {
			continue
		}
		delivered[item.RequestID] = struct{}{}
		if item.Error != "" || item.Response == nil {
			retryBatch = append(retryBatch, queued)
			continue
		}
		s.deliverFetchResponse(queued.env.RequestID, *item.Response)
	}

	for requestID, queued := range originalByRequestID {
		if _, ok := delivered[requestID]; ok {
			continue
		}
		retryBatch = append(retryBatch, queued)
	}
	return s.retrySingles(retryBatch)
}

func (s *peerSession) retrySingles(batch []queuedFetchRequest) error {
	var firstErr error
	for _, item := range batch {
		if err := s.sendSingleFetch(item); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *peerSession) sendSingleFetch(item queuedFetchRequest) error {
	if err := s.sendFetchRequest(item.env); err != nil {
		s.deliverFetchFailure(item.env, err)
		return err
	}
	return nil
}

func (s *peerSession) sendProgressAck(env runtime.Envelope) error {
	if env.ProgressAck == nil {
		return fmt.Errorf("channeltransport: missing progress ack payload")
	}

	body, err := encodeProgressAck(*env.ProgressAck)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.adapter.rpcTimeout)
	defer cancel()
	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey(env.ChannelKey), RPCServiceProgressAck, body)
	if err != nil {
		return err
	}
	if len(respBody) == 0 {
		return nil
	}
	resp, err := decodeProgressAckResponse(respBody)
	if err != nil {
		return err
	}
	if resp.LeaderHW == 0 {
		return nil
	}
	s.deliverFetchResponse(0, runtime.FetchResponseEnvelope{
		ChannelKey: env.ChannelKey,
		Epoch:      env.Epoch,
		Generation: env.Generation,
		LeaderHW:   resp.LeaderHW,
	})
	return nil
}

func (s *peerSession) sendReconcileProbe(env runtime.Envelope) error {
	if env.ReconcileProbeRequest == nil {
		return fmt.Errorf("channeltransport: missing reconcile probe payload")
	}

	body, err := encodeReconcileProbeRequest(*env.ReconcileProbeRequest)
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

	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey(env.ChannelKey), RPCServiceReconcileProbe, body)
	if err != nil {
		return err
	}
	resp, err := decodeReconcileProbeResponse(respBody)
	if err != nil {
		return err
	}
	s.releasePending(pendingBytes)
	pendingReleased = true
	s.deliverReconcileProbeResponse(env.RequestID, resp)
	return nil
}

func (s *peerSession) deliverFetchResponse(requestID uint64, resp runtime.FetchResponseEnvelope) {
	s.adapter.deliver(runtime.Envelope{
		Peer:          s.peer,
		ChannelKey:    resp.ChannelKey,
		Epoch:         resp.Epoch,
		Generation:    resp.Generation,
		RequestID:     requestID,
		Kind:          runtime.MessageKindFetchResponse,
		Sync:          true,
		FetchResponse: &resp,
	})
}

func (s *peerSession) deliverFetchFailure(env runtime.Envelope, err error) {
	failed := runtime.Envelope{
		Peer:       s.peer,
		ChannelKey: env.ChannelKey,
		Epoch:      env.Epoch,
		Generation: env.Generation,
		RequestID:  env.RequestID,
		Kind:       runtime.MessageKindFetchFailure,
		Sync:       true,
	}
	if err != nil {
		failed.Payload = []byte(err.Error())
	}
	s.adapter.deliver(failed)
}

func (s *peerSession) deliverReconcileProbeResponse(requestID uint64, resp runtime.ReconcileProbeResponseEnvelope) {
	s.adapter.deliver(runtime.Envelope{
		Peer:                   s.peer,
		ChannelKey:             resp.ChannelKey,
		Epoch:                  resp.Epoch,
		Generation:             resp.Generation,
		RequestID:              requestID,
		Kind:                   runtime.MessageKindReconcileProbeResponse,
		Sync:                   true,
		ReconcileProbeResponse: &resp,
	})
}

func (s *peerSession) drainBatchQueue() []queuedFetchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.drainBatchQueueLocked()
}

func (s *peerSession) drainBatchQueueLocked() []queuedFetchRequest {
	if len(s.batchFetchQueue) == 0 {
		s.batchFlushScheduled = false
		s.batchEagerFlushPending = false
		return nil
	}
	batch := append([]queuedFetchRequest(nil), s.batchFetchQueue...)
	s.batchFetchQueue = s.batchFetchQueue[:0]
	s.batchQueuedBytes = 0
	s.batchFlushScheduled = false
	s.batchEagerFlushPending = false
	return batch
}

func (s *peerSession) Backpressure() runtime.BackpressureState {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := runtime.BackpressureState{
		PendingRequests: s.pendingRequests + len(s.batchFetchQueue),
		PendingBytes:    s.pendingBytes + s.batchQueuedBytes,
	}
	if s.adapter.maxPending > 0 && s.pendingRequests >= s.adapter.maxPending {
		state.Level = runtime.BackpressureHard
	}
	if len(s.batchFetchQueue) >= s.maxQueuedFetchRequests() {
		state.Level = runtime.BackpressureHard
	}
	return state
}

func (s *peerSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.batchFetchQueue = nil
	s.batchQueuedBytes = 0
	s.batchFlushScheduled = false
	s.batchEagerFlushPending = false
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

func fetchRPCShardKey(channelKey channel.ChannelKey) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)

	hash := uint64(fnvOffset64)
	for i := 0; i < len(channelKey); i++ {
		hash ^= uint64(channelKey[i])
		hash *= fnvPrime64
	}
	return hash
}
