package runtime

import (
	"context"
	"errors"
	"sync"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type peerRequestState struct {
	mu       sync.Mutex
	inflight map[core.NodeID]int
	groups   map[channelPeerKey]inflightReservation
	queued   map[core.NodeID]*peerEnvelopeQueue
}

type channelPeerKey struct {
	channelKey core.ChannelKey
	peer       core.NodeID
}

type inflightReservation struct {
	generation uint64
	requestID  uint64
}

type peerEnvelopeQueue struct {
	items []Envelope
	head  int
}

func newPeerRequestState() peerRequestState {
	return peerRequestState{
		inflight: make(map[core.NodeID]int),
		groups:   make(map[channelPeerKey]inflightReservation),
		queued:   make(map[core.NodeID]*peerEnvelopeQueue),
	}
}

func (s *peerRequestState) tryAcquire(peer core.NodeID, limit int) bool {
	if limit <= 0 {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] >= limit {
		return false
	}
	s.inflight[peer]++
	return true
}

func (s *peerRequestState) tryAcquireChannel(env Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	channelPeer := channelPeerKey{channelKey: env.ChannelKey, peer: env.Peer}
	if _, ok := s.groups[channelPeer]; ok {
		return false
	}
	s.groups[channelPeer] = inflightReservation{
		generation: env.Generation,
		requestID:  env.RequestID,
	}
	return true
}

func (s *peerRequestState) enqueue(env Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queueLocked(env.Peer)
	q.enqueue(env)
}

func (s *peerRequestState) queuedCount(peer core.NodeID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queued[peer]
	if q == nil {
		return 0
	}
	return q.len()
}

func (s *peerRequestState) release(peer core.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] > 0 {
		s.inflight[peer]--
	}
}

func (s *peerRequestState) releaseChannel(key core.ChannelKey, peer core.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groups, channelPeerKey{channelKey: key, peer: peer})
}

func (s *peerRequestState) clearChannel(key core.ChannelKey) []core.NodeID {
	s.mu.Lock()
	defer s.mu.Unlock()

	affected := make(map[core.NodeID]struct{})
	for channelPeer := range s.groups {
		if channelPeer.channelKey != key {
			continue
		}
		delete(s.groups, channelPeer)
		if s.inflight[channelPeer.peer] > 0 {
			s.inflight[channelPeer.peer]--
		}
		affected[channelPeer.peer] = struct{}{}
	}
	for peer, queue := range s.queued {
		if queue == nil {
			continue
		}
		if queue.dropChannel(key) {
			affected[peer] = struct{}{}
		}
	}
	peers := make([]core.NodeID, 0, len(affected))
	for peer := range affected {
		peers = append(peers, peer)
	}
	return peers
}

func (s *peerRequestState) clearChannelInvalidPeers(key core.ChannelKey, allow func(core.NodeID) bool) []core.NodeID {
	s.mu.Lock()
	defer s.mu.Unlock()

	affected := make(map[core.NodeID]struct{})
	for channelPeer := range s.groups {
		if channelPeer.channelKey != key {
			continue
		}
		if allow != nil && allow(channelPeer.peer) {
			continue
		}
		delete(s.groups, channelPeer)
		if s.inflight[channelPeer.peer] > 0 {
			s.inflight[channelPeer.peer]--
		}
		affected[channelPeer.peer] = struct{}{}
	}
	for peer, queue := range s.queued {
		if queue == nil {
			continue
		}
		if allow != nil && allow(peer) {
			continue
		}
		if queue.dropChannel(key) {
			affected[peer] = struct{}{}
		}
	}
	peers := make([]core.NodeID, 0, len(affected))
	for peer := range affected {
		peers = append(peers, peer)
	}
	return peers
}

func (s *peerRequestState) releaseInflightForEnvelope(env Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelPeerKey{channelKey: env.ChannelKey, peer: env.Peer}
	reservation, ok := s.groups[key]
	if !ok {
		return false
	}
	if reservation.generation != 0 && env.Generation != 0 && reservation.generation != env.Generation {
		return false
	}
	if reservation.requestID != 0 && env.RequestID != 0 && reservation.requestID != env.RequestID {
		return false
	}
	delete(s.groups, key)
	if s.inflight[env.Peer] > 0 {
		s.inflight[env.Peer]--
	}
	return true
}

func (s *peerRequestState) popQueued(peer core.NodeID) (Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queued[peer]
	if q == nil {
		return Envelope{}, false
	}
	return q.pop()
}

func (s *peerRequestState) queueLocked(peer core.NodeID) *peerEnvelopeQueue {
	if q, ok := s.queued[peer]; ok {
		return q
	}
	q := &peerEnvelopeQueue{}
	s.queued[peer] = q
	return q
}

func (q *peerEnvelopeQueue) enqueue(env Envelope) {
	if env.Kind == MessageKindFetchRequest {
		for i := q.head; i < len(q.items); i++ {
			if q.items[i].Kind == MessageKindFetchRequest && q.items[i].ChannelKey == env.ChannelKey {
				q.items[i] = env
				return
			}
		}
	}
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, env)
}

func (q *peerEnvelopeQueue) pop() (Envelope, bool) {
	if q.head >= len(q.items) {
		return Envelope{}, false
	}

	env := q.items[q.head]
	q.items[q.head] = Envelope{}
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return env, true
}

func (q *peerEnvelopeQueue) len() int {
	return len(q.items) - q.head
}

func (q *peerEnvelopeQueue) dropChannel(key core.ChannelKey) bool {
	if q.head >= len(q.items) {
		return false
	}

	write := 0
	removed := false
	for i := q.head; i < len(q.items); i++ {
		env := q.items[i]
		if env.ChannelKey == key {
			removed = true
			continue
		}
		q.items[write] = env
		write++
	}
	for i := write; i < len(q.items); i++ {
		q.items[i] = Envelope{}
	}
	q.items = q.items[:write]
	q.head = 0
	return removed
}

func (q *peerEnvelopeQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = Envelope{}
	}
	q.items = q.items[:n]
	q.head = 0
}

func (r *runtime) sendEnvelope(env Envelope) error {
	env = r.refreshFetchEnvelope(env)
	trackInflight := env.Kind == MessageKindFetchRequest

	session := r.peerSession(env.Peer)
	if state := session.Backpressure(); state.Level == BackpressureHard {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}

	if trackInflight && !r.peerRequests.tryAcquireChannel(env) {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}
	if trackInflight && !r.peerRequests.tryAcquire(env.Peer, r.cfg.Limits.MaxFetchInflightPeer) {
		r.peerRequests.releaseChannel(env.ChannelKey, env.Peer)
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}

	if env.Kind == MessageKindFetchRequest && session.TryBatch(env) {
		if err := session.Flush(); err != nil {
			if trackInflight {
				r.peerRequests.release(env.Peer)
				r.peerRequests.releaseChannel(env.ChannelKey, env.Peer)
			}
			return err
		}
		return nil
	}
	if err := session.Send(env); err != nil {
		if trackInflight {
			r.peerRequests.release(env.Peer)
			r.peerRequests.releaseChannel(env.ChannelKey, env.Peer)
		}
		return err
	}
	return nil
}

func (r *runtime) refreshFetchEnvelope(env Envelope) Envelope {
	if env.Kind != MessageKindFetchRequest || env.FetchRequest == nil {
		return env
	}

	ch, ok := r.lookupChannel(env.ChannelKey)
	if !ok {
		return env
	}

	state := ch.Status()
	req := *env.FetchRequest
	req.ChannelKey = env.ChannelKey
	req.Epoch = state.Epoch
	req.Generation = ch.gen
	req.ReplicaID = r.cfg.LocalNode
	req.FetchOffset = state.LEO
	req.OffsetEpoch = state.OffsetEpoch
	if req.MaxBytes <= 0 {
		req.MaxBytes = defaultFetchMaxBytes
	}
	env.Epoch = state.Epoch
	env.Generation = ch.gen
	env.FetchRequest = &req
	return env
}

func (r *runtime) queuedPeerRequests(peer core.NodeID) int {
	return r.peerRequests.queuedCount(peer)
}

func (r *runtime) releasePeerInflight(peer core.NodeID) {
	r.peerRequests.release(peer)
}

func (r *runtime) releaseChannelInflight(key core.ChannelKey, peer core.NodeID) {
	r.peerRequests.releaseChannel(key, peer)
}

func (r *runtime) drainPeerQueue(peer core.NodeID) {
	env, ok := r.peerRequests.popQueued(peer)
	if !ok {
		return
	}
	if err := r.sendEnvelope(env); err != nil && !errors.Is(err, ErrBackpressured) {
		r.retryReplication(env.ChannelKey, env.Peer, true)
	}
}

func (r *runtime) releaseInflightForEnvelope(env Envelope) bool {
	return r.peerRequests.releaseInflightForEnvelope(env)
}

func (r *runtime) handleEnvelope(env Envelope) {
	var (
		ch        *channel
		knownDrop bool
	)

	active, ok := r.lookupChannel(env.ChannelKey)
	if ok && active.gen == env.Generation {
		ch = active
	} else if env.Kind == MessageKindFetchResponse && r.tombstones.contains(env.ChannelKey, env.Generation) {
		knownDrop = true
	}

	if env.Kind == MessageKindFetchResponse && knownDrop {
		if r.releaseInflightForEnvelope(env) {
			r.drainPeerQueue(env.Peer)
		}
		return
	}
	if env.Kind == MessageKindFetchFailure {
		if r.releaseInflightForEnvelope(env) {
			if ch != nil {
				r.retryReplication(env.ChannelKey, env.Peer, true)
				r.scheduleFollowerReplication(env.ChannelKey, env.Peer)
			}
			r.drainPeerQueue(env.Peer)
		}
		return
	}

	if ch == nil {
		return
	}

	if env.Kind == MessageKindFetchResponse {
		if r.deliverEnvelope(ch, env) {
			if r.releaseInflightForEnvelope(env) {
				r.drainPeerQueue(env.Peer)
			}
		}
		return
	}
	_ = r.deliverEnvelope(ch, env)
}

func (r *runtime) deliverEnvelope(ch *channel, env Envelope) bool {
	switch env.Kind {
	case MessageKindFetchResponse:
		state := ch.Status()
		if env.Epoch != state.Epoch {
			return true
		}
		if env.FetchResponse == nil {
			return false
		}
		return r.applyFetchResponseEnvelope(ch, env.Peer, *env.FetchResponse) == nil
	case MessageKindProgressAck:
		state := ch.Status()
		if env.Epoch != state.Epoch {
			return true
		}
		if env.ProgressAck == nil {
			return false
		}
		return r.applyProgressAckEnvelope(ch, *env.ProgressAck) == nil
	}
	return true
}

func (r *runtime) applyFetchResponseEnvelope(ch *channel, peer core.NodeID, env FetchResponseEnvelope) error {
	if err := ch.replica.ApplyFetch(context.Background(), core.ReplicaApplyFetchRequest{
		ChannelKey: env.ChannelKey,
		Epoch:      env.Epoch,
		Leader:     peer,
		TruncateTo: env.TruncateTo,
		Records:    env.Records,
		LeaderHW:   env.LeaderHW,
	}); err != nil {
		return err
	}

	meta := ch.metaSnapshot()
	if meta.Leader != r.cfg.LocalNode {
		if len(env.Records) > 0 || env.TruncateTo != nil {
			state := ch.Status()
			if err := r.sendEnvelope(Envelope{
				Peer:       meta.Leader,
				ChannelKey: ch.key,
				Epoch:      meta.Epoch,
				Generation: ch.gen,
				RequestID:  r.requestID.Add(1),
				Kind:       MessageKindProgressAck,
				ProgressAck: &ProgressAckEnvelope{
					ChannelKey:  ch.key,
					Epoch:       meta.Epoch,
					Generation:  ch.gen,
					ReplicaID:   r.cfg.LocalNode,
					MatchOffset: state.LEO,
				},
			}); err != nil && !errors.Is(err, ErrBackpressured) {
				r.retryReplication(ch.key, meta.Leader, true)
			}
		}

		if len(env.Records) == 0 && env.TruncateTo == nil {
			r.scheduleFollowerReplication(ch.key, meta.Leader)
			return nil
		}
		state := ch.Status()
		err := r.sendEnvelope(Envelope{
			Peer:       meta.Leader,
			ChannelKey: ch.key,
			Epoch:      meta.Epoch,
			Generation: ch.gen,
			RequestID:  r.requestID.Add(1),
			Kind:       MessageKindFetchRequest,
			FetchRequest: &FetchRequestEnvelope{
				ChannelKey:  ch.key,
				Epoch:       meta.Epoch,
				Generation:  ch.gen,
				ReplicaID:   r.cfg.LocalNode,
				FetchOffset: state.LEO,
				OffsetEpoch: state.OffsetEpoch,
				MaxBytes:    defaultFetchMaxBytes,
			},
		})
		if err != nil && !errors.Is(err, ErrBackpressured) {
			r.retryReplication(ch.key, meta.Leader, true)
		}
	}
	return nil
}

func (r *runtime) applyProgressAckEnvelope(ch *channel, env ProgressAckEnvelope) error {
	return ch.replica.ApplyProgressAck(context.Background(), core.ReplicaProgressAckRequest{
		ChannelKey:  env.ChannelKey,
		Epoch:       env.Epoch,
		ReplicaID:   env.ReplicaID,
		MatchOffset: env.MatchOffset,
	})
}

func (r *runtime) ServeFetch(ctx context.Context, req FetchRequestEnvelope) (FetchResponseEnvelope, error) {
	ch, ok := r.lookupChannel(req.ChannelKey)
	if !ok {
		return FetchResponseEnvelope{}, ErrChannelNotFound
	}
	if ch.gen != req.Generation {
		return FetchResponseEnvelope{}, ErrGenerationMismatch
	}

	meta := ch.metaSnapshot()
	if req.Epoch != meta.Epoch {
		return FetchResponseEnvelope{}, core.ErrStaleMeta
	}

	fetchReq := core.ReplicaFetchRequest{
		ChannelKey:  req.ChannelKey,
		Epoch:       req.Epoch,
		ReplicaID:   req.ReplicaID,
		FetchOffset: req.FetchOffset,
		OffsetEpoch: req.OffsetEpoch,
		MaxBytes:    req.MaxBytes,
	}
	result, err := ch.replica.Fetch(ctx, fetchReq)
	if err != nil {
		return FetchResponseEnvelope{}, err
	}
	return FetchResponseEnvelope{
		ChannelKey: req.ChannelKey,
		Epoch:      result.Epoch,
		Generation: req.Generation,
		TruncateTo: result.TruncateTo,
		LeaderHW:   result.HW,
		Records:    result.Records,
	}, nil
}

type nopTransport struct {
	mu      sync.Mutex
	handler func(Envelope)
}

func (t *nopTransport) Send(core.NodeID, Envelope) error {
	return nil
}

func (t *nopTransport) RegisterHandler(fn func(Envelope)) {
	t.mu.Lock()
	t.handler = fn
	t.mu.Unlock()
}

type nopPeerSessionManager struct{}

func (nopPeerSessionManager) Session(core.NodeID) PeerSession {
	return nopPeerSession{}
}

type nopPeerSession struct{}

func (nopPeerSession) Send(Envelope) error             { return nil }
func (nopPeerSession) TryBatch(Envelope) bool          { return false }
func (nopPeerSession) Flush() error                    { return nil }
func (nopPeerSession) Backpressure() BackpressureState { return BackpressureState{} }
func (nopPeerSession) Close() error                    { return nil }
