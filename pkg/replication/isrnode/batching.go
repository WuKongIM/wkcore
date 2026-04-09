package isrnode

func (r *runtime) sendEnvelope(env Envelope) error {
	env = r.refreshFetchEnvelope(env)
	trackInflight := env.Kind == MessageKindFetchRequest
	session := r.peerSession(env.Peer)
	if state := session.Backpressure(); state.Level == BackpressureHard {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}
	if trackInflight && !r.peerRequests.tryAcquireGroup(env.GroupKey, env.Peer) {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}
	if trackInflight && !r.peerRequests.tryAcquire(env.Peer, r.cfg.Limits.MaxFetchInflightPeer) {
		r.peerRequests.releaseGroup(env.GroupKey, env.Peer)
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}

	if env.Kind == MessageKindFetchRequest && session.TryBatch(env) {
		return nil
	}
	if err := session.Send(env); err != nil {
		if trackInflight {
			r.peerRequests.release(env.Peer)
			r.peerRequests.releaseGroup(env.GroupKey, env.Peer)
		}
		return err
	}
	return nil
}

func (r *runtime) refreshFetchEnvelope(env Envelope) Envelope {
	if env.Kind != MessageKindFetchRequest || env.FetchRequest == nil {
		return env
	}

	r.mu.RLock()
	g, ok := r.groups[env.GroupKey]
	r.mu.RUnlock()
	if !ok {
		return env
	}

	meta := g.Status()
	req := *env.FetchRequest
	req.GroupKey = env.GroupKey
	req.Epoch = meta.Epoch
	req.Generation = g.generation
	req.ReplicaID = r.cfg.LocalNode
	req.FetchOffset = meta.LEO
	req.OffsetEpoch = meta.OffsetEpoch
	if req.MaxBytes <= 0 {
		req.MaxBytes = defaultFetchMaxBytes
	}

	env.Epoch = meta.Epoch
	env.Generation = g.generation
	env.FetchRequest = &req
	return env
}
