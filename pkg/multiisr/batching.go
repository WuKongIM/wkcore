package multiisr

func (r *runtime) sendEnvelope(env Envelope) error {
	if !r.peerRequests.tryAcquire(env.Peer, r.cfg.Limits.MaxFetchInflightPeer) {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}

	session := r.peerSession(env.Peer)
	if session.TryBatch(env) {
		return nil
	}
	return session.Send(env)
}
