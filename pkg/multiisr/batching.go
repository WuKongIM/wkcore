package multiisr

func (r *runtime) sendEnvelope(env Envelope) error {
	trackInflight := env.Kind == MessageKindFetchRequest
	if trackInflight && !r.peerRequests.tryAcquire(env.Peer, r.cfg.Limits.MaxFetchInflightPeer) {
		r.peerRequests.enqueue(env)
		return ErrBackpressured
	}

	session := r.peerSession(env.Peer)
	if session.TryBatch(env) {
		return nil
	}
	if err := session.Send(env); err != nil {
		if trackInflight {
			r.peerRequests.release(env.Peer)
		}
		return err
	}
	return nil
}
