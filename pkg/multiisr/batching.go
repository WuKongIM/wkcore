package multiisr

func (r *runtime) sendEnvelope(env Envelope) error {
	session := r.peerSession(env.Peer)
	if session.TryBatch(env) {
		return nil
	}
	return session.Send(env)
}
