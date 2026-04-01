package api

func (s *Server) registerRoutes() {
	if s == nil || s.engine == nil {
		return
	}

	s.engine.GET("/healthz", s.handleHealthz)
	s.engine.POST("/api/messages/send", s.handleSendMessage)
}
