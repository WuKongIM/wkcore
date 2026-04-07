package api

func (s *Server) registerRoutes() {
	if s == nil || s.engine == nil {
		return
	}

	s.engine.GET("/healthz", s.handleHealthz)
	s.engine.POST("/user/token", s.handleUpdateToken)
	s.engine.POST("/message/send", s.handleSendMessage)
	s.engine.POST("/conversation/sync", s.handleConversationSync)
}
