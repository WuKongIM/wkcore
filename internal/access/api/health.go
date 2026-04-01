package api

import "github.com/gin-gonic/gin"

func (s *Server) handleHealthz(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}
