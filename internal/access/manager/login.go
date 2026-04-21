package manager

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Username  string    `json:"username"`
	TokenType string    `json:"token_type"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Server) handleLogin(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		jsonError(c, http.StatusBadRequest, "invalid request")
		return
	}
	if !s.auth.verifyCredentials(req.Username, req.Password) {
		jsonError(c, http.StatusUnauthorized, "invalid credentials")
		return
	}

	now := time.Now()
	token, err := s.issueToken(req.Username, now)
	if err != nil {
		jsonError(c, http.StatusInternalServerError, "failed to issue token")
		return
	}
	c.JSON(http.StatusOK, loginResponse{
		Username:  req.Username,
		TokenType: "Bearer",
		Token:     token,
		ExpiresAt: now.Add(s.auth.jwtExpire),
	})
}
