package manager

import (
	"net/http"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	"github.com/gin-gonic/gin"
)

// NodesResponse is the manager node list response body.
type NodesResponse struct {
	// Items contains the ordered manager node DTO list.
	Items []managementusecase.Node `json:"items"`
}

func (s *Server) handleNodes(c *gin.Context) {
	if s.management == nil {
		jsonError(c, http.StatusServiceUnavailable, "management not configured")
		return
	}
	items, err := s.management.ListNodes(c.Request.Context())
	if err != nil {
		jsonError(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, NodesResponse{Items: items})
}
