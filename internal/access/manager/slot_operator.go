package manager

import (
	"errors"
	"net/http"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/gin-gonic/gin"
)

type slotLeaderTransferRequest struct {
	TargetNodeID uint64 `json:"target_node_id"`
}

func (s *Server) handleSlotLeaderTransfer(c *gin.Context) {
	if s.management == nil {
		jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "management not configured")
		return
	}

	slotID, err := parseSlotIDParam(c.Param("slot_id"))
	if err != nil {
		jsonError(c, http.StatusBadRequest, "bad_request", "invalid slot_id")
		return
	}

	var req slotLeaderTransferRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		jsonError(c, http.StatusBadRequest, "bad_request", "invalid body")
		return
	}
	if req.TargetNodeID == 0 {
		jsonError(c, http.StatusBadRequest, "bad_request", "invalid target_node_id")
		return
	}

	item, err := s.management.TransferSlotLeader(c.Request.Context(), slotID, req.TargetNodeID)
	if err != nil {
		if errors.Is(err, controllermeta.ErrNotFound) {
			jsonError(c, http.StatusNotFound, "not_found", "slot not found")
			return
		}
		if errors.Is(err, managementusecase.ErrTargetNodeNotAssigned) {
			jsonError(c, http.StatusBadRequest, "bad_request", "target_node_id is not a desired peer")
			return
		}
		if leaderConsistentReadUnavailable(err) {
			jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "slot leader unavailable")
			return
		}
		jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	c.JSON(http.StatusOK, slotDetailDTO(item))
}
