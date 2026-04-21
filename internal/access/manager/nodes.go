package manager

import (
	"net/http"
	"time"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	"github.com/gin-gonic/gin"
)

// NodesResponse is the manager node list response body.
type NodesResponse struct {
	// Total is the number of returned manager nodes.
	Total int `json:"total"`
	// Items contains the ordered manager node DTO list.
	Items []NodeDTO `json:"items"`
}

// NodeDTO is the manager-facing node response item.
type NodeDTO struct {
	// NodeID is the node identifier.
	NodeID uint64 `json:"node_id"`
	// Addr is the cluster listen address of the node.
	Addr string `json:"addr"`
	// Status is the manager-facing node status string.
	Status string `json:"status"`
	// LastHeartbeatAt is the latest controller heartbeat timestamp.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// IsLocal reports whether the node is the current process node.
	IsLocal bool `json:"is_local"`
	// CapacityWeight is the configured controller capacity weight.
	CapacityWeight int `json:"capacity_weight"`
	// Controller contains controller-related node view fields.
	Controller NodeControllerDTO `json:"controller"`
	// SlotStats contains slot hosting summary fields.
	SlotStats NodeSlotStatsDTO `json:"slot_stats"`
}

// NodeControllerDTO contains controller-facing node state.
type NodeControllerDTO struct {
	// Role is the controller role summary for the node.
	Role string `json:"role"`
}

// NodeSlotStatsDTO contains slot placement summary fields.
type NodeSlotStatsDTO struct {
	// Count is the number of observed slot peers hosted by the node.
	Count int `json:"count"`
	// LeaderCount is the number of observed slots led by the node.
	LeaderCount int `json:"leader_count"`
}

func (s *Server) handleNodes(c *gin.Context) {
	if s.management == nil {
		jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "management not configured")
		return
	}
	items, err := s.management.ListNodes(c.Request.Context())
	if err != nil {
		jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	c.JSON(http.StatusOK, NodesResponse{
		Total: len(items),
		Items: nodeDTOs(items),
	})
}

func nodeDTOs(items []managementusecase.Node) []NodeDTO {
	out := make([]NodeDTO, 0, len(items))
	for _, item := range items {
		out = append(out, NodeDTO{
			NodeID:          item.NodeID,
			Addr:            item.Addr,
			Status:          item.Status,
			LastHeartbeatAt: item.LastHeartbeatAt,
			IsLocal:         item.IsLocal,
			CapacityWeight:  item.CapacityWeight,
			Controller: NodeControllerDTO{
				Role: item.ControllerRole,
			},
			SlotStats: NodeSlotStatsDTO{
				Count:       item.SlotCount,
				LeaderCount: item.LeaderSlotCount,
			},
		})
	}
	return out
}
