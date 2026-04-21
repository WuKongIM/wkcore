package manager

import (
	"net/http"
	"time"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	"github.com/gin-gonic/gin"
)

// SlotsResponse is the manager slot list response body.
type SlotsResponse struct {
	// Total is the number of returned manager slots.
	Total int `json:"total"`
	// Items contains the ordered manager slot DTO list.
	Items []SlotDTO `json:"items"`
}

// SlotDTO is the manager-facing slot response item.
type SlotDTO struct {
	// SlotID is the physical slot identifier.
	SlotID uint32 `json:"slot_id"`
	// State contains lightweight derived slot summaries.
	State SlotStateDTO `json:"state"`
	// Assignment contains the desired slot placement view.
	Assignment SlotAssignmentDTO `json:"assignment"`
	// Runtime contains the observed slot runtime view.
	Runtime SlotRuntimeDTO `json:"runtime"`
}

// SlotStateDTO contains derived slot list state fields.
type SlotStateDTO struct {
	// Quorum summarizes whether the slot currently has quorum.
	Quorum string `json:"quorum"`
	// Sync summarizes whether runtime peers/config match the assignment.
	Sync string `json:"sync"`
}

// SlotAssignmentDTO contains desired slot placement fields.
type SlotAssignmentDTO struct {
	// DesiredPeers is the desired slot voter set.
	DesiredPeers []uint64 `json:"desired_peers"`
	// ConfigEpoch is the desired slot config epoch.
	ConfigEpoch uint64 `json:"config_epoch"`
	// BalanceVersion is the desired slot balance generation.
	BalanceVersion uint64 `json:"balance_version"`
}

// SlotRuntimeDTO contains observed slot runtime fields.
type SlotRuntimeDTO struct {
	// CurrentPeers is the currently observed voter set.
	CurrentPeers []uint64 `json:"current_peers"`
	// LeaderID is the observed slot leader.
	LeaderID uint64 `json:"leader_id"`
	// HealthyVoters is the observed healthy voter count.
	HealthyVoters uint32 `json:"healthy_voters"`
	// HasQuorum reports whether the slot currently has quorum.
	HasQuorum bool `json:"has_quorum"`
	// ObservedConfigEpoch is the observed runtime config epoch.
	ObservedConfigEpoch uint64 `json:"observed_config_epoch"`
	// LastReportAt is the latest runtime observation timestamp.
	LastReportAt time.Time `json:"last_report_at"`
}

func (s *Server) handleSlots(c *gin.Context) {
	if s.management == nil {
		jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "management not configured")
		return
	}
	items, err := s.management.ListSlots(c.Request.Context())
	if err != nil {
		if leaderConsistentReadUnavailable(err) {
			jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "controller leader consistent read unavailable")
			return
		}
		jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	c.JSON(http.StatusOK, SlotsResponse{
		Total: len(items),
		Items: slotDTOs(items),
	})
}

func slotDTOs(items []managementusecase.Slot) []SlotDTO {
	out := make([]SlotDTO, 0, len(items))
	for _, item := range items {
		out = append(out, SlotDTO{
			SlotID: item.SlotID,
			State: SlotStateDTO{
				Quorum: item.State.Quorum,
				Sync:   item.State.Sync,
			},
			Assignment: SlotAssignmentDTO{
				DesiredPeers:   append([]uint64(nil), item.Assignment.DesiredPeers...),
				ConfigEpoch:    item.Assignment.ConfigEpoch,
				BalanceVersion: item.Assignment.BalanceVersion,
			},
			Runtime: SlotRuntimeDTO{
				CurrentPeers:        append([]uint64(nil), item.Runtime.CurrentPeers...),
				LeaderID:            item.Runtime.LeaderID,
				HealthyVoters:       item.Runtime.HealthyVoters,
				HasQuorum:           item.Runtime.HasQuorum,
				ObservedConfigEpoch: item.Runtime.ObservedConfigEpoch,
				LastReportAt:        item.Runtime.LastReportAt,
			},
		})
	}
	return out
}
