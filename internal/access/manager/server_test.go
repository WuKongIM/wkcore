package manager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/stretchr/testify/require"
)

func TestManagerLoginIssuesJWTForAuthorizedUser(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/manager/login", bytes.NewBufferString(`{"username":"admin","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body loginResponseBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "admin", body.Username)
	require.Equal(t, "Bearer", body.TokenType)
	require.NotEmpty(t, body.AccessToken)
	require.Equal(t, int64(time.Hour/time.Second), body.ExpiresIn)
	require.WithinDuration(t, time.Now().Add(time.Hour), body.ExpiresAt, 2*time.Second)
	require.Equal(t, []permissionBody{{
		Resource: "cluster.node",
		Actions:  []string{"r"},
	}}, body.Permissions)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	_, hasLegacyToken := raw["token"]
	require.False(t, hasLegacyToken)
}

func TestManagerLoginRejectsInvalidCredentials(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
		}}),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/manager/login", bytes.NewBufferString(`{"username":"admin","password":"bad"}`))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"invalid_credentials","message":"invalid credentials"}`, rec.Body.String())
}

func TestManagerNodesRejectsMissingToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/nodes", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerNodesRejectsExpiredToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueExpiredTestToken(t, srv, "ghost"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerNodesRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerNodesReturnsAggregatedList(t *testing.T) {
	lastHeartbeatAt := time.Date(2026, 4, 21, 15, 4, 5, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			nodes: []managementusecase.Node{{
				NodeID:          1,
				Addr:            "127.0.0.1:7000",
				Status:          "alive",
				LastHeartbeatAt: lastHeartbeatAt,
				ControllerRole:  "leader",
				SlotCount:       3,
				LeaderSlotCount: 2,
				IsLocal:         true,
				CapacityWeight:  1,
			}},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"total": 1,
		"items": [{
			"node_id": 1,
			"addr": "127.0.0.1:7000",
			"status": "alive",
			"last_heartbeat_at": "2026-04-21T15:04:05Z",
			"is_local": true,
			"capacity_weight": 1,
			"controller": {
				"role": "leader"
			},
			"slot_stats": {
				"count": 3,
				"leader_count": 2
			}
		}]
	}`, rec.Body.String())
}

func TestManagerNodesReturnsServiceUnavailableWhenLeaderConsistentReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{nodesErr: raftcluster.ErrNoLeader},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"controller leader consistent read unavailable"}`, rec.Body.String())
}

func TestManagerSlotsRejectsMissingToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerSlotsRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerSlotsReturnsAggregatedList(t *testing.T) {
	lastReportAt := time.Date(2026, 4, 21, 16, 0, 0, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			slots: []managementusecase.Slot{{
				SlotID: 2,
				State: managementusecase.SlotState{
					Quorum: "ready",
					Sync:   "matched",
				},
				Assignment: managementusecase.SlotAssignment{
					DesiredPeers:   []uint64{1, 2, 3},
					ConfigEpoch:    8,
					BalanceVersion: 3,
				},
				Runtime: managementusecase.SlotRuntime{
					CurrentPeers:        []uint64{1, 2, 3},
					LeaderID:            1,
					HealthyVoters:       3,
					HasQuorum:           true,
					ObservedConfigEpoch: 8,
					LastReportAt:        lastReportAt,
				},
			}},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"total": 1,
		"items": [{
			"slot_id": 2,
			"state": {
				"quorum": "ready",
				"sync": "matched"
			},
			"assignment": {
				"desired_peers": [1, 2, 3],
				"config_epoch": 8,
				"balance_version": 3
			},
			"runtime": {
				"current_peers": [1, 2, 3],
				"leader_id": 1,
				"healthy_voters": 3,
				"has_quorum": true,
				"observed_config_epoch": 8,
				"last_report_at": "2026-04-21T16:00:00Z"
			}
		}]
	}`, rec.Body.String())
}

func TestManagerSlotsReturnsServiceUnavailableWhenLeaderConsistentReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{slotsErr: context.DeadlineExceeded},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"controller leader consistent read unavailable"}`, rec.Body.String())
}

func TestManagerSlotDetailRejectsMissingToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/2", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerSlotDetailRejectsInvalidSlotID(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/bad", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"bad_request","message":"invalid slot_id"}`, rec.Body.String())
}

func TestManagerSlotDetailRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerSlotDetailReturnsDetailWithTaskSummary(t *testing.T) {
	nextRunAt := time.Date(2026, 4, 21, 16, 5, 0, 0, time.UTC)
	lastReportAt := time.Date(2026, 4, 21, 16, 0, 0, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			slotDetail: managementusecase.SlotDetail{
				Slot: managementusecase.Slot{
					SlotID: 2,
					State: managementusecase.SlotState{
						Quorum: "ready",
						Sync:   "matched",
					},
					Assignment: managementusecase.SlotAssignment{
						DesiredPeers:   []uint64{2, 3, 5},
						ConfigEpoch:    8,
						BalanceVersion: 3,
					},
					Runtime: managementusecase.SlotRuntime{
						CurrentPeers:        []uint64{2, 3, 5},
						LeaderID:            2,
						HealthyVoters:       3,
						HasQuorum:           true,
						ObservedConfigEpoch: 8,
						LastReportAt:        lastReportAt,
					},
				},
				Task: &managementusecase.Task{
					SlotID:     2,
					Kind:       "repair",
					Step:       "catch_up",
					Status:     "retrying",
					SourceNode: 3,
					TargetNode: 5,
					Attempt:    1,
					NextRunAt:  &nextRunAt,
					LastError:  "learner catch-up timeout",
				},
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"slot_id": 2,
		"state": {
			"quorum": "ready",
			"sync": "matched"
		},
		"assignment": {
			"desired_peers": [2, 3, 5],
			"config_epoch": 8,
			"balance_version": 3
		},
		"runtime": {
			"current_peers": [2, 3, 5],
			"leader_id": 2,
			"healthy_voters": 3,
			"has_quorum": true,
			"observed_config_epoch": 8,
			"last_report_at": "2026-04-21T16:00:00Z"
		},
		"task": {
			"kind": "repair",
			"step": "catch_up",
			"status": "retrying",
			"source_node": 3,
			"target_node": 5,
			"attempt": 1,
			"next_run_at": "2026-04-21T16:05:00Z",
			"last_error": "learner catch-up timeout"
		}
	}`, rec.Body.String())
}

func TestManagerSlotDetailReturnsDetailWithNullTask(t *testing.T) {
	lastReportAt := time.Date(2026, 4, 21, 16, 0, 0, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			slotDetail: managementusecase.SlotDetail{
				Slot: managementusecase.Slot{
					SlotID: 7,
					State: managementusecase.SlotState{
						Quorum: "lost",
						Sync:   "unreported",
					},
					Runtime: managementusecase.SlotRuntime{
						CurrentPeers:        []uint64{7, 8, 9},
						LeaderID:            8,
						HealthyVoters:       2,
						HasQuorum:           false,
						ObservedConfigEpoch: 0,
						LastReportAt:        lastReportAt,
					},
				},
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/7", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"slot_id": 7,
		"state": {
			"quorum": "lost",
			"sync": "unreported"
		},
		"assignment": {
			"desired_peers": null,
			"config_epoch": 0,
			"balance_version": 0
		},
		"runtime": {
			"current_peers": [7, 8, 9],
			"leader_id": 8,
			"healthy_voters": 2,
			"has_quorum": false,
			"observed_config_epoch": 0,
			"last_report_at": "2026-04-21T16:00:00Z"
		},
		"task": null
	}`, rec.Body.String())
}

func TestManagerSlotDetailReturnsNotFound(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{slotDetailErr: controllermeta.ErrNotFound},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"error":"not_found","message":"slot not found"}`, rec.Body.String())
}

func TestManagerSlotDetailReturnsServiceUnavailableWhenLeaderConsistentReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{slotDetailErr: context.DeadlineExceeded},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/slots/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"controller leader consistent read unavailable"}`, rec.Body.String())
}

func TestManagerTasksRejectsMissingToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerTasksRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerTasksReturnsAggregatedList(t *testing.T) {
	nextRunAt := time.Date(2026, 4, 21, 16, 5, 0, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			tasks: []managementusecase.Task{{
				SlotID:     2,
				Kind:       "repair",
				Step:       "catch_up",
				Status:     "retrying",
				SourceNode: 3,
				TargetNode: 5,
				Attempt:    1,
				NextRunAt:  &nextRunAt,
				LastError:  "learner catch-up timeout",
			}},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"total": 1,
		"items": [{
			"slot_id": 2,
			"kind": "repair",
			"step": "catch_up",
			"status": "retrying",
			"source_node": 3,
			"target_node": 5,
			"attempt": 1,
			"next_run_at": "2026-04-21T16:05:00Z",
			"last_error": "learner catch-up timeout"
		}]
	}`, rec.Body.String())
}

func TestManagerTasksReturnsServiceUnavailableWhenLeaderConsistentReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{tasksErr: raftcluster.ErrNoLeader},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"controller leader consistent read unavailable"}`, rec.Body.String())
}

func TestManagerTaskDetailRejectsMissingToken(t *testing.T) {
	srv := New(Options{
		Auth:       testAuthConfig(nil),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/2", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"error":"unauthorized","message":"unauthorized"}`, rec.Body.String())
}

func TestManagerTaskDetailRejectsInvalidSlotID(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/bad", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"bad_request","message":"invalid slot_id"}`, rec.Body.String())
}

func TestManagerTaskDetailRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.node",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerTaskDetailReturnsTaskWithSlotContext(t *testing.T) {
	nextRunAt := time.Date(2026, 4, 21, 16, 5, 0, 0, time.UTC)
	lastReportAt := time.Date(2026, 4, 21, 16, 0, 0, 0, time.UTC)
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			task: managementusecase.TaskDetail{
				Task: managementusecase.Task{
					SlotID:     2,
					Kind:       "repair",
					Step:       "catch_up",
					Status:     "retrying",
					SourceNode: 3,
					TargetNode: 5,
					Attempt:    1,
					NextRunAt:  &nextRunAt,
					LastError:  "learner catch-up timeout",
				},
				Slot: managementusecase.Slot{
					SlotID: 2,
					State: managementusecase.SlotState{
						Quorum: "ready",
						Sync:   "matched",
					},
					Assignment: managementusecase.SlotAssignment{
						DesiredPeers:   []uint64{2, 3, 5},
						ConfigEpoch:    8,
						BalanceVersion: 3,
					},
					Runtime: managementusecase.SlotRuntime{
						CurrentPeers:        []uint64{2, 3, 5},
						LeaderID:            2,
						HealthyVoters:       3,
						HasQuorum:           true,
						ObservedConfigEpoch: 8,
						LastReportAt:        lastReportAt,
					},
				},
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"slot_id": 2,
		"kind": "repair",
		"step": "catch_up",
		"status": "retrying",
		"source_node": 3,
		"target_node": 5,
		"attempt": 1,
		"next_run_at": "2026-04-21T16:05:00Z",
		"last_error": "learner catch-up timeout",
		"slot": {
			"state": {
				"quorum": "ready",
				"sync": "matched"
			},
			"assignment": {
				"desired_peers": [2, 3, 5],
				"config_epoch": 8,
				"balance_version": 3
			},
			"runtime": {
				"current_peers": [2, 3, 5],
				"leader_id": 2,
				"healthy_voters": 3,
				"has_quorum": true,
				"observed_config_epoch": 8,
				"last_report_at": "2026-04-21T16:00:00Z"
			}
		}
	}`, rec.Body.String())
}

func TestManagerTaskDetailReturnsNotFound(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{taskErr: controllermeta.ErrNotFound},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"error":"not_found","message":"task not found"}`, rec.Body.String())
}

func TestManagerTaskDetailReturnsServiceUnavailableWhenLeaderConsistentReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.task",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{taskErr: context.DeadlineExceeded},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/tasks/2", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"controller leader consistent read unavailable"}`, rec.Body.String())
}

func TestManagerChannelRuntimeMetaRejectsInsufficientPermission(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "viewer",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.slot",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/channel-runtime-meta", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "viewer"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.JSONEq(t, `{"error":"forbidden","message":"forbidden"}`, rec.Body.String())
}

func TestManagerChannelRuntimeMetaRejectsInvalidLimit(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.channel",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/channel-runtime-meta?limit=0", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"bad_request","message":"invalid limit"}`, rec.Body.String())
}

func TestManagerChannelRuntimeMetaRejectsInvalidCursor(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.channel",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/channel-runtime-meta?cursor=not-base64", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"bad_request","message":"invalid cursor"}`, rec.Body.String())
}

func TestManagerChannelRuntimeMetaReturnsPagedList(t *testing.T) {
	var received managementusecase.ListChannelRuntimeMetaRequest
	inputCursor := managementusecase.ChannelRuntimeMetaListCursor{SlotID: 1, ChannelID: "g1", ChannelType: 1}
	nextCursor := managementusecase.ChannelRuntimeMetaListCursor{SlotID: 2, ChannelID: "g2", ChannelType: 2}
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.channel",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{
			channelRuntimeMetaReqSink: &received,
			channelRuntimeMetaPage: managementusecase.ListChannelRuntimeMetaResponse{
				Items: []managementusecase.ChannelRuntimeMeta{{
					ChannelID:    "g2",
					ChannelType:  2,
					SlotID:       2,
					ChannelEpoch: 11,
					LeaderEpoch:  5,
					Leader:       3,
					Replicas:     []uint64{3, 4},
					ISR:          []uint64{3},
					MinISR:       1,
					Status:       "active",
				}},
				HasMore:    true,
				NextCursor: nextCursor,
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/channel-runtime-meta?limit=2&cursor="+mustEncodeChannelRuntimeMetaCursorForTest(t, inputCursor), nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, managementusecase.ListChannelRuntimeMetaRequest{
		Limit:  2,
		Cursor: inputCursor,
	}, received)
	require.NotContains(t, rec.Body.String(), `"total"`)
	require.JSONEq(t, fmt.Sprintf(`{
		"items": [{
			"channel_id": "g2",
			"channel_type": 2,
			"slot_id": 2,
			"channel_epoch": 11,
			"leader_epoch": 5,
			"leader": 3,
			"replicas": [3, 4],
			"isr": [3],
			"min_isr": 1,
			"status": "active"
		}],
		"has_more": true,
		"next_cursor": %q
	}`, mustEncodeChannelRuntimeMetaCursorForTest(t, nextCursor)), rec.Body.String())
}

func TestManagerChannelRuntimeMetaReturnsServiceUnavailableWhenAuthoritativeReadUnavailable(t *testing.T) {
	srv := New(Options{
		Auth: testAuthConfig([]UserConfig{{
			Username: "admin",
			Password: "secret",
			Permissions: []PermissionConfig{{
				Resource: "cluster.channel",
				Actions:  []string{"r"},
			}},
		}}),
		Management: managementStub{channelRuntimeMetaErr: raftcluster.ErrSlotNotFound},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/manager/channel-runtime-meta", nil)
	req.Header.Set("Authorization", "Bearer "+mustIssueTestToken(t, srv, "admin"))

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"service_unavailable","message":"slot leader authoritative read unavailable"}`, rec.Body.String())
}

func testAuthConfig(users []UserConfig) AuthConfig {
	return AuthConfig{
		On:        true,
		JWTSecret: "test-secret",
		JWTIssuer: "wukongim-manager",
		JWTExpire: time.Hour,
		Users:     users,
	}
}

func mustIssueTestToken(t *testing.T, srv *Server, username string) string {
	t.Helper()
	token, err := srv.issueToken(username, time.Now())
	require.NoError(t, err)
	return token
}

func mustIssueExpiredTestToken(t *testing.T, srv *Server, username string) string {
	t.Helper()
	token, err := srv.issueToken(username, time.Now().Add(-2*srv.auth.jwtExpire))
	require.NoError(t, err)
	return token
}

func mustEncodeChannelRuntimeMetaCursorForTest(t *testing.T, cursor managementusecase.ChannelRuntimeMetaListCursor) string {
	t.Helper()
	payload, err := json.Marshal(channelRuntimeMetaCursorPayload{
		SlotID:      cursor.SlotID,
		ChannelID:   cursor.ChannelID,
		ChannelType: cursor.ChannelType,
	})
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(payload)
}

type managementStub struct {
	nodes                     []managementusecase.Node
	nodesErr                  error
	slots                     []managementusecase.Slot
	slotsErr                  error
	slotDetail                managementusecase.SlotDetail
	slotDetailErr             error
	tasks                     []managementusecase.Task
	tasksErr                  error
	task                      managementusecase.TaskDetail
	taskErr                   error
	channelRuntimeMetaReqSink *managementusecase.ListChannelRuntimeMetaRequest
	channelRuntimeMetaPage    managementusecase.ListChannelRuntimeMetaResponse
	channelRuntimeMetaErr     error
}

func (s managementStub) ListNodes(context.Context) ([]managementusecase.Node, error) {
	return append([]managementusecase.Node(nil), s.nodes...), s.nodesErr
}

func (s managementStub) ListSlots(context.Context) ([]managementusecase.Slot, error) {
	return append([]managementusecase.Slot(nil), s.slots...), s.slotsErr
}

func (s managementStub) GetSlot(context.Context, uint32) (managementusecase.SlotDetail, error) {
	return s.slotDetail, s.slotDetailErr
}

func (s managementStub) ListTasks(context.Context) ([]managementusecase.Task, error) {
	return append([]managementusecase.Task(nil), s.tasks...), s.tasksErr
}

func (s managementStub) GetTask(context.Context, uint32) (managementusecase.TaskDetail, error) {
	return s.task, s.taskErr
}

func (s managementStub) ListChannelRuntimeMeta(_ context.Context, req managementusecase.ListChannelRuntimeMetaRequest) (managementusecase.ListChannelRuntimeMetaResponse, error) {
	if s.channelRuntimeMetaReqSink != nil {
		*s.channelRuntimeMetaReqSink = req
	}
	return s.channelRuntimeMetaPage, s.channelRuntimeMetaErr
}

type loginResponseBody struct {
	Username    string           `json:"username"`
	TokenType   string           `json:"token_type"`
	AccessToken string           `json:"access_token"`
	ExpiresIn   int64            `json:"expires_in"`
	ExpiresAt   time.Time        `json:"expires_at"`
	Permissions []permissionBody `json:"permissions"`
}

type permissionBody struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}
