package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
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

type managementStub struct {
	nodes []managementusecase.Node
	err   error
}

func (s managementStub) ListNodes(context.Context) ([]managementusecase.Node, error) {
	return append([]managementusecase.Node(nil), s.nodes...), s.err
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
