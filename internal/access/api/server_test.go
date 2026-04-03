package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/stretchr/testify/require"
)

func TestHealthzReturnsOK(t *testing.T) {
	srv := New(Options{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

func TestSendMessageMapsJSONToUsecaseCommand(t *testing.T) {
	msgs := &recordingMessageUsecase{
		result: message.SendResult{
			MessageID:  99,
			MessageSeq: uint64(^uint32(0)) + 7,
			Reason:     wkframe.ReasonSuccess,
		},
	}
	srv := New(Options{Messages: msgs})

	body := map[string]any{
		"sender_uid":   "u1",
		"channel_id":   "u2",
		"channel_type": float64(wkframe.ChannelTypePerson),
		"payload":      base64.StdEncoding.EncodeToString([]byte("hi")),
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/messages/send", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"message_id":99,"message_seq":4294967302,"reason":1}`, rec.Body.String())
	require.Len(t, msgs.calls, 1)
	require.Equal(t, "u1", msgs.calls[0].SenderUID)
	require.Equal(t, "u2", msgs.calls[0].ChannelID)
	require.Equal(t, uint8(wkframe.ChannelTypePerson), msgs.calls[0].ChannelType)
	require.Equal(t, []byte("hi"), msgs.calls[0].Payload)
}

func TestSendMessageRejectsInvalidBase64Payload(t *testing.T) {
	srv := New(Options{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/messages/send", bytes.NewBufferString(`{"sender_uid":"u1","channel_id":"u2","channel_type":1,"payload":"not-base64"}`))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"invalid payload"}`, rec.Body.String())
}

func TestSendMessageRejectsInvalidJSON(t *testing.T) {
	srv := New(Options{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/messages/send", bytes.NewBufferString(`{"sender_uid":`))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"error":"invalid request"}`, rec.Body.String())
}

func TestSendMessageReturnsInternalServerErrorWhenUsecaseFails(t *testing.T) {
	msgs := &recordingMessageUsecase{err: errors.New("boom")}
	srv := New(Options{Messages: msgs})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/messages/send", bytes.NewBufferString(`{"sender_uid":"u1","channel_id":"u2","channel_type":1,"payload":"aGk="}`))
	req.Header.Set("Content-Type", "application/json")

	srv.Engine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.JSONEq(t, `{"error":"boom"}`, rec.Body.String())
}

func TestSendMessageMapsSemanticErrorsToHTTPStatus(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		body   string
	}{
		{
			name:   "channel not found",
			err:    channellog.ErrChannelNotFound,
			status: http.StatusNotFound,
			body:   `{"error":"channel not found"}`,
		},
		{
			name:   "channel deleting",
			err:    channellog.ErrChannelDeleting,
			status: http.StatusConflict,
			body:   `{"error":"channel deleting"}`,
		},
		{
			name:   "protocol upgrade required",
			err:    channellog.ErrProtocolUpgradeRequired,
			status: http.StatusUpgradeRequired,
			body:   `{"error":"protocol upgrade required"}`,
		},
		{
			name:   "idempotency conflict",
			err:    channellog.ErrIdempotencyConflict,
			status: http.StatusConflict,
			body:   `{"error":"idempotency conflict"}`,
		},
		{
			name:   "message seq exhausted",
			err:    channellog.ErrMessageSeqExhausted,
			status: http.StatusConflict,
			body:   `{"error":"message seq exhausted"}`,
		},
		{
			name:   "stale meta",
			err:    channellog.ErrStaleMeta,
			status: http.StatusServiceUnavailable,
			body:   `{"error":"retry required"}`,
		},
		{
			name:   "not leader",
			err:    channellog.ErrNotLeader,
			status: http.StatusServiceUnavailable,
			body:   `{"error":"retry required"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := New(Options{
				Messages: &recordingMessageUsecase{err: tt.err},
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/messages/send", bytes.NewBufferString(`{"sender_uid":"u1","channel_id":"u2","channel_type":1,"payload":"aGk="}`))
			req.Header.Set("Content-Type", "application/json")

			srv.Engine().ServeHTTP(rec, req)

			require.Equal(t, tt.status, rec.Code)
			require.JSONEq(t, tt.body, rec.Body.String())
		})
	}
}

type recordingMessageUsecase struct {
	calls  []message.SendCommand
	result message.SendResult
	err    error
}

func (r *recordingMessageUsecase) Send(cmd message.SendCommand) (message.SendResult, error) {
	r.calls = append(r.calls, cmd)
	return r.result, r.err
}
