package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestAppConversationSyncReturnsLegacyConversationAfterSend(t *testing.T) {
	cfg := testConfig(t)
	cfg.API.ListenAddr = "127.0.0.1:0"
	cfg.Conversation.SyncEnabled = true

	app, err := New(cfg)
	require.NoError(t, err)
	channelID := deliveryusecase.EncodePersonChannel("u1", "u2")
	seedChannelRuntimeMeta(t, app, channelID, wkframe.ChannelTypePerson)

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	_, err = app.Message().Send(context.Background(), message.SendCommand{
		FromUID:     "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientMsgNo: "conversation-sync-1",
		Payload:     []byte("hello sync"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversation/sync", bytes.NewBufferString(`{"uid":"u2","limit":10}`))
		req.Header.Set("Content-Type", "application/json")

		app.API().Engine().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}

		var got []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			return false
		}
		if len(got) != 1 {
			return false
		}
		return got[0]["channel_id"] == "u1" &&
			got[0]["last_client_msg_no"] == "conversation-sync-1" &&
			got[0]["last_msg_seq"] == float64(1) &&
			got[0]["unread"] == float64(1)
	}, 3*time.Second, 20*time.Millisecond)
}
