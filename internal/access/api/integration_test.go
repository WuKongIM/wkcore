package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
	metadb "github.com/WuKongIM/WuKongIM/pkg/group/meta"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestAPIServerSendMessageWithRealMessageApp(t *testing.T) {
	msgApp := message.New(message.Options{
		IdentityStore: &fakeIdentityStore{},
		ChannelStore:  &fakeChannelStore{},
		Cluster: &fakeChannelCluster{
			result: channellog.AppendResult{MessageID: 66, MessageSeq: 7},
		},
	})
	require.NoError(t, msgApp.OnlineRegistry().Register(online.OnlineConn{
		SessionID: 2,
		UID:       "u2",
		Session:   session.New(session.Config{ID: 2, Listener: "api"}),
	}))

	srv := New(Options{
		ListenAddr: "127.0.0.1:0",
		Messages:   msgApp,
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		require.NoError(t, srv.Stop(context.Background()))
	})

	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + srv.Addr() + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 20*time.Millisecond)

	body := map[string]any{
		"from_uid":     "u1",
		"channel_id":   "u2",
		"channel_type": float64(frame.ChannelTypePerson),
		"payload":      base64.StdEncoding.EncodeToString([]byte("hi")),
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	resp, err := http.Post("http://"+srv.Addr()+"/message/send", "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got struct {
		MessageID  int64  `json:"message_id"`
		MessageSeq uint64 `json:"message_seq"`
		Reason     uint8  `json:"reason"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, int64(66), got.MessageID)
	require.Equal(t, uint64(7), got.MessageSeq)
	require.Equal(t, uint8(frame.ReasonSuccess), got.Reason)
}

type fakeIdentityStore struct{}

func (*fakeIdentityStore) GetUser(context.Context, string) (metadb.User, error) {
	return metadb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (metadb.Channel, error) {
	return metadb.Channel{}, nil
}

type fakeChannelCluster struct {
	result channellog.AppendResult
	err    error
}

func (*fakeChannelCluster) ApplyMeta(channellog.ChannelMeta) error {
	return nil
}

func (f *fakeChannelCluster) Append(context.Context, channellog.AppendRequest) (channellog.AppendResult, error) {
	return f.result, f.err
}
