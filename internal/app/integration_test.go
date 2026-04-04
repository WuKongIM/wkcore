package app

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

const appReadTimeout = 2 * time.Second

func TestAppStartAcceptsWKProtoConnectionAndStopsCleanly(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	conn, err := net.Dial("tcp", app.Gateway().ListenerAddr("tcp-wkproto"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sendAppWKProtoFrame(t, conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             "app-user",
		DeviceID:        "app-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	})

	frame := readAppWKProtoFrame(t, conn)
	connack, ok := frame.(*wkframe.ConnackPacket)
	require.True(t, ok, "expected *wkframe.ConnackPacket, got %T", frame)
	require.Equal(t, wkframe.ReasonSuccess, connack.ReasonCode)
}

func TestAppStartPreloadsLocalChannelRuntimeMeta(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	key := channellog.ChannelKey{ChannelID: "preload-user", ChannelType: 1}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 3,
		LeaderEpoch:  4,
		Replicas:     []uint64{cfg.Node.ID},
		ISR:          []uint64{cfg.Node.ID},
		Leader:       cfg.Node.ID,
		MinISR:       1,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, app.DB().ForSlot(1).UpsertChannelRuntimeMeta(context.Background(), meta))

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	status, err := app.ChannelLog().Status(key)
	require.NoError(t, err)
	require.Equal(t, key, status.Key)
	require.Equal(t, channellog.ChannelStatusActive, status.Status)
	require.Equal(t, channellog.NodeID(cfg.Node.ID), status.Leader)
	require.Equal(t, uint64(4), status.LeaderEpoch)
}

func TestAppStartWiresMessageSendThroughDurableChannelLog(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	key := channellog.ChannelKey{ChannelID: "durable-user", ChannelType: wkframe.ChannelTypePerson}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 9,
		LeaderEpoch:  10,
		Replicas:     []uint64{cfg.Node.ID},
		ISR:          []uint64{cfg.Node.ID},
		Leader:       cfg.Node.ID,
		MinISR:       1,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	require.NoError(t, app.DB().ForSlot(1).UpsertChannelRuntimeMeta(context.Background(), meta))

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	result, err := app.Message().Send(context.Background(), message.SendCommand{
		SenderUID:   "sender",
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		ClientMsgNo: "durable-1",
		Payload:     []byte("hello durable"),
	})
	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.NotZero(t, result.MessageID)
	require.Equal(t, uint64(1), result.MessageSeq)

	fetch, err := app.ChannelLog().Fetch(context.Background(), channellog.FetchRequest{
		Key:      key,
		FromSeq:  1,
		Limit:    10,
		MaxBytes: 1024,
	})
	require.NoError(t, err)
	require.Len(t, fetch.Messages, 1)
	require.Equal(t, uint64(1), fetch.Messages[0].MessageSeq)
	require.Equal(t, []byte("hello durable"), fetch.Messages[0].Payload)
}

func TestAppStartServesLegacyUserTokenEndpoint(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.ListenAddr = "127.0.0.1:0"
	cfg.API.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, app.Start())
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})
	require.Eventually(t, func() bool {
		_, err := app.Cluster().LeaderOf(1)
		return err == nil
	}, 3*time.Second, 50*time.Millisecond)

	req, err := http.NewRequest(http.MethodPost, "http://"+app.API().Addr()+"/user/token", bytes.NewBufferString(`{"uid":"token-user","token":"token-1","device_flag":1,"device_level":1}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.JSONEq(t, `{"status":200}`, string(body))

	gotUser, err := app.Store().GetUser(context.Background(), "token-user")
	require.NoError(t, err)
	require.Equal(t, "token-user", gotUser.UID)

	gotDevice, err := app.Store().GetDevice(context.Background(), "token-user", 1)
	require.NoError(t, err)
	require.Equal(t, metadb.Device{
		UID:         "token-user",
		DeviceFlag:  1,
		Token:       "token-1",
		DeviceLevel: 1,
	}, gotDevice)
}

func sendAppWKProtoFrame(t *testing.T, conn net.Conn, frame wkframe.Frame) {
	t.Helper()

	payload, err := codec.New().EncodeFrame(frame, wkframe.LatestVersion)
	require.NoError(t, err)

	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readAppWKProtoFrame(t *testing.T, conn net.Conn) wkframe.Frame {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(appReadTimeout)))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	frame, err := codec.New().DecodePacketWithConn(conn, wkframe.LatestVersion)
	require.NoError(t, err)
	return frame
}
