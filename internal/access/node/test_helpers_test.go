package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func mustEncodeFrame(t *testing.T, frame wkframe.Frame) []byte {
	t.Helper()
	body, err := wkcodec.New().EncodeFrame(frame, wkframe.LatestVersion)
	require.NoError(t, err)
	return body
}

func mustDecodeDeliveryResponse(t *testing.T, body []byte) deliveryResponse {
	t.Helper()
	resp, err := decodeDeliveryResponse(body)
	require.NoError(t, err)
	return resp
}

func testOnlineConn(sessionID uint64, uid string, groupID uint64) online.OnlineConn {
	return online.OnlineConn{
		SessionID:   sessionID,
		UID:         uid,
		DeviceID:    "d1",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		GroupID:     groupID,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		ConnectedAt: time.Unix(200, 0),
		Session:     session.New(session.Config{ID: sessionID, Listener: "tcp"}),
	}
}
