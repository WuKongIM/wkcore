package node

import (
	"context"
	"encoding/json"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type deliveryRequest struct {
	UID        string   `json:"uid"`
	GroupID    uint64   `json:"group_id,omitempty"`
	BootID     uint64   `json:"boot_id"`
	SessionIDs []uint64 `json:"session_ids"`
	Frame      []byte   `json:"frame"`
}

type deliveryResponse struct {
	Status string `json:"status"`
}

func (a *Adapter) handleDeliveryRPC(ctx context.Context, body []byte) ([]byte, error) {
	_ = ctx
	var req deliveryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.BootID != a.gatewayBootID {
		return encodeDeliveryResponse(deliveryResponse{Status: rpcStatusOK})
	}
	frame, _, err := a.codec.DecodeFrame(req.Frame, wkframe.LatestVersion)
	if err != nil {
		return nil, err
	}
	for _, sessionID := range req.SessionIDs {
		conn, ok := a.online.Connection(sessionID)
		if !ok || conn.UID != req.UID || conn.State != online.LocalRouteStateActive {
			continue
		}
		_ = conn.Session.WriteFrame(frame)
	}
	return encodeDeliveryResponse(deliveryResponse{Status: rpcStatusOK})
}

func encodeDeliveryResponse(resp deliveryResponse) ([]byte, error) {
	return json.Marshal(resp)
}
