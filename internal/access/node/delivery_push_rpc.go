package node

import (
	"context"
	"encoding/json"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type deliveryPushRequest struct {
	OwnerNodeID uint64                      `json:"owner_node_id"`
	ChannelID   string                      `json:"channel_id"`
	ChannelType uint8                       `json:"channel_type"`
	MessageID   uint64                      `json:"message_id"`
	MessageSeq  uint64                      `json:"message_seq"`
	Routes      []deliveryruntime.RouteKey  `json:"routes"`
	Frame       []byte                      `json:"frame"`
}

func (a *Adapter) handleDeliveryPushRPC(ctx context.Context, body []byte) ([]byte, error) {
	_ = ctx
	var req deliveryPushRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	frame, _, err := a.codec.DecodeFrame(req.Frame, wkframe.LatestVersion)
	if err != nil {
		return nil, err
	}

	resp := deliveryPushResponse{Status: rpcStatusOK}
	for _, route := range req.Routes {
		switch {
		case a.localNodeID != 0 && route.NodeID != a.localNodeID:
			resp.Dropped = append(resp.Dropped, route)
		case a.gatewayBootID != 0 && route.BootID != a.gatewayBootID:
			resp.Dropped = append(resp.Dropped, route)
		default:
			conn, ok := a.online.Connection(route.SessionID)
			if !ok || conn.UID != route.UID || conn.State != online.LocalRouteStateActive || conn.Session == nil {
				resp.Dropped = append(resp.Dropped, route)
				continue
			}
			if a.deliveryAckIndex != nil {
				a.deliveryAckIndex.Bind(deliveryruntime.AckBinding{
					SessionID:   route.SessionID,
					MessageID:   req.MessageID,
					ChannelID:   req.ChannelID,
					ChannelType: req.ChannelType,
					OwnerNodeID: req.OwnerNodeID,
					Route:       route,
				})
			}
			if err := conn.Session.WriteFrame(frame); err != nil {
				if a.deliveryAckIndex != nil {
					a.deliveryAckIndex.Remove(route.SessionID, req.MessageID)
				}
				resp.Retryable = append(resp.Retryable, route)
				continue
			}
			resp.Accepted = append(resp.Accepted, route)
		}
	}
	return encodeDeliveryPushResponse(resp)
}
