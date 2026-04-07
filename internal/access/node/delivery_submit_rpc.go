package node

import (
	"context"
	"encoding/json"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

type deliverySubmitRequest struct {
	Message channellog.Message `json:"message"`
}

func (a *Adapter) handleDeliverySubmitRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req deliverySubmitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if a.deliverySubmit != nil {
		if err := a.deliverySubmit.SubmitCommitted(ctx, req.Message); err != nil {
			return nil, err
		}
	}
	return encodeDeliveryResponse(deliveryResponse{Status: rpcStatusOK})
}
