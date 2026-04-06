package node

import (
	"context"
	"encoding/json"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
)

type deliverySubmitRequest struct {
	Envelope message.CommittedMessageEnvelope `json:"envelope"`
}

func (a *Adapter) handleDeliverySubmitRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req deliverySubmitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if a.deliverySubmit != nil {
		if err := a.deliverySubmit.SubmitCommitted(ctx, req.Envelope); err != nil {
			return nil, err
		}
	}
	return encodeDeliveryResponse(deliveryResponse{Status: rpcStatusOK})
}
