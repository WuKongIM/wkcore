package cluster

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type slotHandler struct {
	cluster *Cluster
}

func (h *slotHandler) Handle(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeManagedSlotRequest(body)
	if err != nil {
		return nil, err
	}
	if h == nil || h.cluster == nil {
		return nil, ErrNotStarted
	}

	switch req.Kind {
	case managedSlotRPCStatus:
		status, err := h.cluster.managedSlots().localStatus(multiraft.SlotID(req.SlotID))
		switch {
		case err == nil:
			return encodeManagedSlotResponse(managedSlotRPCResponse{
				LeaderID:     uint64(status.LeaderID),
				CommitIndex:  status.CommitIndex,
				AppliedIndex: status.AppliedIndex,
			})
		case errors.Is(err, ErrSlotNotFound), errors.Is(err, multiraft.ErrSlotNotFound):
			return encodeManagedSlotResponse(managedSlotRPCResponse{NotFound: true})
		default:
			return encodeManagedSlotResponse(managedSlotRPCResponse{Message: err.Error()})
		}
	case managedSlotRPCChangeConfig:
		err := h.cluster.managedSlots().changeConfigLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.ConfigChange{
			Type:   req.ChangeType,
			NodeID: multiraft.NodeID(req.NodeID),
		})
		return marshalManagedSlotError(err)
	case managedSlotRPCTransferLeader:
		err := h.cluster.managedSlots().transferLeaderLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.NodeID(req.TargetNode))
		return marshalManagedSlotError(err)
	default:
		return nil, ErrInvalidConfig
	}
}
