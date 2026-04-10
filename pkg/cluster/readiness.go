package cluster

import (
	"context"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const (
	controllerObservationInterval = 200 * time.Millisecond
	controllerRequestTimeout      = 2 * time.Second
)

func withControllerTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, controllerRequestTimeout)
}

func buildRuntimeView(now time.Time, slotID multiraft.SlotID, status multiraft.Status, peers []multiraft.NodeID) controllermeta.SlotRuntimeView {
	currentPeers := make([]uint64, 0, len(peers))
	for _, peer := range peers {
		currentPeers = append(currentPeers, uint64(peer))
	}

	view := controllermeta.SlotRuntimeView{
		SlotID:       uint32(slotID),
		CurrentPeers: currentPeers,
		LeaderID:     uint64(status.LeaderID),
		LastReportAt: now,
	}
	if len(currentPeers) > 0 {
		view.HealthyVoters = uint32(len(currentPeers))
		view.HasQuorum = status.LeaderID != 0
	}
	return view
}
