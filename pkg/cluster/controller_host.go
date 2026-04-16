package cluster

import (
	"context"
	"fmt"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type controllerHost struct {
	meta         *controllermeta.Store
	raftDB       *raftstorage.DB
	sm           *slotcontroller.StateMachine
	service      *controllerraft.Service
	observations *observationCache
}

func newControllerHost(cfg Config, layer *transportLayer) (*controllerHost, error) {
	meta, err := controllermeta.Open(cfg.ControllerMetaPath)
	if err != nil {
		return nil, fmt.Errorf("open controller meta: %w", err)
	}
	logDB, err := raftstorage.Open(cfg.ControllerRaftPath)
	if err != nil {
		_ = meta.Close()
		return nil, fmt.Errorf("open controller raft: %w", err)
	}

	peers := cfg.DerivedControllerNodes()
	controllerPeers := make([]controllerraft.Peer, 0, len(peers))
	for _, peer := range peers {
		controllerPeers = append(controllerPeers, controllerraft.Peer{
			NodeID: uint64(peer.NodeID),
			Addr:   peer.Addr,
		})
	}

	sm := slotcontroller.NewStateMachine(meta, slotcontroller.StateMachineConfig{})
	service := controllerraft.NewService(controllerraft.Config{
		NodeID:         uint64(cfg.NodeID),
		Peers:          controllerPeers,
		AllowBootstrap: true,
		LogDB:          logDB,
		StateMachine:   sm,
		Server:         layer.server,
		RPCMux:         layer.rpcMux,
		Pool:           layer.raftPool,
	})

	return &controllerHost{
		meta:         meta,
		raftDB:       logDB,
		sm:           sm,
		service:      service,
		observations: newObservationCache(),
	}, nil
}

func (h *controllerHost) Start(ctx context.Context) error {
	if h == nil || h.service == nil {
		return nil
	}
	return h.service.Start(ctx)
}

func (h *controllerHost) Stop() {
	if h == nil {
		return
	}
	if h.service != nil {
		_ = h.service.Stop()
	}
	if h.raftDB != nil {
		_ = h.raftDB.Close()
	}
	if h.meta != nil {
		_ = h.meta.Close()
	}
}

func (h *controllerHost) IsLeader(local multiraft.NodeID) bool {
	return h != nil && h.LeaderID() == local
}

func (h *controllerHost) LeaderID() multiraft.NodeID {
	if h == nil || h.service == nil {
		return 0
	}
	return multiraft.NodeID(h.service.LeaderID())
}

func (h *controllerHost) applyObservation(report slotcontroller.AgentReport) {
	if h == nil || h.observations == nil {
		return
	}
	h.observations.applyNodeReport(report)
	if report.Runtime != nil {
		h.observations.applyRuntimeView(*report.Runtime)
	}
}

func (h *controllerHost) snapshotObservations() observationSnapshot {
	if h == nil || h.observations == nil {
		return observationSnapshot{}
	}
	return h.observations.snapshot()
}
