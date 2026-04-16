package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type controllerHost struct {
	meta            *controllermeta.Store
	raftDB          *raftstorage.DB
	sm              *slotcontroller.StateMachine
	service         *controllerraft.Service
	observations    *observationCache
	healthScheduler *nodeHealthScheduler
	localNode       multiraft.NodeID
	hashSlotMu      sync.RWMutex
	hashSlotTable   *HashSlotTable

	warmupMu         sync.RWMutex
	warmupLeaderID   multiraft.NodeID
	warmupGeneration uint64
	warmupReady      bool
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
	host := &controllerHost{
		meta:         meta,
		raftDB:       logDB,
		sm:           sm,
		observations: newObservationCache(),
		localNode:    cfg.NodeID,
	}
	host.healthScheduler = newNodeHealthScheduler(nodeHealthSchedulerConfig{
		suspectTimeout: 3 * time.Second,
		deadTimeout:    10 * time.Second,
		loadNode:       host.meta.GetNode,
	})
	service := controllerraft.NewService(controllerraft.Config{
		NodeID:         uint64(cfg.NodeID),
		Peers:          controllerPeers,
		AllowBootstrap: true,
		LogDB:          logDB,
		StateMachine:   sm,
		Server:         layer.server,
		RPCMux:         layer.rpcMux,
		Pool:           layer.raftPool,
		OnLeaderChange: func(from, to uint64) {
			host.handleLeaderChange(multiraft.NodeID(from), multiraft.NodeID(to))
		},
		OnCommittedCommand: func(cmd slotcontroller.Command) {
			host.handleCommittedCommand(cmd)
		},
	})
	host.service = service
	host.healthScheduler.cfg.propose = func(ctx context.Context, cmd slotcontroller.Command) error {
		return host.service.Propose(ctx, cmd)
	}
	return host, nil
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
	if h.healthScheduler != nil {
		h.healthScheduler.reset()
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
	h.syncLeaderWarmupState()
	h.observations.applyNodeReport(report)
	if report.Runtime != nil {
		h.observations.applyRuntimeView(*report.Runtime)
	}
	h.markWarmupReady()
	if h.healthScheduler != nil {
		h.healthScheduler.observe(nodeObservation{
			NodeID:               report.NodeID,
			Addr:                 report.Addr,
			ObservedAt:           report.ObservedAt,
			CapacityWeight:       report.CapacityWeight,
			HashSlotTableVersion: report.HashSlotTableVersion,
		})
	}
}

func (h *controllerHost) snapshotObservations() observationSnapshot {
	if h == nil || h.observations == nil {
		return observationSnapshot{}
	}
	return h.observations.snapshot()
}

func (h *controllerHost) hashSlotTableSnapshot() (*HashSlotTable, bool) {
	if h == nil {
		return nil, false
	}

	h.hashSlotMu.RLock()
	defer h.hashSlotMu.RUnlock()

	if h.hashSlotTable == nil {
		return nil, false
	}
	return h.hashSlotTable, true
}

func (h *controllerHost) storeHashSlotTableSnapshot(table *HashSlotTable) {
	if h == nil {
		return
	}

	h.hashSlotMu.Lock()
	defer h.hashSlotMu.Unlock()

	if table == nil {
		h.hashSlotTable = nil
		return
	}
	h.hashSlotTable = table.Clone()
}

func (h *controllerHost) clearHashSlotTableSnapshot() {
	if h == nil {
		return
	}

	h.hashSlotMu.Lock()
	defer h.hashSlotMu.Unlock()
	h.hashSlotTable = nil
}

func (h *controllerHost) reloadHashSlotTableSnapshot(ctx context.Context) error {
	if h == nil || h.meta == nil {
		return nil
	}
	table, err := h.meta.LoadHashSlotTable(ctx)
	if err != nil {
		h.clearHashSlotTableSnapshot()
		return err
	}
	h.storeHashSlotTableSnapshot(table)
	return nil
}

func (h *controllerHost) warmupComplete() bool {
	if h == nil {
		return false
	}
	h.syncLeaderWarmupState()

	h.warmupMu.RLock()
	defer h.warmupMu.RUnlock()
	return h.warmupLeaderID == h.localNode && h.warmupReady
}

func (h *controllerHost) plannerSnapshot() (observationSnapshot, bool) {
	if h == nil {
		return observationSnapshot{}, false
	}
	if !h.warmupComplete() {
		return observationSnapshot{}, false
	}
	return h.snapshotObservations(), true
}

func (h *controllerHost) syncLeaderWarmupState() {
	if h == nil {
		return
	}

	leaderID := h.LeaderID()

	h.warmupMu.Lock()
	defer h.warmupMu.Unlock()

	if leaderID != h.warmupLeaderID {
		h.warmupLeaderID = leaderID
		h.warmupReady = false
		if leaderID == h.localNode {
			h.warmupGeneration++
		}
		return
	}
	if leaderID != h.localNode {
		h.warmupReady = false
	}
}

func (h *controllerHost) markWarmupReady() {
	if h == nil {
		return
	}

	h.warmupMu.Lock()
	defer h.warmupMu.Unlock()

	if h.warmupLeaderID == h.localNode {
		h.warmupReady = true
	}
}

func (h *controllerHost) handleLeaderChange(_, to multiraft.NodeID) {
	if h == nil {
		return
	}
	if h.observations != nil {
		h.observations.reset()
	}
	if h.healthScheduler != nil {
		h.healthScheduler.reset()
	}
	if to == h.localNode {
		_ = h.reloadHashSlotTableSnapshot(context.Background())
	} else {
		h.clearHashSlotTableSnapshot()
	}

	h.warmupMu.Lock()
	defer h.warmupMu.Unlock()

	h.warmupLeaderID = to
	h.warmupReady = false
	if to == h.localNode {
		h.warmupGeneration++
	}
}

func (h *controllerHost) handleCommittedCommand(cmd slotcontroller.Command) {
	if h == nil {
		return
	}
	if shouldRefreshHashSlotSnapshot(cmd) {
		_ = h.reloadHashSlotTableSnapshot(context.Background())
	}
	if h.healthScheduler != nil {
		h.healthScheduler.handleCommittedCommand(cmd)
	}
}

func shouldRefreshHashSlotSnapshot(cmd slotcontroller.Command) bool {
	switch cmd.Kind {
	case slotcontroller.CommandKindStartMigration,
		slotcontroller.CommandKindAdvanceMigration,
		slotcontroller.CommandKindFinalizeMigration,
		slotcontroller.CommandKindAbortMigration,
		slotcontroller.CommandKindAddSlot,
		slotcontroller.CommandKindRemoveSlot:
		return true
	default:
		return false
	}
}
