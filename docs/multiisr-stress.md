# MultiISR Stress Guide

## Benchmark Commands

Run the lightweight benchmark suite:

```bash
go test ./pkg/multiisr -run '^$' -bench 'BenchmarkRuntime(ReplicationScheduling|PeerQueueDrain|MixedReplicationAndSnapshot)$' -benchmem -count=1
```

## Stress Commands

Run the gated stress suite with the default scale (`groups=256`, `peers=8`):

```bash
MULTIISR_STRESS=1 MULTIISR_STRESS_DURATION=3s go test ./pkg/multiisr -run 'TestRuntimeStress(MixedScheduling|PeerQueueRecovery|SnapshotInterference)' -count=1 -v
```

Run a heavier variant:

```bash
MULTIISR_STRESS=1 \
MULTIISR_STRESS_DURATION=10s \
MULTIISR_STRESS_GROUPS=512 \
MULTIISR_STRESS_PEERS=12 \
MULTIISR_STRESS_SNAPSHOT_INTERVAL=24 \
MULTIISR_STRESS_BACKPRESSURE_INTERVAL=12 \
go test ./pkg/multiisr -run 'TestRuntimeStress(MixedScheduling|PeerQueueRecovery|SnapshotInterference)' -count=1 -v
```

## Environment Variables

The stress harness reads these variables:

- `MULTIISR_STRESS`
  Set to `1` or `true` to enable gated stress tests.
- `MULTIISR_STRESS_DURATION`
  Logical stress duration. Default: `10s`.
- `MULTIISR_STRESS_GROUPS`
  Number of groups to preload. Default: `256`.
- `MULTIISR_STRESS_PEERS`
  Number of remote peers to model. Default: `8`.
- `MULTIISR_STRESS_SEED`
  Deterministic peer-selection seed. Default: `1`.
- `MULTIISR_STRESS_SNAPSHOT_INTERVAL`
  Snapshot injection interval in driver rounds. Default: `32`.
- `MULTIISR_STRESS_BACKPRESSURE_INTERVAL`
  Backpressure toggle interval in driver rounds. Default: `16`.

## Reading Results

- `BenchmarkRuntimeReplicationScheduling`
  Measures the steady-state cost of enqueueing replication work and running the scheduler.
- `BenchmarkRuntimePeerQueueDrain`
  Measures the queue recovery path where a fetch response releases inflight state and drains the next queued request.
- `BenchmarkRuntimeMixedReplicationAndSnapshot`
  Measures replication scheduling while periodic snapshot work is injected.
- `TestRuntimeStressMixedScheduling`
  Should finish with every group making progress, peer queues empty, and snapshot waiting drained.
- `TestRuntimeStressPeerQueueRecovery`
  Should show queue buildup during hard backpressure and a fully drained queue after recovery.
- `TestRuntimeStressSnapshotInterference`
  Should observe a non-zero snapshot waiting high-watermark during the run and a zero waiting depth at the end.
