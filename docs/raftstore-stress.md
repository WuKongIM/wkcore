# Raftstore Stress Guide

## Benchmark Commands

日常 benchmark：

```bash
go test ./raftstore -run '^$' -bench . -count=1
```

放大 benchmark 规模：

```bash
WRAFT_RAFTSTORE_BENCH_SCALE=heavy go test ./raftstore -run '^$' -bench . -count=1
```

建议先看这些基准：

- `BenchmarkPebbleSaveEntries`
- `BenchmarkPebbleEntries`
- `BenchmarkPebbleMarkApplied`
- `BenchmarkPebbleInitialStateAndReopen`

## Stress Commands

显式开启全部 stress：

```bash
WRAFT_RAFTSTORE_STRESS=1 \
WRAFT_RAFTSTORE_STRESS_DURATION=5m \
go test ./raftstore -run '^TestPebbleStress' -count=1 -v
```

快速验证单个 stress 用例：

```bash
WRAFT_RAFTSTORE_STRESS=1 \
WRAFT_RAFTSTORE_STRESS_DURATION=3s \
go test ./raftstore -run '^TestPebbleStressConcurrentWriters$' -count=1 -v
```

```bash
WRAFT_RAFTSTORE_STRESS=1 \
WRAFT_RAFTSTORE_STRESS_DURATION=3s \
go test ./raftstore -run '^TestPebbleStressMixedReadWriteReopen$' -count=1 -v
```

```bash
WRAFT_RAFTSTORE_STRESS=1 \
WRAFT_RAFTSTORE_STRESS_DURATION=3s \
go test ./raftstore -run '^TestPebbleStressSnapshotAndRecovery$' -count=1 -v
```

## Environment Variables

- `WRAFT_RAFTSTORE_BENCH_SCALE`
  - `default`: 中小规模 benchmark
  - `heavy`: 放大 group 数、entry 数和 payload
- `WRAFT_RAFTSTORE_STRESS`
  - 设为 `1` 才会执行长压测试
- `WRAFT_RAFTSTORE_STRESS_DURATION`
  - stress 运行时长，默认 `5m`
- `WRAFT_RAFTSTORE_STRESS_GROUPS`
  - stress group 数，默认 `64`
- `WRAFT_RAFTSTORE_STRESS_WRITERS`
  - stress writer goroutine 数，默认 `8`
- `WRAFT_RAFTSTORE_STRESS_PAYLOAD`
  - entry payload 大小，默认 `256`

## Expected Runtime And Result Interpretation

- 默认 `go test ./...` 不会跑这些长压测试；只有显式设置 `WRAFT_RAFTSTORE_STRESS=1` 才会执行。
- benchmark 输出主要看 `ns/op`、`B/op`、`allocs/op`，用于横向比较变更前后趋势，不建议在仓库里写死机器相关阈值。
- stress 关注的是正确性而不是绝对速度。失败通常意味着：
  - reopen 后 `AppliedIndex`、`FirstIndex`、`LastIndex` 不一致
  - `Entries()` 返回非连续日志
  - snapshot 后 `Term(snapshotIndex)` 或 `ConfState` 恢复错误
- `TestPebbleStressMixedReadWriteReopen` 会在每轮负载结束后 clean close/reopen，再做一致性校验。
- `TestPebbleStressSnapshotAndRecovery` 使用周期性 snapshot 加少量 post-snapshot entry，避免长压时日志无限增长。
