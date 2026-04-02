# ISR Pressure Analysis

## Environment

- `go version`: go1.23.4 darwin/arm64
- `GOOS`: darwin
- `GOARCH`: arm64
- `GOPATH`: /Users/tt/go
- `GOMOD`: /Users/tt/Desktop/work/go/WuKongIM-v3.1/.worktrees/isr-library-implementation/go.mod
- `uname -a`: Darwin ttdeMac-mini.local 24.1.0 Darwin Kernel Version 24.1.0: Thu Oct 10 21:06:23 PDT 2024; root:xnu-11215.41.3~3/RELEASE_ARM64_T8132 arm64
- `CPU`: Apple M4

## Commands

### Baseline

```bash
go test ./pkg/isr -run '^$' -bench '^BenchmarkReplicaAppend$' -benchmem -benchtime=3s | tee tmp/isr-pressure/2026-04-02/append-baseline.txt
go test ./pkg/isr -run '^$' -bench '^BenchmarkThreeReplicaReplicationRoundTrip$' -benchmem -benchtime=3s | tee tmp/isr-pressure/2026-04-02/roundtrip-baseline.txt
go test ./pkg/isr -run '^$' -bench '^(BenchmarkReplicaFetch|BenchmarkReplicaApplyFetch|BenchmarkNewReplicaRecovery)$' -benchmem -benchtime=3s | tee tmp/isr-pressure/2026-04-02/aux-baseline.txt
```

### Profiles

```bash
go test ./pkg/isr -run '^$' -bench '^BenchmarkReplicaAppend$' -benchtime=3s \
  -cpuprofile tmp/isr-pressure/2026-04-02/append.cpu.pprof \
  -memprofile tmp/isr-pressure/2026-04-02/append.mem.pprof
go tool pprof -top tmp/isr-pressure/2026-04-02/append.cpu.pprof > tmp/isr-pressure/2026-04-02/append.cpu.top.txt
go tool pprof -top tmp/isr-pressure/2026-04-02/append.mem.pprof > tmp/isr-pressure/2026-04-02/append.mem.top.txt
go test ./pkg/isr -run '^$' -bench '^BenchmarkThreeReplicaReplicationRoundTrip$' -benchtime=3s \
  -cpuprofile tmp/isr-pressure/2026-04-02/roundtrip.cpu.pprof \
  -memprofile tmp/isr-pressure/2026-04-02/roundtrip.mem.pprof
go tool pprof -top tmp/isr-pressure/2026-04-02/roundtrip.cpu.pprof > tmp/isr-pressure/2026-04-02/roundtrip.cpu.top.txt
go tool pprof -top tmp/isr-pressure/2026-04-02/roundtrip.mem.pprof > tmp/isr-pressure/2026-04-02/roundtrip.mem.top.txt
```

## Baseline Results

Pending benchmark execution.

## CPU Hotspots

Pending profile capture.

## Memory Hotspots

Pending profile capture.

## Recommendation

Pending analysis.

## Conclusion

Pending analysis.
