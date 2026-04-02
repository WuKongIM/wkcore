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

Append baseline:

- `BenchmarkReplicaAppend/batch=1/payload=128`: `2148 ns/op`, `1909 B/op`, `23 allocs/op`
- `BenchmarkReplicaAppend/batch=16/payload=128`: `5528 ns/op`, `13463 B/op`, `109 allocs/op`
- `BenchmarkReplicaAppend/batch=16/payload=1024`: `15018 ns/op`, `85158 B/op`, `109 allocs/op`

Round-trip baseline:

- `BenchmarkThreeReplicaReplicationRoundTrip/batch=1/payload=128`: `2221 ns/op`, `1907 B/op`, `23 allocs/op`

Supporting baselines:

- `BenchmarkReplicaFetch/max_bytes=4096/backlog=32`: `1167 ns/op`, `6264 B/op`, `39 allocs/op`
- `BenchmarkReplicaFetch/max_bytes=65536/backlog=256`: `7998 ns/op`, `51832 B/op`, `266 allocs/op`
- `BenchmarkReplicaApplyFetch/mode=append_only`: `129.9 ns/op`, `347 B/op`, `2 allocs/op`
- `BenchmarkReplicaApplyFetch/mode=truncate_append`: `851.1 ns/op`, `417 B/op`, `10 allocs/op`
- `BenchmarkNewReplicaRecovery/state=empty`: `92.12 ns/op`, `368 B/op`, `2 allocs/op`
- `BenchmarkNewReplicaRecovery/state=clean_checkpoint`: `100.3 ns/op`, `400 B/op`, `4 allocs/op`
- `BenchmarkNewReplicaRecovery/state=dirty_tail`: `947.9 ns/op`, `2793 B/op`, `9 allocs/op`

## CPU Hotspots

Pending profile capture.

## Memory Hotspots

Pending profile capture.

## Recommendation

Pending analysis.

## Conclusion

Pending analysis.
