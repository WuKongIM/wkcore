# E2E Tests

This directory contains process-level black-box end-to-end tests.

Rules:

- run these tests with `go test -tags=e2e ./test/e2e/... -count=1`
- keep the layer black-box and do not import `internal/app`
- assert behavior only through external protocols and child-process diagnostics
- do not use fixed sleeps for readiness; use `/readyz` plus real WKProto handshakes
- discover three-node slot roles through `/manager/slots/1`
- inspect per-test temp directories, generated configs, and stdout/stderr logs when failures happen
- inspect node-scoped `logs/app.log` and `logs/error.log` tails when three-node failures happen

Coverage:

- phase 1 baseline: one real `cmd/wukongim` child process and one real
  `WKProto` send/recv/ack scenario in a `单节点集群`
- phase 2 baseline: one real three-node cluster and one cross-node
  `Connect -> Send -> SendAck -> Recv -> RecvAck` WKProto closure
