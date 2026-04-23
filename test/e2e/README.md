# E2E Tests

This directory contains process-level black-box end-to-end tests.

Rules:

- run these tests with `go test -tags=e2e ./test/e2e/... -count=1`
- keep the layer black-box and do not import `internal/app`
- assert behavior only through external protocols and child-process diagnostics
- inspect per-test temp directories, generated configs, and stdout/stderr logs when failures happen

The first phase focuses on one real `cmd/wukongim` child process and one real
`WKProto` send/recv/ack scenario in a `单节点集群`.
