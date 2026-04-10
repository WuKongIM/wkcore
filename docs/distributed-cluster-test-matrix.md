# Distributed Cluster Test Matrix

这份清单把当前仓库里和分布式节点相关的关键测试入口收拢到一起，重点覆盖：

- 选举与 leader 重启
- ISR/日志复制与恢复
- MinISR 与提交语义
- 节点滚动重启与写可用性
- 集群压力与转发竞争

## 1. 快速冒烟

适合本地改完复制、选举或多节点路径后先跑一轮。

```bash
go test ./pkg/channel/isr -run 'TestBecomeLeader(ReplaysIdenticalEpochPointIdempotently|ReusesRecoveredEpochPointWhenLeaderRestartsSameEpoch)' -count=1

go test ./pkg/channel/node -run 'TestQueuedReplicationRecomputesReplicaProgressBetweenSends|TestReplicationRequest(PopulatesFetchRequestEnvelope|UsesReplicaOffsetEpochInsteadOfGroupMetaEpoch)' -count=1

go test ./pkg/cluster -run 'Test(TestNodeRestartReopensClusterWithSameListenAddr|ThreeNodeClusterReelectsAfterLeaderRestart)' -count=1

go test ./pkg/channel/log -run 'Test(ThreeNodeClusterAppendCommitsBeforeAckAndSurvivesFollowerRestart|ThreeNodeClusterHarnessRestartNodeReopensData|ThreeNodeClusterBlocksCommitUntilMinISRRecovers|FollowerRestartCatchesUpAfterLeaderProgress)' -count=1 -timeout 30s

go test ./internal/app -run 'TestThreeNodeApp(SendAckSurvivesLeaderCrash|RollingRestartPreservesWriteAvailability)' -count=1 -timeout 10m
```

## 2. 场景矩阵

### 选举与重启

- `pkg/cluster.TestTestNodeRestartReopensClusterWithSameListenAddr`
  验证节点重启后仍复用原监听地址和数据目录。
- `pkg/cluster.TestThreeNodeClusterReelectsAfterLeaderRestart`
  验证 leader 停机后可重新选主，旧 leader 重启后仍能继续对外写入。

命令：

```bash
go test ./pkg/cluster -run 'Test(TestNodeRestartReopensClusterWithSameListenAddr|ThreeNodeClusterReelectsAfterLeaderRestart)' -count=1
```

### ISR 恢复与复制进度

- `pkg/channel/isr.TestBecomeLeaderReusesRecoveredEpochPointWhenLeaderRestartsSameEpoch`
  验证 leader 同 epoch 重启不会重复写 epoch point。
- `pkg/channel/node.TestQueuedReplicationRecomputesReplicaProgressBetweenSends`
  验证排队 fetch 请求会按最新 follower 进度发送，不复用 stale offset。

命令：

```bash
go test ./pkg/channel/isr -run 'TestBecomeLeader(ReplaysIdenticalEpochPointIdempotently|ReusesRecoveredEpochPointWhenLeaderRestartsSameEpoch)' -count=1

go test ./pkg/channel/node -run 'TestQueuedReplicationRecomputesReplicaProgressBetweenSends|TestReplicationRequest(PopulatesFetchRequestEnvelope|UsesReplicaOffsetEpochInsteadOfGroupMetaEpoch)' -count=1
```

### ChannelLog 容灾与 MinISR

- `pkg/channel/log.TestThreeNodeClusterAppendCommitsBeforeAckAndSurvivesFollowerRestart`
  验证 follower 重启后仍能恢复已提交消息。
- `pkg/channel/log.TestThreeNodeClusterHarnessRestartNodeReopensData`
  验证测试 harness 的节点重启路径真实复用落盘目录。
- `pkg/channel/log.TestThreeNodeClusterBlocksCommitUntilMinISRRecovers`
  验证 `MinISR` 不满足时提交会阻塞，节点恢复后未提交记录可继续达成提交。
- `pkg/channel/log.TestFollowerRestartCatchesUpAfterLeaderProgress`
  验证 follower 离线期间 leader 持续推进，重启后 follower 能追平 `HW/LEO`。

命令：

```bash
go test ./pkg/channel/log -run 'Test(ThreeNodeClusterAppendCommitsBeforeAckAndSurvivesFollowerRestart|ThreeNodeClusterHarnessRestartNodeReopensData|ThreeNodeClusterBlocksCommitUntilMinISRRecovers|FollowerRestartCatchesUpAfterLeaderProgress)' -count=1 -timeout 30s
```

说明：

- 这里的 `context deadline exceeded` 只表示本次客户端提交等待超时，不代表 leader 本地已追加数据被回滚。
- 当前实现语义下，未满足 `MinISR` 的记录在 ISR 恢复后仍可能继续完成提交，测试应按这个语义断言。

### App 端到端多节点写入

- `internal/app.TestThreeNodeAppSendAckSurvivesLeaderCrash`
  验证发包拿到 sendack 后 leader 崩溃，已确认消息仍会在新 leader 和重启节点上可见。
- `internal/app.TestThreeNodeAppRollingRestartPreservesWriteAvailability`
  验证三节点滚动重启过程中 owner channel 写入持续可用。

命令：

```bash
go test ./internal/app -run 'TestThreeNodeApp(SendAckSurvivesLeaderCrash|RollingRestartPreservesWriteAvailability)' -count=1 -timeout 10m
```

## 3. 压力与稳定性

轻量压力回归：

```bash
WKCLUSTER_STRESS=1 \
WKCLUSTER_STRESS_DURATION=2s \
WKCLUSTER_STRESS_WORKERS=2 \
go test ./pkg/cluster -run 'TestStress(ThreeNodeMixedWorkloadWithRestarts|ForwardingContentionWithLeaderRestarts)' -count=1 -timeout 10m
```

覆盖点：

- `TestStressThreeNodeMixedWorkloadWithRestarts`
  三节点混合读写，加周期性 leader 重启，校验写后可见性。
- `TestStressForwardingContentionWithLeaderRestarts`
  只从 follower 发起写入，叠加 leader 重启，校验 forwarding 路径与最终探针写入。

## 4. 全量回归

在改动跨多个层次时，最终以全量回归收口：

```bash
go test ./...
```

## 5. 推荐执行顺序

1. 先跑“快速冒烟”，确认选举、复制、重启路径没有立即回归。
2. 再跑 `pkg/channel/log` 和 `internal/app`，确认多节点持久化和端到端行为。
3. 最后跑轻量 stress 和 `go test ./...`。

## 6. 适用改动

下列改动建议至少跑完“快速冒烟”：

- `pkg/cluster`
- `pkg/channel/isr`
- `pkg/channel/node`
- `pkg/channel/log`
- `internal/app` 多节点集成路径

下列改动建议直接补跑压力回归：

- leader/follower 重启逻辑
- `MinISR`、`HW`、epoch、fetch offset 相关逻辑
- forwarding、proposal、queue drain、backpressure 相关逻辑
