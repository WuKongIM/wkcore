package app

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

const (
	sendStressEnv                  = "WK_SEND_STRESS"
	sendStressDurationEnv          = "WK_SEND_STRESS_DURATION"
	sendStressWorkersEnv           = "WK_SEND_STRESS_WORKERS"
	sendStressSendersEnv           = "WK_SEND_STRESS_SENDERS"
	sendStressMessagesPerWorkerEnv = "WK_SEND_STRESS_MESSAGES_PER_WORKER"
	sendStressDialTimeoutEnv       = "WK_SEND_STRESS_DIAL_TIMEOUT"
	sendStressAckTimeoutEnv        = "WK_SEND_STRESS_ACK_TIMEOUT"
	sendStressSeedEnv              = "WK_SEND_STRESS_SEED"
	sendStressWarmupAckTimeout     = 12 * time.Second
)

type sendStressConfig struct {
	Enabled           bool
	Duration          time.Duration
	Workers           int
	Senders           int
	MessagesPerWorker int
	DialTimeout       time.Duration
	AckTimeout        time.Duration
	Seed              int64
}

type sendStressLatencySummary struct {
	Count int
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

type sendStressTarget struct {
	SenderUID     string
	RecipientUID  string
	ChannelID     string
	ChannelType   uint8
	OwnerNodeID   uint64
	ConnectNodeID uint64
}

type sendStressRecord struct {
	Worker          int
	Iteration       int
	SenderUID       string
	RecipientUID    string
	ChannelID       string
	ChannelType     uint8
	ClientSeq       uint64
	ClientMsgNo     string
	Payload         []byte
	MessageID       int64
	MessageSeq      uint64
	AckLatency      time.Duration
	OwnerNodeID     uint64
	ConnectNodeID   uint64
	FramesBeforeAck []string
}

type sendStressOutcome struct {
	Total   uint64
	Success uint64
	Failed  uint64
}

type sendStressWorkerClient struct {
	target sendStressTarget
	conn   net.Conn
}

func (o sendStressOutcome) ErrorRate() float64 {
	if o.Total == 0 {
		return 0
	}
	return float64(o.Failed) * 100 / float64(o.Total)
}

func loadSendStressConfig(t *testing.T) sendStressConfig {
	t.Helper()

	enabled, ok, err := parseSendStressEnabled(os.Getenv(sendStressEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", sendStressEnv, err)
	}
	if !ok {
		enabled = false
	}

	cfg := sendStressConfig{
		Enabled:           enabled,
		Duration:          envDuration(t, sendStressDurationEnv, 5*time.Second),
		Workers:           envInt(t, sendStressWorkersEnv, max(4, runtime.GOMAXPROCS(0))),
		MessagesPerWorker: envInt(t, sendStressMessagesPerWorkerEnv, 50),
		DialTimeout:       envDuration(t, sendStressDialTimeoutEnv, 3*time.Second),
		AckTimeout:        envDuration(t, sendStressAckTimeoutEnv, 5*time.Second),
		Seed:              envInt64(t, sendStressSeedEnv, 20260408),
	}

	if cfg.Workers <= 0 {
		t.Fatalf("%s must be > 0, got %d", sendStressWorkersEnv, cfg.Workers)
	}
	if cfg.MessagesPerWorker <= 0 {
		t.Fatalf("%s must be > 0, got %d", sendStressMessagesPerWorkerEnv, cfg.MessagesPerWorker)
	}
	if cfg.Duration <= 0 {
		t.Fatalf("%s must be > 0, got %s", sendStressDurationEnv, cfg.Duration)
	}
	if cfg.DialTimeout <= 0 {
		t.Fatalf("%s must be > 0, got %s", sendStressDialTimeoutEnv, cfg.DialTimeout)
	}
	if cfg.AckTimeout <= 0 {
		t.Fatalf("%s must be > 0, got %s", sendStressAckTimeoutEnv, cfg.AckTimeout)
	}
	if value, ok := os.LookupEnv(sendStressSendersEnv); !ok || strings.TrimSpace(value) == "" {
		cfg.Senders = max(8, cfg.Workers)
	} else {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("parse %s: %v", sendStressSendersEnv, err)
		}
		cfg.Senders = parsed
	}
	if err := validateSendStressConfig(cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validateSendStressConfig(cfg sendStressConfig) error {
	if cfg.Workers <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", sendStressWorkersEnv, cfg.Workers)
	}
	if cfg.Senders <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", sendStressSendersEnv, cfg.Senders)
	}
	if cfg.Senders < cfg.Workers {
		return fmt.Errorf("%s must be >= %s, got %d < %d", sendStressSendersEnv, sendStressWorkersEnv, cfg.Senders, cfg.Workers)
	}
	if cfg.MessagesPerWorker <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", sendStressMessagesPerWorkerEnv, cfg.MessagesPerWorker)
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf("%s must be > 0, got %s", sendStressDurationEnv, cfg.Duration)
	}
	if cfg.DialTimeout <= 0 {
		return fmt.Errorf("%s must be > 0, got %s", sendStressDialTimeoutEnv, cfg.DialTimeout)
	}
	if cfg.AckTimeout <= 0 {
		return fmt.Errorf("%s must be > 0, got %s", sendStressAckTimeoutEnv, cfg.AckTimeout)
	}
	return nil
}

func parseSendStressEnabled(value string) (bool, bool, error) {
	if strings.TrimSpace(value) == "" {
		return false, false, nil
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, true, nil
	case "0", "false", "no", "off":
		return false, true, nil
	default:
		return false, true, strconv.ErrSyntax
	}
}

func requireSendStressEnabled(t *testing.T, cfg sendStressConfig) {
	t.Helper()
	if !cfg.Enabled {
		t.Skip("set WK_SEND_STRESS=1 to enable send stress test")
	}
}

func summarizeSendStressLatencies(latencies []time.Duration) sendStressLatencySummary {
	if len(latencies) == 0 {
		return sendStressLatencySummary{}
	}

	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sendStressLatencySummary{
		Count: len(sorted),
		P50:   percentileSendStressDuration(sorted, 0.50),
		P95:   percentileSendStressDuration(sorted, 0.95),
		P99:   percentileSendStressDuration(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
	}
}

func percentileSendStressDuration(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted))*pct)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func TestSendStressConfigDefaultsAndOverrides(t *testing.T) {
	if os.Getenv("SEND_STRESS_FORCE_INVALID_LOAD") == "1" {
		_ = loadSendStressConfig(t)
		return
	}

	clearSendStressConfigEnv(t)
	defaultCfg := loadSendStressConfig(t)
	defaultWorkers := max(4, runtime.GOMAXPROCS(0))
	require.False(t, defaultCfg.Enabled)
	require.Equal(t, 5*time.Second, defaultCfg.Duration)
	require.Equal(t, defaultWorkers, defaultCfg.Workers)
	require.Equal(t, max(8, defaultWorkers), defaultCfg.Senders)
	require.Equal(t, 50, defaultCfg.MessagesPerWorker)
	require.Equal(t, 3*time.Second, defaultCfg.DialTimeout)
	require.Equal(t, 5*time.Second, defaultCfg.AckTimeout)

	t.Setenv("WK_SEND_STRESS", "1")
	t.Setenv("WK_SEND_STRESS_DURATION", "1500ms")
	t.Setenv("WK_SEND_STRESS_WORKERS", "7")
	t.Setenv("WK_SEND_STRESS_SENDERS", "11")
	t.Setenv("WK_SEND_STRESS_MESSAGES_PER_WORKER", "13")
	t.Setenv("WK_SEND_STRESS_DIAL_TIMEOUT", "2s")
	t.Setenv("WK_SEND_STRESS_ACK_TIMEOUT", "1800ms")
	t.Setenv("WK_SEND_STRESS_SEED", "42")

	cfg := loadSendStressConfig(t)
	require.True(t, cfg.Enabled)
	require.Equal(t, 1500*time.Millisecond, cfg.Duration)
	require.Equal(t, 7, cfg.Workers)
	require.Equal(t, 11, cfg.Senders)
	require.Equal(t, 13, cfg.MessagesPerWorker)
	require.Equal(t, 2*time.Second, cfg.DialTimeout)
	require.Equal(t, 1800*time.Millisecond, cfg.AckTimeout)
	require.EqualValues(t, 42, cfg.Seed)

	enabled, ok, err := parseSendStressEnabled("")
	require.NoError(t, err)
	require.False(t, enabled)
	require.False(t, ok)

	enabled, ok, err = parseSendStressEnabled("maybe")
	require.Error(t, err)
	require.True(t, ok)
	require.False(t, enabled)

	err = validateSendStressConfig(sendStressConfig{
		Workers:           0,
		Senders:           1,
		MessagesPerWorker: 1,
		Duration:          time.Second,
		DialTimeout:       time.Second,
		AckTimeout:        time.Second,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), sendStressWorkersEnv)

	err = validateSendStressConfig(sendStressConfig{
		Workers:           2,
		Senders:           1,
		MessagesPerWorker: 1,
		Duration:          time.Second,
		DialTimeout:       time.Second,
		AckTimeout:        time.Second,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), sendStressSendersEnv)

	err = validateSendStressConfig(sendStressConfig{
		Workers:           2,
		Senders:           2,
		MessagesPerWorker: 0,
		Duration:          time.Second,
		DialTimeout:       time.Second,
		AckTimeout:        time.Second,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), sendStressMessagesPerWorkerEnv)

	assertLoadSendStressConfigFailsOnInvalidEnv(t)
}

func TestSendStressLatencySummaryPercentiles(t *testing.T) {
	summary := summarizeSendStressLatencies([]time.Duration{
		90 * time.Millisecond,
		10 * time.Millisecond,
		70 * time.Millisecond,
		30 * time.Millisecond,
		50 * time.Millisecond,
	})

	require.Equal(t, 5, summary.Count)
	require.Equal(t, 50*time.Millisecond, summary.P50)
	require.Equal(t, 90*time.Millisecond, summary.P95)
	require.Equal(t, 90*time.Millisecond, summary.P99)
	require.Equal(t, 90*time.Millisecond, summary.Max)
}

func TestSendStressOutcomeErrorRate(t *testing.T) {
	outcome := sendStressOutcome{Total: 10, Success: 8, Failed: 2}
	require.InDelta(t, 20.0, outcome.ErrorRate(), 0.001)
}

func TestSendStressThreeNode(t *testing.T) {
	cfg := loadSendStressConfig(t)
	requireSendStressEnabled(t, cfg)

	harness := newThreeNodeAppHarness(t)
	leaderID := harness.waitForStableLeader(t, 1)
	leader := harness.apps[leaderID]

	targets := preloadSendStressChannels(t, harness, leader, cfg)
	outcome, records, failures := runSendStressWorkers(t, harness, targets, cfg)
	verifySendStressCommittedRecords(t, harness, records)

	t.Logf("send stress results: total=%d success=%d failed=%d error_rate=%.2f%%", outcome.Total, outcome.Success, outcome.Failed, outcome.ErrorRate())
	if len(failures) > 0 {
		t.Logf("send stress failures: %s", strings.Join(failures, " | "))
	}

	require.NotZero(t, outcome.Total)
	require.Equal(t, outcome.Total, outcome.Success)
	require.Zero(t, outcome.Failed)
	require.Len(t, records, int(outcome.Success))
}

func clearSendStressConfigEnv(t *testing.T) {
	t.Helper()

	for _, name := range []string{
		sendStressEnv,
		sendStressDurationEnv,
		sendStressWorkersEnv,
		sendStressSendersEnv,
		sendStressMessagesPerWorkerEnv,
		sendStressDialTimeoutEnv,
		sendStressAckTimeoutEnv,
		sendStressSeedEnv,
	} {
		name := name
		if value, ok := os.LookupEnv(name); ok {
			if err := os.Unsetenv(name); err != nil {
				t.Fatalf("clear %s: %v", name, err)
			}
			t.Cleanup(func() {
				if err := os.Setenv(name, value); err != nil {
					t.Fatalf("restore %s: %v", name, err)
				}
			})
		}
	}
}

func assertLoadSendStressConfigFailsOnInvalidEnv(t *testing.T) {
	t.Helper()

	cmd := exec.Command("go", "test", "./internal/app", "-run", "^TestSendStressConfigDefaultsAndOverrides$", "-count=1")
	cmd.Dir = filepath.Clean(filepath.Join("..", ".."))
	cmd.Env = filterSendStressEnv(os.Environ())
	cmd.Env = append(cmd.Env,
		"SEND_STRESS_FORCE_INVALID_LOAD=1",
		"WK_SEND_STRESS=1",
		"WK_SEND_STRESS_WORKERS=0",
		"WK_SEND_STRESS_SENDERS=1",
	)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, output.String(), sendStressWorkersEnv)
}

func filterSendStressEnv(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, sendStressEnv+"=") ||
			strings.HasPrefix(entry, sendStressDurationEnv+"=") ||
			strings.HasPrefix(entry, sendStressWorkersEnv+"=") ||
			strings.HasPrefix(entry, sendStressSendersEnv+"=") ||
			strings.HasPrefix(entry, sendStressMessagesPerWorkerEnv+"=") ||
			strings.HasPrefix(entry, sendStressDialTimeoutEnv+"=") ||
			strings.HasPrefix(entry, sendStressAckTimeoutEnv+"=") ||
			strings.HasPrefix(entry, sendStressSeedEnv+"=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func preloadSendStressChannels(t *testing.T, harness *threeNodeAppHarness, leader *App, cfg sendStressConfig) []sendStressTarget {
	t.Helper()
	require.NotNil(t, harness)
	require.NotNil(t, leader)
	require.NotZero(t, cfg.Senders)

	leaderID := leader.cfg.Node.ID
	targets := make([]sendStressTarget, 0, cfg.Senders)
	for idx := 0; idx < cfg.Senders; idx++ {
		senderUID := fmt.Sprintf("stress-sender-%03d", idx)
		recipientUID := fmt.Sprintf("stress-recipient-%03d", idx)
		channelID := deliveryusecase.EncodePersonChannel(senderUID, recipientUID)
		channelType := wkframe.ChannelTypePerson
		channelEpoch := uint64(1000 + idx)

		meta := metadb.ChannelRuntimeMeta{
			ChannelID:    channelID,
			ChannelType:  int64(channelType),
			ChannelEpoch: channelEpoch,
			LeaderEpoch:  channelEpoch,
			Replicas:     []uint64{1, 2, 3},
			ISR:          []uint64{1, 2, 3},
			Leader:       leaderID,
			MinISR:       3,
			Status:       uint8(channellog.ChannelStatusActive),
			Features:     uint64(channellog.MessageSeqFormatLegacyU32),
			LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		}
		require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

		key := channellog.ChannelKey{ChannelID: channelID, ChannelType: channelType}
		for _, app := range harness.appsWithLeaderFirst(leaderID) {
			_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), key)
			require.NoError(t, err)
		}

		targets = append(targets, sendStressTarget{
			SenderUID:     senderUID,
			RecipientUID:  recipientUID,
			ChannelID:     channelID,
			ChannelType:   channelType,
			OwnerNodeID:   leaderID,
			ConnectNodeID: leaderID,
		})
	}
	return targets
}

func runSendStressWorkers(t *testing.T, harness *threeNodeAppHarness, targets []sendStressTarget, cfg sendStressConfig) (sendStressOutcome, []sendStressRecord, []string) {
	t.Helper()
	require.NotNil(t, harness)
	require.NotEmpty(t, targets)
	require.NotZero(t, cfg.Workers)

	require.GreaterOrEqual(t, len(targets), cfg.Workers)

	clients := make([]sendStressWorkerClient, 0, cfg.Workers)
	for worker := 0; worker < cfg.Workers; worker++ {
		target := targets[worker]
		app := harness.apps[target.ConnectNodeID]
		require.NotNil(t, app, "connect node %d is not running", target.ConnectNodeID)
		clients = append(clients, sendStressWorkerClient{
			target: target,
			conn:   runSendStressClient(t, app, target.SenderUID, cfg),
		})
	}
	defer func() {
		for _, client := range clients {
			if client.conn != nil {
				_ = client.conn.Close()
			}
		}
	}()

	warmupRecords := warmupSendStressClients(t, clients, cfg)
	verifySendStressCommittedRecords(t, harness, warmupRecords)

	var total atomic.Uint64
	var success atomic.Uint64
	var failed atomic.Uint64
	var mu sync.Mutex
	records := make([]sendStressRecord, 0, cfg.Workers*cfg.MessagesPerWorker)
	latencies := make([]time.Duration, 0, cfg.Workers*cfg.MessagesPerWorker)
	failures := make([]string, 0, 8)
	appendFailure := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		if len(failures) >= 8 {
			return
		}
		failures = append(failures, fmt.Sprintf(format, args...))
	}

	startedAt := time.Now()
	deadline := startedAt.Add(cfg.Duration)
	var wg sync.WaitGroup
	for worker, client := range clients {
		worker := worker
		client := client
		wg.Add(1)
		go func() {
			defer wg.Done()

			for iteration := 0; iteration < cfg.MessagesPerWorker; iteration++ {
				if time.Now().After(deadline) {
					return
				}

				total.Add(1)

				clientSeq := uint64(iteration + 2)
				clientMsgNo := fmt.Sprintf("send-stress-%d-%s-%02d-%d", worker, client.target.SenderUID, iteration, cfg.Seed)
				payload := []byte(fmt.Sprintf("send-stress payload worker=%d sender=%s recipient=%s iteration=%d seed=%d", worker, client.target.SenderUID, client.target.RecipientUID, iteration, cfg.Seed))
				record, failure, ok := executeSendStressAttempt(client, worker, "measure", iteration, clientSeq, clientMsgNo, payload, cfg.AckTimeout)
				if !ok {
					failed.Add(1)
					appendFailure("%s", failure)
					return
				}
				success.Add(1)

				mu.Lock()
				records = append(records, record)
				latencies = append(latencies, record.AckLatency)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(startedAt)
	outcome := sendStressOutcome{
		Total:   total.Load(),
		Success: success.Load(),
		Failed:  failed.Load(),
	}
	latencySummary := summarizeSendStressLatencies(latencies)
	qps := 0.0
	if elapsed > 0 {
		qps = float64(outcome.Success) / elapsed.Seconds()
	}
	t.Logf(
		"send stress metrics: duration=%s workers=%d senders=%d total=%d success=%d failed=%d qps=%.2f p50=%s p95=%s p99=%s max=%s verification_count=%d verification_failures=%d",
		elapsed,
		cfg.Workers,
		cfg.Senders,
		outcome.Total,
		outcome.Success,
		outcome.Failed,
		qps,
		latencySummary.P50,
		latencySummary.P95,
		latencySummary.P99,
		latencySummary.Max,
		len(records),
		0,
	)
	if len(failures) > 0 {
		t.Logf("send stress failure samples: %s", strings.Join(failures, " | "))
	}

	return outcome, records, failures
}

func warmupSendStressClients(t *testing.T, clients []sendStressWorkerClient, cfg sendStressConfig) []sendStressRecord {
	t.Helper()
	require.NotEmpty(t, clients)

	ackTimeout := cfg.AckTimeout
	if ackTimeout < sendStressWarmupAckTimeout {
		ackTimeout = sendStressWarmupAckTimeout
	}

	startedAt := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	records := make([]sendStressRecord, 0, len(clients))
	failures := make([]string, 0, len(clients))
	for worker, client := range clients {
		worker := worker
		client := client
		wg.Add(1)
		go func() {
			defer wg.Done()

			clientMsgNo := fmt.Sprintf("send-stress-warmup-%d-%s-%d", worker, client.target.SenderUID, cfg.Seed)
			payload := []byte(fmt.Sprintf("send-stress warmup worker=%d sender=%s recipient=%s seed=%d", worker, client.target.SenderUID, client.target.RecipientUID, cfg.Seed))
			record, failure, ok := executeSendStressAttempt(client, worker, "warmup", -1, 1, clientMsgNo, payload, ackTimeout)

			mu.Lock()
			defer mu.Unlock()
			if ok {
				records = append(records, record)
				return
			}
			failures = append(failures, failure)
		}()
	}
	wg.Wait()

	t.Logf("send stress warmup: workers=%d success=%d failed=%d timeout=%s duration=%s", len(clients), len(records), len(failures), ackTimeout, time.Since(startedAt))
	if len(failures) > 0 {
		t.Fatalf("send stress warmup failures: %s", strings.Join(failures, " | "))
	}
	return records
}

func executeSendStressAttempt(client sendStressWorkerClient, worker int, phase string, iteration int, clientSeq uint64, clientMsgNo string, payload []byte, ackTimeout time.Duration) (sendStressRecord, string, bool) {
	packet := &wkframe.SendPacket{
		ChannelID:   client.target.RecipientUID,
		ChannelType: client.target.ChannelType,
		ClientSeq:   clientSeq,
		ClientMsgNo: clientMsgNo,
		Payload:     payload,
	}

	sendStart := time.Now()
	if err := writeSendStressFrame(client.conn, packet, ackTimeout); err != nil {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d write error=%v", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, err), false
	}

	sendack, framesBeforeAck, err := waitForSendStressSendack(client.conn, ackTimeout)
	ackLatency := time.Since(sendStart)
	if err != nil {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d readack error=%v", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, err), false
	}
	if sendack.ClientSeq != clientSeq || sendack.ClientMsgNo != clientMsgNo {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d ack_mismatch client_seq=%d/%d client_msg_no=%s/%s", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, sendack.ClientSeq, clientSeq, sendack.ClientMsgNo, clientMsgNo), false
	}
	if sendack.ReasonCode != wkframe.ReasonSuccess || sendack.MessageID == 0 || sendack.MessageSeq == 0 {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d reason=%s message_id=%d message_seq=%d", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, sendack.ReasonCode, sendack.MessageID, sendack.MessageSeq), false
	}

	return sendStressRecord{
		Worker:          worker,
		Iteration:       iteration,
		SenderUID:       client.target.SenderUID,
		RecipientUID:    client.target.RecipientUID,
		ChannelID:       client.target.ChannelID,
		ChannelType:     client.target.ChannelType,
		ClientSeq:       clientSeq,
		ClientMsgNo:     clientMsgNo,
		Payload:         payload,
		MessageID:       sendack.MessageID,
		MessageSeq:      sendack.MessageSeq,
		AckLatency:      ackLatency,
		OwnerNodeID:     client.target.OwnerNodeID,
		ConnectNodeID:   client.target.ConnectNodeID,
		FramesBeforeAck: append([]string(nil), framesBeforeAck...),
	}, "", true
}

func verifySendStressCommittedRecords(t *testing.T, harness *threeNodeAppHarness, records []sendStressRecord) {
	t.Helper()
	require.NotNil(t, harness)

	verificationCount := 0
	for _, record := range records {
		key := channellog.ChannelKey{
			ChannelID:   record.ChannelID,
			ChannelType: record.ChannelType,
		}
		owner := harness.apps[record.OwnerNodeID]
		require.NotNil(t, owner, "owner node %d is not running", record.OwnerNodeID)

		ownerMsg := waitForAppCommittedMessage(t, owner.ChannelLogDB().ForChannel(key), record.MessageSeq, 5*time.Second)
		require.Equalf(t, record.Payload, ownerMsg.Payload, "owner mismatch worker=%d iteration=%d sender=%s recipient=%s connect_node=%d message_seq=%d message_id=%d client_seq=%d client_msg_no=%s frames_before_ack=%v owner_from=%s owner_client_msg_no=%s owner_payload=%q", record.Worker, record.Iteration, record.SenderUID, record.RecipientUID, record.ConnectNodeID, record.MessageSeq, record.MessageID, record.ClientSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.FromUID, ownerMsg.ClientMsgNo, string(ownerMsg.Payload))
		require.Equalf(t, record.SenderUID, ownerMsg.FromUID, "owner sender mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s frames_before_ack=%v owner_from=%s owner_payload=%q", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.FromUID, string(ownerMsg.Payload))
		require.Equalf(t, record.ClientMsgNo, ownerMsg.ClientMsgNo, "owner client_msg_no mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s frames_before_ack=%v owner_client_msg_no=%s owner_payload=%q", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.ClientMsgNo, string(ownerMsg.Payload))
		require.Equalf(t, record.ChannelID, ownerMsg.ChannelID, "owner channel mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)
		require.Equalf(t, record.ChannelType, ownerMsg.ChannelType, "owner channel type mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)

		for _, app := range harness.orderedApps() {
			msg := waitForAppCommittedMessage(t, app.ChannelLogDB().ForChannel(key), record.MessageSeq, 5*time.Second)
			require.Equalf(t, record.Payload, msg.Payload, "replica mismatch node=%d worker=%d iteration=%d sender=%s recipient=%s connect_node=%d message_seq=%d message_id=%d client_seq=%d client_msg_no=%s frames_before_ack=%v replica_from=%s replica_client_msg_no=%s replica_payload=%q", app.cfg.Node.ID, record.Worker, record.Iteration, record.SenderUID, record.RecipientUID, record.ConnectNodeID, record.MessageSeq, record.MessageID, record.ClientSeq, record.ClientMsgNo, record.FramesBeforeAck, msg.FromUID, msg.ClientMsgNo, string(msg.Payload))
			require.Equalf(t, record.SenderUID, msg.FromUID, "replica sender mismatch node=%d worker=%d iteration=%d message_seq=%d client_msg_no=%s", app.cfg.Node.ID, record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)
			require.Equalf(t, record.ClientMsgNo, msg.ClientMsgNo, "replica client_msg_no mismatch node=%d worker=%d iteration=%d message_seq=%d client_msg_no=%s replica_client_msg_no=%s", app.cfg.Node.ID, record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo, msg.ClientMsgNo)
			require.Equalf(t, record.ChannelID, msg.ChannelID, "replica channel mismatch node=%d worker=%d iteration=%d message_seq=%d client_msg_no=%s", app.cfg.Node.ID, record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)
			require.Equalf(t, record.ChannelType, msg.ChannelType, "replica channel type mismatch node=%d worker=%d iteration=%d message_seq=%d client_msg_no=%s", app.cfg.Node.ID, record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)
			require.Equalf(t, record.MessageSeq, msg.MessageSeq, "replica message_seq mismatch node=%d worker=%d iteration=%d client_msg_no=%s", app.cfg.Node.ID, record.Worker, record.Iteration, record.ClientMsgNo)
		}
		verificationCount++
	}

	t.Logf("send stress durable verification: verification_count=%d verification_failures=%d", verificationCount, 0)
}

func runSendStressClient(t *testing.T, app *App, senderUID string, cfg sendStressConfig) net.Conn {
	t.Helper()
	require.NotNil(t, app)
	require.NotEmpty(t, senderUID)
	require.NotZero(t, cfg.DialTimeout)

	conn, err := dialSendStressClient(app, senderUID, cfg)
	require.NoError(t, err)
	return conn
}

func dialSendStressClient(app *App, senderUID string, cfg sendStressConfig) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", app.Gateway().ListenerAddr("tcp-wkproto"), cfg.DialTimeout)
	if err != nil {
		return nil, err
	}

	if err := writeSendStressFrame(conn, &wkframe.ConnectPacket{
		Version:         wkframe.LatestVersion,
		UID:             senderUID,
		DeviceID:        senderUID + "-stress-device",
		DeviceFlag:      wkframe.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	}, cfg.DialTimeout); err != nil {
		_ = conn.Close()
		return nil, err
	}

	frame, err := readSendStressFrameWithin(conn, cfg.AckTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	connack, ok := frame.(*wkframe.ConnackPacket)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("expected *wkframe.ConnackPacket, got %T", frame)
	}
	if connack.ReasonCode != wkframe.ReasonSuccess {
		_ = conn.Close()
		return nil, fmt.Errorf("connect failed with reason %s", connack.ReasonCode)
	}
	return conn, nil
}

func writeSendStressFrame(conn net.Conn, frame wkframe.Frame, timeout time.Duration) error {
	payload, err := codec.New().EncodeFrame(frame, wkframe.LatestVersion)
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	defer func() {
		_ = conn.SetWriteDeadline(time.Time{})
	}()
	_, err = conn.Write(payload)
	return err
}

func readSendStressFrameWithin(conn net.Conn, timeout time.Duration) (wkframe.Frame, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()
	return codec.New().DecodePacketWithConn(conn, wkframe.LatestVersion)
}

func waitForSendStressSendack(conn net.Conn, timeout time.Duration) (*wkframe.SendackPacket, []string, error) {
	deadline := time.Now().Add(timeout)
	framesBeforeAck := make([]string, 0, 2)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, framesBeforeAck, fmt.Errorf("timed out waiting for sendack")
		}

		frame, err := readSendStressFrameWithin(conn, remaining)
		if err != nil {
			return nil, framesBeforeAck, err
		}

		switch pkt := frame.(type) {
		case *wkframe.SendackPacket:
			return pkt, framesBeforeAck, nil
		case *wkframe.RecvPacket:
			framesBeforeAck = append(framesBeforeAck, fmt.Sprintf("recv(message_id=%d,message_seq=%d,client_seq=%d,client_msg_no=%s,payload=%q)", pkt.MessageID, pkt.MessageSeq, pkt.ClientSeq, pkt.ClientMsgNo, string(pkt.Payload)))
			if err := writeSendStressFrame(conn, &wkframe.RecvackPacket{
				MessageID:  pkt.MessageID,
				MessageSeq: pkt.MessageSeq,
			}, remaining); err != nil {
				return nil, framesBeforeAck, err
			}
		default:
			return nil, framesBeforeAck, fmt.Errorf("unexpected frame while waiting for sendack: %T", frame)
		}
	}
}
