package app

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	codec "github.com/WuKongIM/WuKongIM/pkg/protocol/codec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/stretchr/testify/require"
)

const (
	sendStressEnv                  = "WK_SEND_STRESS"
	sendStressModeEnv              = "WK_SEND_STRESS_MODE"
	sendStressDurationEnv          = "WK_SEND_STRESS_DURATION"
	sendStressWorkersEnv           = "WK_SEND_STRESS_WORKERS"
	sendStressSendersEnv           = "WK_SEND_STRESS_SENDERS"
	sendStressMessagesPerWorkerEnv = "WK_SEND_STRESS_MESSAGES_PER_WORKER"
	sendStressMaxInflightEnv       = "WK_SEND_STRESS_MAX_INFLIGHT_PER_WORKER"
	sendStressDialTimeoutEnv       = "WK_SEND_STRESS_DIAL_TIMEOUT"
	sendStressAckTimeoutEnv        = "WK_SEND_STRESS_ACK_TIMEOUT"
	sendStressSeedEnv              = "WK_SEND_STRESS_SEED"
	sendStressWarmupAckTimeout     = 12 * time.Second
	sendStressThroughputInflight   = 32
)

type sendStressMode string

const (
	sendStressModeLatency    sendStressMode = "latency"
	sendStressModeThroughput sendStressMode = "throughput"
)

type sendStressConfig struct {
	Enabled              bool
	Mode                 sendStressMode
	MaxInflightPerWorker int
	Duration             time.Duration
	Workers              int
	Senders              int
	MessagesPerWorker    int
	DialTimeout          time.Duration
	AckTimeout           time.Duration
	Seed                 int64
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
	target  sendStressTarget
	conn    net.Conn
	reader  *sendStressFrameReader
	writeMu *sync.Mutex
}

type sendStressAttemptResult struct {
	record  sendStressRecord
	failure string
	ok      bool
}

type sendStressPendingAttempt struct {
	client      sendStressWorkerClient
	worker      int
	phase       string
	iteration   int
	clientSeq   uint64
	clientMsgNo string
	payload     []byte
	startedAt   time.Time
	ch          chan sendStressAttemptResult
	onComplete  func(sendStressAttemptResult)
}

type sendStressInflightTracker struct {
	mu      sync.Mutex
	pending map[uint64]*sendStressPendingAttempt
}

type sendStressFrameReader struct {
	conn    net.Conn
	codec   codec.Protocol
	buf     []byte
	scratch []byte
}

func newSendStressFrameReader(conn net.Conn) *sendStressFrameReader {
	return &sendStressFrameReader{
		conn:    conn,
		codec:   codec.New(),
		scratch: make([]byte, 4096),
	}
}

func (r *sendStressFrameReader) ReadWithin(timeout time.Duration) (frame.Frame, error) {
	if r == nil || r.conn == nil {
		return nil, fmt.Errorf("send stress frame reader: nil connection")
	}
	deadline := time.Now().Add(timeout)
	for {
		if len(r.buf) > 0 {
			f, size, err := r.codec.DecodeFrame(r.buf, frame.LatestVersion)
			if err != nil {
				return nil, err
			}
			if f != nil && size > 0 {
				copy(r.buf, r.buf[size:])
				r.buf = r.buf[:len(r.buf)-size]
				return f, nil
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, &net.OpError{Err: os.ErrDeadlineExceeded}
		}
		if err := r.conn.SetReadDeadline(time.Now().Add(remaining)); err != nil {
			return nil, err
		}
		n, err := r.conn.Read(r.scratch)
		_ = r.conn.SetReadDeadline(time.Time{})
		if n > 0 {
			r.buf = append(r.buf, r.scratch[:n]...)
		}
		if err != nil {
			if isSendStressTimeout(err) {
				continue
			}
			return nil, err
		}
	}
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
	mode, ok, err := parseSendStressMode(os.Getenv(sendStressModeEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", sendStressModeEnv, err)
	}
	if !ok {
		mode = sendStressModeLatency
	}

	cfg := sendStressConfig{
		Enabled:              enabled,
		Mode:                 mode,
		Duration:             envDuration(t, sendStressDurationEnv, 5*time.Second),
		Workers:              envInt(t, sendStressWorkersEnv, max(4, runtime.GOMAXPROCS(0))),
		MessagesPerWorker:    envInt(t, sendStressMessagesPerWorkerEnv, 50),
		DialTimeout:          envDuration(t, sendStressDialTimeoutEnv, 3*time.Second),
		AckTimeout:           envDuration(t, sendStressAckTimeoutEnv, 5*time.Second),
		Seed:                 envInt64(t, sendStressSeedEnv, 20260408),
		MaxInflightPerWorker: 1,
	}
	if cfg.Mode == sendStressModeThroughput {
		cfg.MaxInflightPerWorker = envInt(t, sendStressMaxInflightEnv, sendStressThroughputInflight)
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
	switch cfg.Mode {
	case "", sendStressModeLatency:
		cfg.Mode = sendStressModeLatency
	case sendStressModeThroughput:
	default:
		return fmt.Errorf("%s must be one of %q or %q, got %q", sendStressModeEnv, sendStressModeLatency, sendStressModeThroughput, cfg.Mode)
	}
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
	if cfg.Mode == sendStressModeThroughput && cfg.MaxInflightPerWorker <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", sendStressMaxInflightEnv, cfg.MaxInflightPerWorker)
	}
	if cfg.Mode != sendStressModeThroughput {
		cfg.MaxInflightPerWorker = 1
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

func parseSendStressMode(value string) (sendStressMode, bool, error) {
	if strings.TrimSpace(value) == "" {
		return "", false, nil
	}
	switch sendStressMode(strings.ToLower(strings.TrimSpace(value))) {
	case sendStressModeLatency:
		return sendStressModeLatency, true, nil
	case sendStressModeThroughput:
		return sendStressModeThroughput, true, nil
	default:
		return "", true, strconv.ErrSyntax
	}
}

func newSendStressInflightTracker() *sendStressInflightTracker {
	return &sendStressInflightTracker{
		pending: make(map[uint64]*sendStressPendingAttempt),
	}
}

func (t *sendStressInflightTracker) Start(client sendStressWorkerClient, worker int, phase string, iteration int, clientSeq uint64, clientMsgNo string, payload []byte) <-chan sendStressAttemptResult {
	return t.startAt(client, worker, phase, iteration, clientSeq, clientMsgNo, payload, time.Now(), nil)
}

func (t *sendStressInflightTracker) startAt(client sendStressWorkerClient, worker int, phase string, iteration int, clientSeq uint64, clientMsgNo string, payload []byte, startedAt time.Time, onComplete func(sendStressAttemptResult)) <-chan sendStressAttemptResult {
	ch := make(chan sendStressAttemptResult, 1)

	t.mu.Lock()
	t.pending[clientSeq] = &sendStressPendingAttempt{
		client:      client,
		worker:      worker,
		phase:       phase,
		iteration:   iteration,
		clientSeq:   clientSeq,
		clientMsgNo: clientMsgNo,
		payload:     payload,
		startedAt:   startedAt,
		ch:          ch,
		onComplete:  onComplete,
	}
	t.mu.Unlock()
	return ch
}

func (t *sendStressInflightTracker) Complete(sendack *frame.SendackPacket, framesBeforeAck []string) error {
	if sendack == nil {
		return fmt.Errorf("send stress inflight tracker: nil sendack")
	}

	t.mu.Lock()
	attempt, ok := t.pending[sendack.ClientSeq]
	if ok {
		delete(t.pending, sendack.ClientSeq)
	}
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("send stress inflight tracker: unexpected sendack client_seq=%d", sendack.ClientSeq)
	}
	if sendack.ClientMsgNo != attempt.clientMsgNo {
		t.finish(attempt, sendStressAttemptResult{
			failure: fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d ack_mismatch client_seq=%d client_msg_no=%s/%s", attempt.worker, attempt.client.target.SenderUID, attempt.client.target.ConnectNodeID, attempt.phase, attempt.iteration, sendack.ClientSeq, sendack.ClientMsgNo, attempt.clientMsgNo),
		})
		return nil
	}
	if sendack.ReasonCode != frame.ReasonSuccess || sendack.MessageID == 0 || sendack.MessageSeq == 0 {
		t.finish(attempt, sendStressAttemptResult{
			failure: fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d reason=%s message_id=%d message_seq=%d", attempt.worker, attempt.client.target.SenderUID, attempt.client.target.ConnectNodeID, attempt.phase, attempt.iteration, sendack.ReasonCode, sendack.MessageID, sendack.MessageSeq),
		})
		return nil
	}

	t.finish(attempt, sendStressAttemptResult{
		ok: true,
		record: sendStressRecord{
			Worker:          attempt.worker,
			Iteration:       attempt.iteration,
			SenderUID:       attempt.client.target.SenderUID,
			RecipientUID:    attempt.client.target.RecipientUID,
			ChannelID:       attempt.client.target.ChannelID,
			ChannelType:     attempt.client.target.ChannelType,
			ClientSeq:       attempt.clientSeq,
			ClientMsgNo:     attempt.clientMsgNo,
			Payload:         attempt.payload,
			MessageID:       sendack.MessageID,
			MessageSeq:      sendack.MessageSeq,
			AckLatency:      time.Since(attempt.startedAt),
			OwnerNodeID:     attempt.client.target.OwnerNodeID,
			ConnectNodeID:   attempt.client.target.ConnectNodeID,
			FramesBeforeAck: append([]string(nil), framesBeforeAck...),
		},
	})
	return nil
}

func (t *sendStressInflightTracker) Fail(clientSeq uint64, failure string) error {
	t.mu.Lock()
	attempt, ok := t.pending[clientSeq]
	if ok {
		delete(t.pending, clientSeq)
	}
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("send stress inflight tracker: missing client_seq=%d", clientSeq)
	}
	t.finish(attempt, sendStressAttemptResult{failure: failure})
	return nil
}

func (t *sendStressInflightTracker) FailAll(format string, args ...any) {
	t.mu.Lock()
	pending := make([]*sendStressPendingAttempt, 0, len(t.pending))
	for clientSeq, attempt := range t.pending {
		delete(t.pending, clientSeq)
		pending = append(pending, attempt)
	}
	t.mu.Unlock()

	failure := fmt.Sprintf(format, args...)
	for _, attempt := range pending {
		t.finish(attempt, sendStressAttemptResult{failure: failure})
	}
}

func (t *sendStressInflightTracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func (t *sendStressInflightTracker) finish(attempt *sendStressPendingAttempt, result sendStressAttemptResult) {
	attempt.ch <- result
	close(attempt.ch)
	if attempt.onComplete != nil {
		attempt.onComplete(result)
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

func sendStressActiveTargetCount(cfg sendStressConfig, totalTargets int) int {
	if totalTargets <= 0 {
		return 0
	}
	if cfg.Mode == sendStressModeThroughput {
		return totalTargets
	}
	if cfg.Workers <= 0 {
		return 0
	}
	return min(cfg.Workers, totalTargets)
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
	require.Equal(t, sendStressModeLatency, defaultCfg.Mode)
	require.Equal(t, 5*time.Second, defaultCfg.Duration)
	require.Equal(t, defaultWorkers, defaultCfg.Workers)
	require.Equal(t, max(8, defaultWorkers), defaultCfg.Senders)
	require.Equal(t, 50, defaultCfg.MessagesPerWorker)
	require.Equal(t, 1, defaultCfg.MaxInflightPerWorker)
	require.Equal(t, 3*time.Second, defaultCfg.DialTimeout)
	require.Equal(t, 5*time.Second, defaultCfg.AckTimeout)

	t.Setenv("WK_SEND_STRESS", "1")
	t.Setenv("WK_SEND_STRESS_MODE", "throughput")
	t.Setenv("WK_SEND_STRESS_DURATION", "1500ms")
	t.Setenv("WK_SEND_STRESS_WORKERS", "7")
	t.Setenv("WK_SEND_STRESS_SENDERS", "11")
	t.Setenv("WK_SEND_STRESS_MESSAGES_PER_WORKER", "13")
	t.Setenv("WK_SEND_STRESS_MAX_INFLIGHT_PER_WORKER", "9")
	t.Setenv("WK_SEND_STRESS_DIAL_TIMEOUT", "2s")
	t.Setenv("WK_SEND_STRESS_ACK_TIMEOUT", "1800ms")
	t.Setenv("WK_SEND_STRESS_SEED", "42")

	cfg := loadSendStressConfig(t)
	require.True(t, cfg.Enabled)
	require.Equal(t, sendStressModeThroughput, cfg.Mode)
	require.Equal(t, 1500*time.Millisecond, cfg.Duration)
	require.Equal(t, 7, cfg.Workers)
	require.Equal(t, 11, cfg.Senders)
	require.Equal(t, 13, cfg.MessagesPerWorker)
	require.Equal(t, 9, cfg.MaxInflightPerWorker)
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

func TestSendStressConfigDefaultsToLatencyMode(t *testing.T) {
	clearSendStressConfigEnv(t)

	cfg := loadSendStressConfig(t)

	require.Equal(t, sendStressModeLatency, cfg.Mode)
	require.Equal(t, 1, cfg.MaxInflightPerWorker)
}

func TestSendStressConfigParsesThroughputModeAndInflightOverride(t *testing.T) {
	clearSendStressConfigEnv(t)
	t.Setenv(sendStressModeEnv, string(sendStressModeThroughput))
	t.Setenv(sendStressMaxInflightEnv, "7")

	cfg := loadSendStressConfig(t)

	require.Equal(t, sendStressModeThroughput, cfg.Mode)
	require.Equal(t, 7, cfg.MaxInflightPerWorker)
}

func TestSendStressConfigDefaultsThroughputModeToMultiInflight(t *testing.T) {
	clearSendStressConfigEnv(t)
	t.Setenv(sendStressModeEnv, string(sendStressModeThroughput))

	cfg := loadSendStressConfig(t)

	require.Equal(t, sendStressModeThroughput, cfg.Mode)
	require.Equal(t, sendStressThroughputInflight, cfg.MaxInflightPerWorker)
}

func TestValidateSendStressConfigRejectsInvalidThroughputInflight(t *testing.T) {
	err := validateSendStressConfig(sendStressConfig{
		Mode:                 sendStressModeThroughput,
		MaxInflightPerWorker: 0,
		Workers:              2,
		Senders:              2,
		MessagesPerWorker:    1,
		Duration:             time.Second,
		DialTimeout:          time.Second,
		AckTimeout:           time.Second,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), sendStressMaxInflightEnv)
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

func TestSendStressActiveTargetCountUsesAllSendersInThroughputMode(t *testing.T) {
	require.Equal(t, 128, sendStressActiveTargetCount(sendStressConfig{Mode: sendStressModeThroughput, Workers: 32}, 128))
	require.Equal(t, 32, sendStressActiveTargetCount(sendStressConfig{Mode: sendStressModeLatency, Workers: 32}, 128))
	require.Equal(t, 16, sendStressActiveTargetCount(sendStressConfig{Mode: sendStressModeLatency, Workers: 32}, 16))
}

func TestSendStressOutcomeErrorRate(t *testing.T) {
	outcome := sendStressOutcome{Total: 10, Success: 8, Failed: 2}
	require.InDelta(t, 20.0, outcome.ErrorRate(), 0.001)
}

func TestSendStressThroughputTrackerCompletesOutOfOrderAcks(t *testing.T) {
	client := sendStressWorkerClient{
		target: sendStressTarget{
			SenderUID:     "sender",
			RecipientUID:  "recipient",
			ChannelID:     "channel",
			ChannelType:   frame.ChannelTypePerson,
			OwnerNodeID:   1,
			ConnectNodeID: 1,
		},
	}
	tracker := newSendStressInflightTracker()
	first := tracker.Start(client, 0, "measure", 0, 2, "m2", []byte("two"))
	second := tracker.Start(client, 0, "measure", 1, 3, "m3", []byte("three"))

	require.NoError(t, tracker.Complete(&frame.SendackPacket{
		ClientSeq:   3,
		ClientMsgNo: "m3",
		ReasonCode:  frame.ReasonSuccess,
		MessageID:   103,
		MessageSeq:  203,
	}, nil))
	require.NoError(t, tracker.Complete(&frame.SendackPacket{
		ClientSeq:   2,
		ClientMsgNo: "m2",
		ReasonCode:  frame.ReasonSuccess,
		MessageID:   102,
		MessageSeq:  202,
	}, nil))

	secondResult := <-second
	require.True(t, secondResult.ok)
	require.EqualValues(t, 3, secondResult.record.ClientSeq)
	require.EqualValues(t, 203, secondResult.record.MessageSeq)

	firstResult := <-first
	require.True(t, firstResult.ok)
	require.EqualValues(t, 2, firstResult.record.ClientSeq)
	require.EqualValues(t, 202, firstResult.record.MessageSeq)
}

func TestSendStressFrameReaderPreservesPartialFrameAcrossTimeout(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	reader := newSendStressFrameReader(clientConn)
	ack := &frame.SendackPacket{
		ClientSeq:   9,
		ClientMsgNo: "m9",
		ReasonCode:  frame.ReasonSuccess,
		MessageID:   109,
		MessageSeq:  209,
	}
	payload, err := codec.New().EncodeFrame(ack, frame.LatestVersion)
	require.NoError(t, err)
	require.Greater(t, len(payload), 4)

	go func() {
		_, _ = serverConn.Write(payload[:3])
		time.Sleep(80 * time.Millisecond)
		_, _ = serverConn.Write(payload[3:])
	}()

	_, err = reader.ReadWithin(20 * time.Millisecond)
	require.Error(t, err)
	require.True(t, isSendStressTimeout(err))

	f, err := reader.ReadWithin(time.Second)
	require.NoError(t, err)
	got, ok := f.(*frame.SendackPacket)
	require.True(t, ok)
	require.EqualValues(t, ack.ClientSeq, got.ClientSeq)
	require.Equal(t, ack.ClientMsgNo, got.ClientMsgNo)
	require.EqualValues(t, ack.MessageID, got.MessageID)
	require.EqualValues(t, ack.MessageSeq, got.MessageSeq)
}

func TestRunSendStressWorkersThroughputModeCapsInflightPerWorker(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	cfg := sendStressConfig{
		Mode:                 sendStressModeThroughput,
		MaxInflightPerWorker: 2,
		MessagesPerWorker:    4,
		AckTimeout:           2 * time.Second,
	}
	client := sendStressWorkerClient{
		target: sendStressTarget{
			SenderUID:     "sender",
			RecipientUID:  "recipient",
			ChannelID:     "channel",
			ChannelType:   frame.ChannelTypePerson,
			OwnerNodeID:   1,
			ConnectNodeID: 1,
		},
		conn:    clientConn,
		reader:  newSendStressFrameReader(clientConn),
		writeMu: &sync.Mutex{},
	}

	serverStarted := make(chan struct{}, cfg.MessagesPerWorker)
	releaseAcks := make(chan struct{})
	serverErr := make(chan error, 1)
	var maxInflight atomic.Int64
	go func() {
		serverErr <- runScriptedSendStressAckServer(serverConn, cfg.MessagesPerWorker, serverStarted, releaseAcks, &maxInflight)
	}()

	done := make(chan struct{})
	var (
		records  []sendStressRecord
		failures []string
		outcome  sendStressOutcome
		runErr   error
	)
	go func() {
		outcome, records, failures, runErr = runSendStressWorkerThroughput(client, 0, cfg, time.Now().Add(5*time.Second))
		close(done)
	}()

	for i := 0; i < cfg.MaxInflightPerWorker; i++ {
		select {
		case <-serverStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for initial inflight sends")
		}
	}

	select {
	case <-serverStarted:
		t.Fatal("worker exceeded max inflight before acknowledgements were released")
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseAcks)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for throughput worker to finish")
	}

	require.NoError(t, runErr)
	require.NoError(t, <-serverErr)
	require.Empty(t, failures)
	require.Len(t, records, cfg.MessagesPerWorker)
	require.EqualValues(t, cfg.MessagesPerWorker, outcome.Total)
	require.EqualValues(t, cfg.MessagesPerWorker, outcome.Success)
	require.Zero(t, outcome.Failed)
	require.LessOrEqual(t, maxInflight.Load(), int64(cfg.MaxInflightPerWorker))
}

func TestSendStressThreeNode(t *testing.T) {
	cfg := loadSendStressConfig(t)
	requireSendStressEnabled(t, cfg)

	harness := newThreeNodeAppHarnessWithConfigMutator(t, func(appCfg *Config) {
		if cfg.Mode == sendStressModeThroughput {
			appCfg.Gateway.DefaultSession.AsyncSendDispatch = true
		}
	})
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
		sendStressModeEnv,
		sendStressDurationEnv,
		sendStressWorkersEnv,
		sendStressSendersEnv,
		sendStressMessagesPerWorkerEnv,
		sendStressMaxInflightEnv,
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
			strings.HasPrefix(entry, sendStressModeEnv+"=") ||
			strings.HasPrefix(entry, sendStressDurationEnv+"=") ||
			strings.HasPrefix(entry, sendStressWorkersEnv+"=") ||
			strings.HasPrefix(entry, sendStressSendersEnv+"=") ||
			strings.HasPrefix(entry, sendStressMessagesPerWorkerEnv+"=") ||
			strings.HasPrefix(entry, sendStressMaxInflightEnv+"=") ||
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
		channelType := frame.ChannelTypePerson
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
			Status:       uint8(channel.StatusActive),
			Features:     uint64(channel.MessageSeqFormatLegacyU32),
			LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		}
		require.NoError(t, leader.Store().UpsertChannelRuntimeMeta(context.Background(), meta))

		id := channel.ChannelID{ID: channelID, Type: channelType}
		for _, app := range harness.appsWithLeaderFirst(leaderID) {
			_, err := app.channelMetaSync.RefreshChannelMeta(context.Background(), id)
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

	activeTargetCount := sendStressActiveTargetCount(cfg, len(targets))
	require.Positive(t, activeTargetCount)

	clients := make([]sendStressWorkerClient, 0, activeTargetCount)
	for worker := 0; worker < activeTargetCount; worker++ {
		target := targets[worker]
		app := harness.apps[target.ConnectNodeID]
		require.NotNil(t, app, "connect node %d is not running", target.ConnectNodeID)
		conn := runSendStressClient(t, app, target.SenderUID, cfg)
		clients = append(clients, sendStressWorkerClient{
			target:  target,
			conn:    conn,
			reader:  newSendStressFrameReader(conn),
			writeMu: &sync.Mutex{},
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
	records := make([]sendStressRecord, 0, activeTargetCount*cfg.MessagesPerWorker)
	latencies := make([]time.Duration, 0, activeTargetCount*cfg.MessagesPerWorker)
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
			var (
				workerOutcome  sendStressOutcome
				workerRecords  []sendStressRecord
				workerFailures []string
				err            error
			)
			if cfg.Mode == sendStressModeThroughput {
				workerOutcome, workerRecords, workerFailures, err = runSendStressWorkerThroughput(client, worker, cfg, deadline)
			} else {
				workerOutcome, workerRecords, workerFailures = runSendStressWorkerLatency(client, worker, cfg, deadline)
			}
			if err != nil {
				workerFailures = append(workerFailures, err.Error())
			}

			total.Add(workerOutcome.Total)
			success.Add(workerOutcome.Success)
			failed.Add(workerOutcome.Failed)

			mu.Lock()
			records = append(records, workerRecords...)
			for _, record := range workerRecords {
				latencies = append(latencies, record.AckLatency)
			}
			mu.Unlock()
			for _, failure := range workerFailures {
				appendFailure("%s", failure)
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
		"send stress metrics: mode=%s max_inflight=%d duration=%s workers=%d senders=%d total=%d success=%d failed=%d qps=%.2f p50=%s p95=%s p99=%s max=%s verification_count=%d verification_failures=%d",
		cfg.Mode,
		cfg.MaxInflightPerWorker,
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

func runSendStressWorkerLatency(client sendStressWorkerClient, worker int, cfg sendStressConfig, deadline time.Time) (sendStressOutcome, []sendStressRecord, []string) {
	records := make([]sendStressRecord, 0, cfg.MessagesPerWorker)
	failures := make([]string, 0, 1)
	outcome := sendStressOutcome{}

	for iteration := 0; iteration < cfg.MessagesPerWorker; iteration++ {
		if time.Now().After(deadline) {
			break
		}

		outcome.Total++
		clientSeq := uint64(iteration + 2)
		clientMsgNo := fmt.Sprintf("send-stress-%d-%s-%02d-%d", worker, client.target.SenderUID, iteration, cfg.Seed)
		payload := []byte(fmt.Sprintf("send-stress payload worker=%d sender=%s recipient=%s iteration=%d seed=%d", worker, client.target.SenderUID, client.target.RecipientUID, iteration, cfg.Seed))
		record, failure, ok := executeSendStressAttempt(client, worker, "measure", iteration, clientSeq, clientMsgNo, payload, cfg.AckTimeout)
		if !ok {
			outcome.Failed++
			failures = append(failures, failure)
			break
		}
		outcome.Success++
		records = append(records, record)
	}
	return outcome, records, failures
}

func runSendStressWorkerThroughput(client sendStressWorkerClient, worker int, cfg sendStressConfig, deadline time.Time) (sendStressOutcome, []sendStressRecord, []string, error) {
	if cfg.MaxInflightPerWorker <= 0 {
		return sendStressOutcome{}, nil, nil, fmt.Errorf("%s must be > 0, got %d", sendStressMaxInflightEnv, cfg.MaxInflightPerWorker)
	}

	var (
		outcome  sendStressOutcome
		records  = make([]sendStressRecord, 0, cfg.MessagesPerWorker)
		failures = make([]string, 0, 1)
		mu       sync.Mutex
	)
	appendResult := func(result sendStressAttemptResult) {
		mu.Lock()
		defer mu.Unlock()
		if result.ok {
			outcome.Success++
			records = append(records, result.record)
			return
		}
		outcome.Failed++
		if len(failures) < 8 {
			failures = append(failures, result.failure)
		}
	}

	tracker := newSendStressInflightTracker()
	slots := make(chan struct{}, cfg.MaxInflightPerWorker)
	writerDone := make(chan struct{})
	readerErrCh := make(chan error, 1)
	stopWriter := make(chan struct{})
	var stopWriterOnce sync.Once
	stop := func() {
		stopWriterOnce.Do(func() {
			close(stopWriter)
		})
	}

	go func() {
		readerErrCh <- readSendStressThroughputAcks(client, cfg.AckTimeout, tracker, writerDone, stop)
	}()

	nextClientSeq := uint64(2)
	for iteration := 0; iteration < cfg.MessagesPerWorker; iteration++ {
		if time.Now().After(deadline) {
			break
		}
		stopped := false
		select {
		case <-stopWriter:
			stopped = true
		default:
		}
		if stopped {
			break
		}

		slots <- struct{}{}
		mu.Lock()
		outcome.Total++
		mu.Unlock()

		clientSeq := nextClientSeq
		nextClientSeq++
		clientMsgNo := fmt.Sprintf("send-stress-%d-%s-%02d-%d", worker, client.target.SenderUID, iteration, cfg.Seed)
		payload := []byte(fmt.Sprintf("send-stress payload worker=%d sender=%s recipient=%s iteration=%d seed=%d", worker, client.target.SenderUID, client.target.RecipientUID, iteration, cfg.Seed))
		packet := &frame.SendPacket{
			ChannelID:   client.target.RecipientUID,
			ChannelType: client.target.ChannelType,
			ClientSeq:   clientSeq,
			ClientMsgNo: clientMsgNo,
			Payload:     payload,
		}
		_ = tracker.startAt(client, worker, "measure", iteration, clientSeq, clientMsgNo, payload, time.Now(), func(result sendStressAttemptResult) {
			appendResult(result)
			<-slots
		})
		if err := writeSendStressClientFrame(client, packet, cfg.AckTimeout); err != nil {
			_ = tracker.Fail(clientSeq, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d write error=%v", worker, client.target.SenderUID, client.target.ConnectNodeID, "measure", iteration, err))
			stop()
			break
		}
	}

	close(writerDone)
	readerErr := <-readerErrCh
	if readerErr != nil {
		return outcome, records, failures, readerErr
	}
	return outcome, records, failures, nil
}

func readSendStressThroughputAcks(client sendStressWorkerClient, ackTimeout time.Duration, tracker *sendStressInflightTracker, writerDone <-chan struct{}, stopWriter func()) error {
	for {
		if tracker.Pending() == 0 {
			select {
			case <-writerDone:
				return nil
			default:
			}
		}

		f, err := readSendStressClientFrame(client, minSendStressDuration(ackTimeout, 200*time.Millisecond))
		if err != nil {
			if isSendStressTimeout(err) {
				select {
				case <-writerDone:
					if tracker.Pending() == 0 {
						return nil
					}
				default:
				}
				continue
			}
			tracker.FailAll("throughput ack reader error: %v", err)
			stopWriter()
			return err
		}

		switch pkt := f.(type) {
		case *frame.SendackPacket:
			if err := tracker.Complete(pkt, nil); err != nil {
				tracker.FailAll("throughput ack reader mismatch: %v", err)
				stopWriter()
				return err
			}
		case *frame.RecvPacket:
			if err := writeSendStressClientFrame(client, &frame.RecvackPacket{
				MessageID:  pkt.MessageID,
				MessageSeq: pkt.MessageSeq,
			}, ackTimeout); err != nil {
				tracker.FailAll("throughput recvack write error: %v", err)
				stopWriter()
				return err
			}
		default:
			err := fmt.Errorf("unexpected frame while waiting for throughput sendack: %T", f)
			tracker.FailAll("%v", err)
			stopWriter()
			return err
		}
	}
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
	packet := &frame.SendPacket{
		ChannelID:   client.target.RecipientUID,
		ChannelType: client.target.ChannelType,
		ClientSeq:   clientSeq,
		ClientMsgNo: clientMsgNo,
		Payload:     payload,
	}

	sendStart := time.Now()
	if err := writeSendStressClientFrame(client, packet, ackTimeout); err != nil {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d write error=%v", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, err), false
	}

	sendack, framesBeforeAck, err := waitForSendStressSendack(client, ackTimeout)
	ackLatency := time.Since(sendStart)
	if err != nil {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d readack error=%v", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, err), false
	}
	if sendack.ClientSeq != clientSeq || sendack.ClientMsgNo != clientMsgNo {
		return sendStressRecord{}, fmt.Sprintf("worker=%d sender=%s connect_node=%d phase=%s iteration=%d ack_mismatch client_seq=%d/%d client_msg_no=%s/%s", worker, client.target.SenderUID, client.target.ConnectNodeID, phase, iteration, sendack.ClientSeq, clientSeq, sendack.ClientMsgNo, clientMsgNo), false
	}
	if sendack.ReasonCode != frame.ReasonSuccess || sendack.MessageID == 0 || sendack.MessageSeq == 0 {
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
		id := channel.ChannelID{
			ID:   record.ChannelID,
			Type: record.ChannelType,
		}
		owner := harness.apps[record.OwnerNodeID]
		require.NotNil(t, owner, "owner node %d is not running", record.OwnerNodeID)

		ownerMsg := waitForAppCommittedMessage(t, channelStoreForID(owner.ChannelLogDB(), id), record.MessageSeq, 5*time.Second)
		require.Equalf(t, record.Payload, ownerMsg.Payload, "owner mismatch worker=%d iteration=%d sender=%s recipient=%s connect_node=%d message_seq=%d message_id=%d client_seq=%d client_msg_no=%s frames_before_ack=%v owner_from=%s owner_client_msg_no=%s owner_payload=%q", record.Worker, record.Iteration, record.SenderUID, record.RecipientUID, record.ConnectNodeID, record.MessageSeq, record.MessageID, record.ClientSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.FromUID, ownerMsg.ClientMsgNo, string(ownerMsg.Payload))
		require.Equalf(t, record.SenderUID, ownerMsg.FromUID, "owner sender mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s frames_before_ack=%v owner_from=%s owner_payload=%q", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.FromUID, string(ownerMsg.Payload))
		require.Equalf(t, record.ClientMsgNo, ownerMsg.ClientMsgNo, "owner client_msg_no mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s frames_before_ack=%v owner_client_msg_no=%s owner_payload=%q", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo, record.FramesBeforeAck, ownerMsg.ClientMsgNo, string(ownerMsg.Payload))
		require.Equalf(t, record.ChannelID, ownerMsg.ChannelID, "owner channel mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)
		require.Equalf(t, record.ChannelType, ownerMsg.ChannelType, "owner channel type mismatch worker=%d iteration=%d message_seq=%d client_msg_no=%s", record.Worker, record.Iteration, record.MessageSeq, record.ClientMsgNo)

		for _, app := range harness.orderedApps() {
			msg := waitForAppCommittedMessage(t, channelStoreForID(app.ChannelLogDB(), id), record.MessageSeq, 5*time.Second)
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

	if err := writeSendStressFrame(conn, &frame.ConnectPacket{
		Version:         frame.LatestVersion,
		UID:             senderUID,
		DeviceID:        senderUID + "-stress-device",
		DeviceFlag:      frame.APP,
		ClientTimestamp: time.Now().UnixMilli(),
	}, cfg.DialTimeout); err != nil {
		_ = conn.Close()
		return nil, err
	}

	f, err := readSendStressFrameWithin(conn, cfg.AckTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	connack, ok := f.(*frame.ConnackPacket)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("expected *frame.ConnackPacket, got %T", f)
	}
	if connack.ReasonCode != frame.ReasonSuccess {
		_ = conn.Close()
		return nil, fmt.Errorf("connect failed with reason %s", connack.ReasonCode)
	}
	return conn, nil
}

func writeSendStressClientFrame(client sendStressWorkerClient, f frame.Frame, timeout time.Duration) error {
	if client.writeMu == nil {
		return writeSendStressFrame(client.conn, f, timeout)
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	return writeSendStressFrame(client.conn, f, timeout)
}

func writeSendStressFrame(conn net.Conn, f frame.Frame, timeout time.Duration) error {
	payload, err := codec.New().EncodeFrame(f, frame.LatestVersion)
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

func minSendStressDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

func isSendStressTimeout(err error) bool {
	var netErr net.Error
	return err != nil && errors.As(err, &netErr) && netErr.Timeout()
}

func readSendStressClientFrame(client sendStressWorkerClient, timeout time.Duration) (frame.Frame, error) {
	if client.reader != nil {
		return client.reader.ReadWithin(timeout)
	}
	return readSendStressFrameWithin(client.conn, timeout)
}

func readSendStressFrameWithin(conn net.Conn, timeout time.Duration) (frame.Frame, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()
	return codec.New().DecodePacketWithConn(conn, frame.LatestVersion)
}

func waitForSendStressSendack(client sendStressWorkerClient, timeout time.Duration) (*frame.SendackPacket, []string, error) {
	deadline := time.Now().Add(timeout)
	framesBeforeAck := make([]string, 0, 2)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, framesBeforeAck, fmt.Errorf("timed out waiting for sendack")
		}

		f, err := readSendStressClientFrame(client, remaining)
		if err != nil {
			return nil, framesBeforeAck, err
		}

		switch pkt := f.(type) {
		case *frame.SendackPacket:
			return pkt, framesBeforeAck, nil
		case *frame.RecvPacket:
			framesBeforeAck = append(framesBeforeAck, fmt.Sprintf("recv(message_id=%d,message_seq=%d,client_seq=%d,client_msg_no=%s,payload=%q)", pkt.MessageID, pkt.MessageSeq, pkt.ClientSeq, pkt.ClientMsgNo, string(pkt.Payload)))
			if err := writeSendStressClientFrame(client, &frame.RecvackPacket{
				MessageID:  pkt.MessageID,
				MessageSeq: pkt.MessageSeq,
			}, remaining); err != nil {
				return nil, framesBeforeAck, err
			}
		default:
			return nil, framesBeforeAck, fmt.Errorf("unexpected frame while waiting for sendack: %T", f)
		}
	}
}

func runScriptedSendStressAckServer(conn net.Conn, expected int, started chan<- struct{}, releaseAcks <-chan struct{}, maxInflightBeforeRelease *atomic.Int64) error {
	type ackEnvelope struct {
		packet *frame.SendackPacket
	}

	acks := make(chan ackEnvelope, expected)
	readerErrCh := make(chan error, 1)
	var currentInflight atomic.Int64
	var releaseObserved atomic.Bool

	go func() {
		for i := 0; i < expected; i++ {
			f, err := readSendStressFrameWithin(conn, 5*time.Second)
			if err != nil {
				readerErrCh <- err
				close(acks)
				return
			}
			send, ok := f.(*frame.SendPacket)
			if !ok {
				readerErrCh <- fmt.Errorf("expected *frame.SendPacket, got %T", f)
				close(acks)
				return
			}
			inflight := currentInflight.Add(1)
			if maxInflightBeforeRelease != nil && !releaseObserved.Load() {
				for {
					prev := maxInflightBeforeRelease.Load()
					if inflight <= prev || maxInflightBeforeRelease.CompareAndSwap(prev, inflight) {
						break
					}
				}
			}
			started <- struct{}{}
			acks <- ackEnvelope{
				packet: &frame.SendackPacket{
					ClientSeq:   send.ClientSeq,
					ClientMsgNo: send.ClientMsgNo,
					ReasonCode:  frame.ReasonSuccess,
					MessageID:   int64(1000 + send.ClientSeq),
					MessageSeq:  send.ClientSeq,
				},
			}
		}
		close(acks)
		readerErrCh <- nil
	}()

	pending := make([]ackEnvelope, 0, expected)
	released := false
	flushPending := func() error {
		for len(pending) > 0 {
			env := pending[0]
			pending = pending[1:]
			if err := writeSendStressFrame(conn, env.packet, 5*time.Second); err != nil {
				return err
			}
			currentInflight.Add(-1)
		}
		return nil
	}

	releaseCh := releaseAcks
	for {
		select {
		case env, ok := <-acks:
			if !ok {
				if !released && releaseCh != nil {
					<-releaseCh
					releaseCh = nil
					released = true
				}
				if err := flushPending(); err != nil {
					return err
				}
				return <-readerErrCh
			}
			if released {
				if err := writeSendStressFrame(conn, env.packet, 5*time.Second); err != nil {
					return err
				}
				currentInflight.Add(-1)
				continue
			}
			pending = append(pending, env)
		case <-releaseCh:
			releaseObserved.Store(true)
			released = true
			releaseCh = nil
			if err := flushPending(); err != nil {
				return err
			}
		}
	}
}
