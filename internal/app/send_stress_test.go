package app

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

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

type sendStressOutcome struct {
	Total   uint64
	Success uint64
	Failed  uint64
}

func (o sendStressOutcome) ErrorRate() float64 {
	if o.Total == 0 {
		return 0
	}
	return float64(o.Failed) * 100 / float64(o.Total)
}

func loadSendStressConfig(t *testing.T) sendStressConfig {
	t.Helper()

	enabled, ok, err := strictEnvBool(sendStressEnv)
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
	if cfg.Senders <= 0 {
		t.Fatalf("%s must be > 0, got %d", sendStressSendersEnv, cfg.Senders)
	}
	if cfg.Senders < cfg.Workers {
		t.Fatalf("%s must be >= %s, got %d < %d", sendStressSendersEnv, sendStressWorkersEnv, cfg.Senders, cfg.Workers)
	}
	return cfg
}

func strictEnvBool(name string) (bool, bool, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
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

	assertSendStressConfigRejectsInvalidEnabledValue(t)
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

func clearSendStressConfigEnv(t *testing.T) {
	t.Helper()

	if os.Getenv("WK_SEND_STRESS_SKIP_CLEAR") == "1" {
		return
	}

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

func assertSendStressConfigRejectsInvalidEnabledValue(t *testing.T) {
	t.Helper()

	cmd := exec.Command("go", "test", "./internal/app", "-run", "TestSendStressConfigDefaultsAndOverrides", "-count=1")
	cmd.Dir = filepath.Clean(filepath.Join("..", ".."))
	cmd.Env = append(os.Environ(), "WK_SEND_STRESS=maybe", "WK_SEND_STRESS_SKIP_CLEAR=1")
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, output.String(), "parse WK_SEND_STRESS")
}
