package multiisr

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPressureConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv("MULTIISR_STRESS", "")

	cfg, err := loadPressureConfig()
	if err != nil {
		t.Fatalf("loadPressureConfig() error = %v", err)
	}
	if cfg.groups != 256 || cfg.peers != 8 {
		t.Fatalf("defaults = %+v, want groups=256 peers=8", cfg)
	}

	t.Setenv("MULTIISR_STRESS_GROUPS", "512")
	t.Setenv("MULTIISR_STRESS_PEERS", "12")

	cfg, err = loadPressureConfig()
	if err != nil {
		t.Fatalf("loadPressureConfig() error = %v", err)
	}
	if cfg.groups != 512 || cfg.peers != 12 {
		t.Fatalf("overrides = %+v, want groups=512 peers=12", cfg)
	}
}

func TestPressureConfigRejectsInvalidDuration(t *testing.T) {
	t.Setenv("MULTIISR_STRESS", "1")
	t.Setenv("MULTIISR_STRESS_DURATION", "bad")

	if _, err := loadPressureConfig(); err == nil {
		t.Fatal("loadPressureConfig() error = nil, want invalid duration error")
	}
}

const (
	defaultPressureDuration             = 10 * time.Second
	defaultPressureGroups               = 256
	defaultPressurePeers                = 8
	defaultPressureSeed           int64 = 1
	defaultPressureSnapshotInterval     = 32
	defaultPressureBackpressureInterval = 16
)

type pressureConfig struct {
	stressEnabled        bool
	duration             time.Duration
	groups               int
	peers                int
	seed                 int64
	snapshotInterval     int
	backpressureInterval int
}

func loadPressureConfig() (pressureConfig, error) {
	cfg := pressureConfig{
		duration:             defaultPressureDuration,
		groups:               defaultPressureGroups,
		peers:                defaultPressurePeers,
		seed:                 defaultPressureSeed,
		snapshotInterval:     defaultPressureSnapshotInterval,
		backpressureInterval: defaultPressureBackpressureInterval,
	}

	var err error
	if cfg.stressEnabled, err = loadPressureBool("MULTIISR_STRESS", false); err != nil {
		return pressureConfig{}, err
	}
	if cfg.duration, err = loadPressureDuration("MULTIISR_STRESS_DURATION", cfg.duration); err != nil {
		return pressureConfig{}, err
	}
	if cfg.groups, err = loadPressurePositiveInt("MULTIISR_STRESS_GROUPS", cfg.groups); err != nil {
		return pressureConfig{}, err
	}
	if cfg.peers, err = loadPressurePositiveInt("MULTIISR_STRESS_PEERS", cfg.peers); err != nil {
		return pressureConfig{}, err
	}
	if cfg.seed, err = loadPressureInt64("MULTIISR_STRESS_SEED", cfg.seed); err != nil {
		return pressureConfig{}, err
	}
	if cfg.snapshotInterval, err = loadPressurePositiveInt("MULTIISR_STRESS_SNAPSHOT_INTERVAL", cfg.snapshotInterval); err != nil {
		return pressureConfig{}, err
	}
	if cfg.backpressureInterval, err = loadPressurePositiveInt("MULTIISR_STRESS_BACKPRESSURE_INTERVAL", cfg.backpressureInterval); err != nil {
		return pressureConfig{}, err
	}
	return cfg, nil
}

func loadPressureBool(key string, defaultValue bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func loadPressureDuration(key string, defaultValue time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultValue, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: must be > 0", key)
	}
	return value, nil
}

func loadPressurePositiveInt(key string, defaultValue int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: must be > 0", key)
	}
	return value, nil
}

func loadPressureInt64(key string, defaultValue int64) (int64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}
