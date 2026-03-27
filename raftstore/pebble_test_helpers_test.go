package raftstore

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type pebbleBenchConfig struct {
	mode    string
	groups  int
	entries int
	payload int
}

type pebbleStressConfig struct {
	enabled  bool
	duration time.Duration
	groups   int
	writers  int
	payload  int
}

func loadPebbleBenchConfig(tb testing.TB) pebbleBenchConfig {
	tb.Helper()

	mode := os.Getenv("WRAFT_RAFTSTORE_BENCH_SCALE")
	if mode == "" {
		mode = "default"
	}

	switch mode {
	case "default":
		return pebbleBenchConfig{
			mode:    mode,
			groups:  8,
			entries: 128,
			payload: 256,
		}
	case "heavy":
		return pebbleBenchConfig{
			mode:    mode,
			groups:  64,
			entries: 2048,
			payload: 1024,
		}
	default:
		tb.Fatalf("unsupported WRAFT_RAFTSTORE_BENCH_SCALE %q", mode)
		return pebbleBenchConfig{}
	}
}

func loadPebbleStressConfig() (pebbleStressConfig, error) {
	cfg := pebbleStressConfig{
		enabled:  os.Getenv("WRAFT_RAFTSTORE_STRESS") == "1",
		duration: 5 * time.Minute,
		groups:   64,
		writers:  8,
		payload:  256,
	}
	if !cfg.enabled {
		return cfg, nil
	}

	if value := os.Getenv("WRAFT_RAFTSTORE_STRESS_DURATION"); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return pebbleStressConfig{}, fmt.Errorf("parse WRAFT_RAFTSTORE_STRESS_DURATION: %w", err)
		}
		cfg.duration = duration
	}

	var err error
	if cfg.groups, err = loadPositiveIntEnv("WRAFT_RAFTSTORE_STRESS_GROUPS", cfg.groups); err != nil {
		return pebbleStressConfig{}, err
	}
	if cfg.writers, err = loadPositiveIntEnv("WRAFT_RAFTSTORE_STRESS_WRITERS", cfg.writers); err != nil {
		return pebbleStressConfig{}, err
	}
	if cfg.payload, err = loadPositiveIntEnv("WRAFT_RAFTSTORE_STRESS_PAYLOAD", cfg.payload); err != nil {
		return pebbleStressConfig{}, err
	}

	return cfg, nil
}

func loadPositiveIntEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be > 0", name)
	}
	return parsed, nil
}

func TestPebbleBenchScaleConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv("WRAFT_RAFTSTORE_BENCH_SCALE", "")
	cfg := loadPebbleBenchConfig(t)
	if cfg.mode != "default" {
		t.Fatalf("mode = %q, want %q", cfg.mode, "default")
	}

	t.Setenv("WRAFT_RAFTSTORE_BENCH_SCALE", "heavy")
	cfg = loadPebbleBenchConfig(t)
	if cfg.mode != "heavy" {
		t.Fatalf("mode = %q, want %q", cfg.mode, "heavy")
	}
}

func TestPebbleStressConfigRejectsInvalidValues(t *testing.T) {
	t.Setenv("WRAFT_RAFTSTORE_STRESS", "1")
	t.Setenv("WRAFT_RAFTSTORE_STRESS_DURATION", "not-a-duration")

	if _, err := loadPebbleStressConfig(); err == nil {
		t.Fatal("loadPebbleStressConfig() error = nil, want invalid duration error")
	}
}
