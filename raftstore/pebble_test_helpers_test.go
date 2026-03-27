package raftstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"go.etcd.io/raft/v3/raftpb"
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

func openBenchDB(tb testing.TB) (*DB, string) {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "raft")
	db, err := Open(path)
	if err != nil {
		tb.Fatalf("Open(%q) error = %v", path, err)
	}
	return db, path
}

func mustOpenPebbleDB(tb testing.TB, path string) *DB {
	tb.Helper()

	db, err := Open(path)
	if err != nil {
		tb.Fatalf("Open(%q) error = %v", path, err)
	}
	return db
}

func reopenPebbleDB(tb testing.TB, db *DB, path string) *DB {
	tb.Helper()

	if db != nil {
		if err := db.Close(); err != nil {
			tb.Fatalf("Close(%q) error = %v", path, err)
		}
	}
	return mustOpenPebbleDB(tb, path)
}

func closeBenchDB(tb testing.TB, db *DB, path string) {
	tb.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		tb.Fatalf("Close(%q) error = %v", path, err)
	}
}

func benchEntry(index, term uint64, payloadSize int) raftpb.Entry {
	entry := raftpb.Entry{
		Index: index,
		Term:  term,
	}
	if payloadSize > 0 {
		entry.Data = make([]byte, payloadSize)
		fill := byte(index)
		for i := range entry.Data {
			entry.Data[i] = fill
		}
	}
	return entry
}

func benchEntries(startIndex uint64, count int, term uint64, payloadSize int) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, benchEntry(startIndex+uint64(i), term, payloadSize))
	}
	return entries
}

func benchPersistentState(startIndex uint64, count int, term uint64, payloadSize int) multiraft.PersistentState {
	entries := benchEntries(startIndex, count, term, payloadSize)
	if len(entries) == 0 {
		return multiraft.PersistentState{}
	}

	hs := raftpb.HardState{
		Term:   term,
		Commit: entries[len(entries)-1].Index,
	}
	return multiraft.PersistentState{
		HardState: &hs,
		Entries:   entries,
	}
}

func mustSave(tb testing.TB, store multiraft.Storage, st multiraft.PersistentState) {
	tb.Helper()
	if err := store.Save(context.Background(), st); err != nil {
		tb.Fatalf("Save() error = %v", err)
	}
}

func mustMarkApplied(tb testing.TB, store multiraft.Storage, index uint64) {
	tb.Helper()
	if err := store.MarkApplied(context.Background(), index); err != nil {
		tb.Fatalf("MarkApplied(%d) error = %v", index, err)
	}
}

func mustEntries(tb testing.TB, store multiraft.Storage, lo, hi, maxSize uint64) []raftpb.Entry {
	tb.Helper()
	entries, err := store.Entries(context.Background(), lo, hi, maxSize)
	if err != nil {
		tb.Fatalf("Entries(%d,%d,%d) error = %v", lo, hi, maxSize, err)
	}
	return entries
}

func mustLastIndex(tb testing.TB, store multiraft.Storage) uint64 {
	tb.Helper()
	index, err := store.LastIndex(context.Background())
	if err != nil {
		tb.Fatalf("LastIndex() error = %v", err)
	}
	return index
}

func mustFirstIndex(tb testing.TB, store multiraft.Storage) uint64 {
	tb.Helper()
	index, err := store.FirstIndex(context.Background())
	if err != nil {
		tb.Fatalf("FirstIndex() error = %v", err)
	}
	return index
}

func mustTerm(tb testing.TB, store multiraft.Storage, index uint64) uint64 {
	tb.Helper()
	term, err := store.Term(context.Background(), index)
	if err != nil {
		tb.Fatalf("Term(%d) error = %v", index, err)
	}
	return term
}

func mustInitialState(tb testing.TB, store multiraft.Storage) multiraft.BootstrapState {
	tb.Helper()
	state, err := store.InitialState(context.Background())
	if err != nil {
		tb.Fatalf("InitialState() error = %v", err)
	}
	return state
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
