package log

import "testing"

func BenchmarkISRBridgeRead(b *testing.B) {
	fixture := newBenchmarkStoreFixture(b, benchmarkStoreFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})
	logStore := fixture.store.isrLogStore()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := benchmarkReadOffset(i, benchmarkDefaultPreloadedMessages, benchmarkDefaultPageLimit)
		if _, err := logStore.Read(offset, benchmarkDefaultFetchBytes); err != nil {
			b.Fatalf("Read(%d): %v", i, err)
		}
	}
}

func BenchmarkNewReplicaRecoveryOnStore(b *testing.B) {
	fixture := newBenchmarkRecoveredReplicaFixture(b, benchmarkRecoveredReplicaFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})
	defer fixture.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replica := fixture.OpenReplica(b)
		if status := replica.Status(); status.HW != uint64(benchmarkDefaultPreloadedMessages) {
			b.Fatalf("HW = %d, want %d", status.HW, benchmarkDefaultPreloadedMessages)
		}
		fixture.CloseReplica(b)
	}
}
