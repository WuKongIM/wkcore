package channellog

import "testing"

func BenchmarkStoreAppendStoredMessages(b *testing.B) {
	fixture := newBenchmarkStoreFixture(b, benchmarkStoreFixtureConfig{})
	payload := benchmarkPayload(benchmarkDefaultPayloadBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fixture.store.appendPayloads([][]byte{mustBenchmarkStoredPayload(b, uint64(i+1), payload)}); err != nil {
			b.Fatalf("appendPayloads(%d): %v", i, err)
		}
	}
}

func BenchmarkStoreReadOffsets(b *testing.B) {
	fixture := newBenchmarkStoreFixture(b, benchmarkStoreFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := benchmarkReadOffset(i, benchmarkDefaultPreloadedMessages, benchmarkDefaultPageLimit)
		if _, err := fixture.store.readOffsets(offset, benchmarkDefaultPageLimit, benchmarkDefaultFetchBytes); err != nil {
			b.Fatalf("readOffsets(%d): %v", i, err)
		}
	}
}

func BenchmarkStoreLoadNextRangeMsgs(b *testing.B) {
	fixture := newBenchmarkStoreFixture(b, benchmarkStoreFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fromSeq := benchmarkReadSeq(i, benchmarkDefaultPreloadedMessages, benchmarkDefaultPageLimit)
		if _, err := fixture.store.LoadNextRangeMsgs(fromSeq, 0, benchmarkDefaultPageLimit); err != nil {
			b.Fatalf("LoadNextRangeMsgs(%d): %v", i, err)
		}
	}
}

func BenchmarkStoreLoadPrevRangeMsgs(b *testing.B) {
	fixture := newBenchmarkStoreFixture(b, benchmarkStoreFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startSeq := benchmarkTailSeq(i, benchmarkDefaultPreloadedMessages, benchmarkDefaultPageLimit)
		if _, err := fixture.store.LoadPrevRangeMsgs(startSeq, 0, benchmarkDefaultPageLimit); err != nil {
			b.Fatalf("LoadPrevRangeMsgs(%d): %v", i, err)
		}
	}
}
