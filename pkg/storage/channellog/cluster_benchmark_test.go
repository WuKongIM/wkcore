package channellog

import (
	"context"
	"testing"
)

func BenchmarkClusterAppend(b *testing.B) {
	fixture := newBenchmarkClusterFixture(b, benchmarkClusterFixtureConfig{})
	payload := benchmarkPayload(benchmarkDefaultPayloadBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fixture.cluster.Append(context.Background(), benchmarkAppendRequest(fixture.key, payload)); err != nil {
			b.Fatalf("Append(%d): %v", i, err)
		}
	}
}

func BenchmarkClusterFetch(b *testing.B) {
	fixture := newBenchmarkClusterFixture(b, benchmarkClusterFixtureConfig{
		preloadedMessages: benchmarkDefaultPreloadedMessages,
		committedHW:       benchmarkDefaultPreloadedMessages,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fromSeq := benchmarkReadSeq(i, benchmarkDefaultPreloadedMessages, benchmarkDefaultPageLimit)
		if _, err := fixture.cluster.Fetch(context.Background(), FetchRequest{
			Key:      fixture.key,
			FromSeq:  fromSeq,
			Limit:    benchmarkDefaultPageLimit,
			MaxBytes: benchmarkDefaultFetchBytes,
		}); err != nil {
			b.Fatalf("Fetch(%d): %v", i, err)
		}
	}
}
