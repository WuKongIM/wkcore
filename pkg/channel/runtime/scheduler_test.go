package runtime

import (
	"testing"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestSchedulerPrefersHighPriorityWork(t *testing.T) {
	s := newScheduler()
	s.enqueue("channel/1/YQ==", PriorityLow)
	s.enqueue("channel/1/Yg==", PriorityHigh)

	first, ok := s.popReady()
	require.True(t, ok)
	require.Equal(t, PriorityHigh, first.priority)
}

func TestSchedulerDoesNotRunSameKeyConcurrently(t *testing.T) {
	s := newScheduler()
	key := core.ChannelKey("channel/1/YQ==")
	s.begin(key)
	s.enqueue(key, PriorityNormal)
	require.True(t, s.isDirty(key))
}
