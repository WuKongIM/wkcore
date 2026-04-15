package message

import (
	"testing"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
)

func TestShouldRefreshAndRetryIncludesErrRerouted(t *testing.T) {
	if !shouldRefreshAndRetry(raftcluster.ErrRerouted) {
		t.Fatal("shouldRefreshAndRetry(ErrRerouted) = false, want true")
	}
}
