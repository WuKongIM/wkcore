package gateway

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestHandlerOnFrameReturnsUnsupportedFrameError(t *testing.T) {
	handler := New(Options{})

	err := handler.OnFrame(newAuthedContext(t, 1, "u1"), unsupportedFrame{})

	require.ErrorIs(t, err, ErrUnsupportedFrame)
}

type unsupportedFrame struct {
	wkframe.Framer
}
