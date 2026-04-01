package service

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestServiceOnFrameReturnsUnsupportedFrameError(t *testing.T) {
	svc := New(Options{})

	err := svc.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.PingPacket{})

	require.ErrorIs(t, err, ErrUnsupportedFrame)
}

func TestServiceOnFrameRecvackIsNoop(t *testing.T) {
	svc := New(Options{})

	err := svc.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.RecvackPacket{})

	require.NoError(t, err)
}
