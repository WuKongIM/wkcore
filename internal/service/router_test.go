package service

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestServiceOnFrameReturnsUnsupportedFrameError(t *testing.T) {
	svc := New(Options{})

	err := svc.OnFrame(newAuthedContext(t, 1, "u1"), unsupportedFrame{})

	require.ErrorIs(t, err, ErrUnsupportedFrame)
}

func TestServiceOnFramePingIsNoop(t *testing.T) {
	svc := New(Options{})

	err := svc.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.PingPacket{})

	require.NoError(t, err)
}

func TestServiceOnFrameRecvackIsNoop(t *testing.T) {
	svc := New(Options{})

	err := svc.OnFrame(newAuthedContext(t, 1, "u1"), &wkpacket.RecvackPacket{})

	require.NoError(t, err)
}

type unsupportedFrame struct {
	wkpacket.Framer
}
