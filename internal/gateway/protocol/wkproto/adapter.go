package wkproto

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/wkproto"
)

const Name = "wkproto"

type Adapter struct {
	codec *codec.WKProto
}

func New() *Adapter {
	return &Adapter{
		codec: codec.New(),
	}
}

func (a *Adapter) Name() string {
	if a == nil {
		return ""
	}
	return Name
}

func (a *Adapter) Decode(_ session.Session, in []byte) ([]wkpacket.Frame, int, error) {
	if a == nil || len(in) == 0 {
		return nil, 0, nil
	}

	frames := make([]wkpacket.Frame, 0, 1)
	consumed := 0
	for consumed < len(in) {
		frame, n, err := a.codec.DecodeFrame(in[consumed:], wkpacket.LatestVersion)
		if err != nil {
			return nil, 0, err
		}
		if frame == nil || n == 0 {
			break
		}
		frames = append(frames, frame)
		consumed += n
	}

	return frames, consumed, nil
}

func (a *Adapter) Encode(_ session.Session, frame wkpacket.Frame, _ session.OutboundMeta) ([]byte, error) {
	if a == nil {
		return nil, nil
	}
	return a.codec.EncodeFrame(frame, wkpacket.LatestVersion)
}

func (a *Adapter) OnOpen(session.Session) error {
	return nil
}

func (a *Adapter) OnClose(session.Session) error {
	return nil
}
