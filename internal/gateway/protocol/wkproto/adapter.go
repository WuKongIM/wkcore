package wkproto

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	gatewaytypes "github.com/WuKongIM/WuKongIM/internal/gateway/types"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	codec "github.com/WuKongIM/WuKongIM/pkg/proto/wkproto"
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

func (a *Adapter) Decode(sess session.Session, in []byte) ([]wkpacket.Frame, int, error) {
	if a == nil || len(in) == 0 {
		return nil, 0, nil
	}

	frames := make([]wkpacket.Frame, 0, 1)
	consumed := 0
	version := uint8(wkpacket.LatestVersion)
	if sessVersion, ok := sessionVersion(sess, false); ok {
		version = sessVersion
	}
	for consumed < len(in) {
		frame, n, err := a.codec.DecodeFrame(in[consumed:], version)
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

func (a *Adapter) Encode(sess session.Session, frame wkpacket.Frame, _ session.OutboundMeta) ([]byte, error) {
	if a == nil {
		return nil, nil
	}
	version, ok := sessionVersion(sess, true)
	if !ok {
		version = uint8(wkpacket.LegacyMessageSeqVersion)
	}
	return a.codec.EncodeFrame(frame, version)
}

func (a *Adapter) OnOpen(session.Session) error {
	return nil
}

func (a *Adapter) OnClose(session.Session) error {
	return nil
}

func sessionVersion(sess session.Session, outbound bool) (uint8, bool) {
	if sess == nil {
		if outbound {
			return 0, false
		}
		return wkpacket.LatestVersion, true
	}
	value := sess.Value(gatewaytypes.SessionValueProtocolVersion)
	version, ok := value.(uint8)
	if !ok || version == 0 {
		if outbound {
			return 0, false
		}
		return wkpacket.LatestVersion, true
	}
	return version, true
}
