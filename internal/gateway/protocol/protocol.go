package protocol

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
)

type Adapter interface {
	Name() string
	Decode(session session.Session, in []byte) ([]wkpacket.Frame, int, error)
	Encode(session session.Session, frame wkpacket.Frame, meta session.OutboundMeta) ([]byte, error)
	OnOpen(session session.Session) error
	OnClose(session session.Session) error
}

type ReplyTokenTracker interface {
	TakeReplyTokens(session session.Session, count int) []string
}
