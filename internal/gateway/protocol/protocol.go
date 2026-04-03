package protocol

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type Adapter interface {
	Name() string
	Decode(session session.Session, in []byte) ([]wkframe.Frame, int, error)
	Encode(session session.Session, frame wkframe.Frame, meta session.OutboundMeta) ([]byte, error)
	OnOpen(session session.Session) error
	OnClose(session session.Session) error
}

type ReplyTokenTracker interface {
	TakeReplyTokens(session session.Session, count int) []string
}
