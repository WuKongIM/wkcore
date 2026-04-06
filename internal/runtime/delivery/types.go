package delivery

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

type ChannelKey = channellog.ChannelKey

type CommittedEnvelope struct {
	ChannelID   string
	ChannelType uint8
	MessageID   uint64
	MessageSeq  uint64
	SenderUID   string
	ClientMsgNo string
	Topic       string
	Payload     []byte
	Framer      wkframe.Framer
	Setting     wkframe.Setting
	MsgKey      string
	Expire      uint32
	StreamNo    string
	ClientSeq   uint64
}

type RouteKey struct {
	UID       string
	NodeID    uint64
	BootID    uint64
	SessionID uint64
}

type RouteAck struct {
	UID        string
	SessionID  uint64
	MessageID  uint64
	MessageSeq uint64
}

type SessionClosed struct {
	UID       string
	SessionID uint64
}

type AckBinding struct {
	SessionID   uint64
	MessageID   uint64
	ChannelID   string
	ChannelType uint8
	Route       RouteKey
}

type PushCommand struct {
	Envelope CommittedEnvelope
	Routes   []RouteKey
	Attempt  int
}

type PushResult struct {
	Accepted  []RouteKey
	Retryable []RouteKey
	Dropped   []RouteKey
}

type RetryEntry struct {
	When        time.Time
	ChannelID   string
	ChannelType uint8
	MessageID   uint64
	Route       RouteKey
	Attempt     int
}

type StartDispatch struct {
	Envelope CommittedEnvelope
}

type RouteAcked struct {
	MessageID uint64
	Route     RouteKey
}

type RouteOffline struct {
	MessageID uint64
	Route     RouteKey
}

type RetryTick struct {
	Entry RetryEntry
}

type RouteDeliveryState struct {
	Attempt  int
	Accepted bool
}

type InflightMessage struct {
	MessageID       uint64
	MessageSeq      uint64
	Envelope        CommittedEnvelope
	Routes          map[RouteKey]*RouteDeliveryState
	PendingRouteCnt int
}

type Resolver interface {
	ResolveRoutes(ctx context.Context, key ChannelKey, env CommittedEnvelope) ([]RouteKey, error)
}

type Pusher interface {
	Push(ctx context.Context, cmd PushCommand) (PushResult, error)
}

type Clock interface {
	Now() time.Time
}

type Config struct {
	Resolver         Resolver
	Push             Pusher
	Clock            Clock
	ShardCount       int
	IdleTimeout      time.Duration
	RetryDelays      []time.Duration
	MaxRetryAttempts int
}

type defaultClock struct{}

func (defaultClock) Now() time.Time {
	return time.Now()
}

type noopResolver struct{}

func (noopResolver) ResolveRoutes(context.Context, ChannelKey, CommittedEnvelope) ([]RouteKey, error) {
	return nil, nil
}

type noopPusher struct{}

func (noopPusher) Push(context.Context, PushCommand) (PushResult, error) {
	return PushResult{}, nil
}
