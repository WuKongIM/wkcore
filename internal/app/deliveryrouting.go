package app

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type asyncCommittedDispatcher struct {
	delivery *deliveryusecase.App
}

func (d asyncCommittedDispatcher) SubmitCommitted(ctx context.Context, env message.CommittedMessageEnvelope) error {
	if d.delivery == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	go func() {
		_ = d.delivery.SubmitCommitted(ctx, env)
	}()
	return nil
}

type localDeliveryResolver struct {
	authority presence.Authoritative
}

func (r localDeliveryResolver) ResolveRoutes(ctx context.Context, key deliveryruntime.ChannelKey, _ deliveryruntime.CommittedEnvelope) ([]deliveryruntime.RouteKey, error) {
	if r.authority == nil {
		return nil, nil
	}
	routes, err := r.authority.EndpointsByUID(ctx, key.ChannelID)
	if err != nil {
		return nil, err
	}
	out := make([]deliveryruntime.RouteKey, 0, len(routes))
	for _, route := range routes {
		out = append(out, deliveryruntime.RouteKey{
			UID:       route.UID,
			NodeID:    route.NodeID,
			BootID:    route.BootID,
			SessionID: route.SessionID,
		})
	}
	return out, nil
}

type localDeliveryPush struct {
	online        online.Registry
	localNodeID   uint64
	gatewayBootID uint64
	now           func() time.Time
}

func (p localDeliveryPush) Push(_ context.Context, cmd deliveryruntime.PushCommand) (deliveryruntime.PushResult, error) {
	if p.now == nil {
		p.now = time.Now
	}
	frame := buildRealtimeRecvPacket(cmd.Envelope, p.now())
	result := deliveryruntime.PushResult{}
	for _, route := range cmd.Routes {
		switch {
		case p.localNodeID != 0 && route.NodeID != p.localNodeID:
			result.Dropped = append(result.Dropped, route)
		case p.gatewayBootID != 0 && route.BootID != p.gatewayBootID:
			result.Dropped = append(result.Dropped, route)
		default:
			conn, ok := p.online.Connection(route.SessionID)
			if !ok || conn.UID != route.UID || conn.State != online.LocalRouteStateActive || conn.Session == nil {
				result.Dropped = append(result.Dropped, route)
				continue
			}
			if err := conn.Session.WriteFrame(frame); err != nil {
				result.Retryable = append(result.Retryable, route)
				continue
			}
			result.Accepted = append(result.Accepted, route)
		}
	}
	return result, nil
}

func buildRealtimeRecvPacket(env deliveryruntime.CommittedEnvelope, now time.Time) *wkframe.RecvPacket {
	framer := env.Framer
	framer.FrameType = wkframe.RECV

	packet := &wkframe.RecvPacket{
		Framer:      framer,
		Setting:     env.Setting,
		MsgKey:      env.MsgKey,
		Expire:      env.Expire,
		MessageID:   int64(env.MessageID),
		MessageSeq:  env.MessageSeq,
		ClientMsgNo: env.ClientMsgNo,
		StreamNo:    env.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   env.ChannelID,
		ChannelType: env.ChannelType,
		Topic:       env.Topic,
		FromUID:     env.SenderUID,
		Payload:     append([]byte(nil), env.Payload...),
		ClientSeq:   env.ClientSeq,
	}
	if env.ChannelType == wkframe.ChannelTypePerson {
		packet.ChannelID = env.SenderUID
		packet.ChannelType = wkframe.ChannelTypePerson
	}
	return packet
}
