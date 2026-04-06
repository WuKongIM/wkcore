package app

import (
	"context"
	"errors"
	"time"

	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
)

type asyncCommittedDispatcher struct {
	localNodeID uint64
	channelLog  channellog.Cluster
	delivery    *deliveryusecase.App
	nodeClient  *accessnode.Client
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
		ownerNodeID := d.localNodeID
		if d.channelLog != nil {
			status, err := d.channelLog.Status(channellog.ChannelKey{
				ChannelID:   env.ChannelID,
				ChannelType: env.ChannelType,
			})
			if err == nil && status.Leader != 0 {
				ownerNodeID = uint64(status.Leader)
			}
		}
		if ownerNodeID == 0 || ownerNodeID == d.localNodeID || d.nodeClient == nil {
			_ = d.delivery.SubmitCommitted(ctx, env)
			return
		}
		_ = d.nodeClient.SubmitCommitted(ctx, ownerNodeID, env)
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
	frame := buildRealtimeRecvPacket(cmd.Envelope, p.nowFn()())
	return p.pushFrame(frame, cmd.Routes), nil
}

func (p localDeliveryPush) pushFrame(frame wkframe.Frame, routes []deliveryruntime.RouteKey) deliveryruntime.PushResult {
	result := deliveryruntime.PushResult{}
	for _, route := range routes {
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
	return result
}

func (p localDeliveryPush) nowFn() func() time.Time {
	if p.now == nil {
		return time.Now
	}
	return p.now
}

type distributedDeliveryPush struct {
	localNodeID uint64
	local       localDeliveryPush
	client      *accessnode.Client
	codec       wkcodec.Protocol
}

func (p distributedDeliveryPush) Push(ctx context.Context, cmd deliveryruntime.PushCommand) (deliveryruntime.PushResult, error) {
	if p.codec == nil {
		p.codec = wkcodec.New()
	}
	frame := buildRealtimeRecvPacket(cmd.Envelope, p.local.nowFn()())
	frameBytes, err := p.codec.EncodeFrame(frame, wkframe.LatestVersion)
	if err != nil {
		return deliveryruntime.PushResult{}, err
	}

	localRoutes := make([]deliveryruntime.RouteKey, 0, len(cmd.Routes))
	remoteRoutes := make(map[uint64][]deliveryruntime.RouteKey)
	for _, route := range cmd.Routes {
		if route.NodeID == p.localNodeID {
			localRoutes = append(localRoutes, route)
			continue
		}
		remoteRoutes[route.NodeID] = append(remoteRoutes[route.NodeID], route)
	}

	result := deliveryruntime.PushResult{}
	if len(localRoutes) > 0 {
		localResult := p.local.pushFrame(frame, localRoutes)
		result.Accepted = append(result.Accepted, localResult.Accepted...)
		result.Retryable = append(result.Retryable, localResult.Retryable...)
		result.Dropped = append(result.Dropped, localResult.Dropped...)
	}

	for nodeID, routes := range remoteRoutes {
		if p.client == nil {
			result.Retryable = append(result.Retryable, routes...)
			continue
		}
		resp, err := p.client.PushBatch(ctx, nodeID, accessnode.DeliveryPushCommand{
			OwnerNodeID: p.localNodeID,
			ChannelID:   cmd.Envelope.ChannelID,
			ChannelType: cmd.Envelope.ChannelType,
			MessageID:   cmd.Envelope.MessageID,
			MessageSeq:  cmd.Envelope.MessageSeq,
			Routes:      append([]deliveryruntime.RouteKey(nil), routes...),
			Frame:       append([]byte(nil), frameBytes...),
		})
		if err != nil {
			result.Retryable = append(result.Retryable, routes...)
			continue
		}
		result.Accepted = append(result.Accepted, resp.Accepted...)
		result.Retryable = append(result.Retryable, resp.Retryable...)
		result.Dropped = append(result.Dropped, resp.Dropped...)
	}
	return result, nil
}

type ackRouting struct {
	localNodeID uint64
	local       *deliveryusecase.App
	remoteAcks  *deliveryruntime.AckIndex
	nodeClient  *accessnode.Client
}

func (r ackRouting) AckRoute(ctx context.Context, cmd message.RouteAckCommand) error {
	if r.remoteAcks != nil {
		if binding, ok := r.remoteAcks.Lookup(cmd.SessionID, cmd.MessageID); ok {
			r.remoteAcks.Remove(cmd.SessionID, cmd.MessageID)
			if binding.OwnerNodeID != 0 && binding.OwnerNodeID != r.localNodeID && r.nodeClient != nil {
				return r.nodeClient.NotifyAck(ctx, binding.OwnerNodeID, cmd)
			}
		}
	}
	if r.local == nil {
		return nil
	}
	return r.local.AckRoute(ctx, cmd)
}

type offlineRouting struct {
	localNodeID uint64
	local       *deliveryusecase.App
	remoteAcks  *deliveryruntime.AckIndex
	nodeClient  *accessnode.Client
}

func (r offlineRouting) SessionClosed(ctx context.Context, cmd message.SessionClosedCommand) error {
	var err error
	if r.remoteAcks != nil {
		ownerNodes := make(map[uint64]struct{})
		for _, binding := range r.remoteAcks.LookupSession(cmd.SessionID) {
			r.remoteAcks.Remove(binding.SessionID, binding.MessageID)
			if binding.OwnerNodeID == 0 || binding.OwnerNodeID == r.localNodeID {
				continue
			}
			ownerNodes[binding.OwnerNodeID] = struct{}{}
		}
		for ownerNodeID := range ownerNodes {
			if r.nodeClient == nil {
				continue
			}
			err = errors.Join(err, r.nodeClient.NotifyOffline(ctx, ownerNodeID, cmd))
		}
	}
	if r.local != nil {
		err = errors.Join(err, r.local.SessionClosed(ctx, cmd))
	}
	return err
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
