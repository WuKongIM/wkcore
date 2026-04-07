package app

import (
	"context"
	"errors"

	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

var (
	errRemoteAckNotifierRequired     = errors.New("app: remote ack notifier required")
	errRemoteOfflineNotifierRequired = errors.New("app: remote offline notifier required")
)

type asyncCommittedDispatcher struct {
	localNodeID uint64
	channelLog  channellog.Cluster
	delivery    committedSubmitter
	nodeClient  *accessnode.Client
}

func (d asyncCommittedDispatcher) SubmitCommitted(ctx context.Context, msg channellog.Message) error {
	if d.delivery == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	go func() {
		if d.channelLog == nil {
			_ = d.delivery.SubmitCommitted(ctx, msg)
			return
		}

		status, err := d.channelLog.Status(channellog.ChannelKey{
			ChannelID:   msg.ChannelID,
			ChannelType: msg.ChannelType,
		})
		if err != nil || status.Leader == 0 {
			return
		}

		ownerNodeID := uint64(status.Leader)
		if ownerNodeID == d.localNodeID {
			_ = d.delivery.SubmitCommitted(ctx, msg)
			return
		}
		if d.nodeClient == nil {
			return
		}
		_ = d.nodeClient.SubmitCommitted(ctx, ownerNodeID, msg)
	}()
	return nil
}

type localDeliveryResolver struct {
	subscribers deliveryusecase.SubscriberResolver
	authority   presence.Authoritative
	pageSize    int
}

type localResolveToken struct {
	snapshot deliveryusecase.SnapshotToken
	pending  []deliveryruntime.RouteKey
	done     bool
}

func (r localDeliveryResolver) BeginResolve(ctx context.Context, key deliveryruntime.ChannelKey, _ deliveryruntime.CommittedEnvelope) (any, error) {
	if r.subscribers == nil {
		return nil, nil
	}
	snapshot, err := r.subscribers.BeginSnapshot(ctx, channellog.ChannelKey{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
	})
	if err != nil {
		return nil, err
	}
	return &localResolveToken{snapshot: snapshot}, nil
}

func (r localDeliveryResolver) ResolvePage(ctx context.Context, token any, cursor string, limit int) ([]deliveryruntime.RouteKey, string, bool, error) {
	if r.subscribers == nil || r.authority == nil {
		return nil, "", true, nil
	}
	if limit <= 0 {
		limit = r.pageSize
	}
	if limit <= 0 {
		limit = 128
	}

	resolveToken, ok := token.(*localResolveToken)
	if !ok {
		return nil, "", true, nil
	}

	out := make([]deliveryruntime.RouteKey, 0, limit)
	if len(resolveToken.pending) > 0 {
		taken := limit
		if taken > len(resolveToken.pending) {
			taken = len(resolveToken.pending)
		}
		out = append(out, resolveToken.pending[:taken]...)
		resolveToken.pending = resolveToken.pending[taken:]
		if len(out) == limit || resolveToken.done {
			return out, cursor, resolveToken.done && len(resolveToken.pending) == 0, nil
		}
	}

	pageSize := r.pageSize
	if pageSize <= 0 {
		pageSize = 128
	}

	for len(out) < limit {
		if resolveToken.done {
			return out, cursor, true, nil
		}

		uids, nextCursor, done, err := r.subscribers.NextPage(ctx, resolveToken.snapshot, cursor, pageSize)
		if err != nil {
			return nil, "", false, err
		}
		cursor = nextCursor
		resolveToken.done = done
		if len(uids) == 0 {
			if done {
				return out, cursor, true, nil
			}
			continue
		}

		endpointsByUID, err := r.authority.EndpointsByUIDs(ctx, uids)
		if err != nil {
			return nil, "", false, err
		}

		expanded := make([]deliveryruntime.RouteKey, 0, len(uids))
		for _, uid := range uids {
			for _, route := range endpointsByUID[uid] {
				expanded = append(expanded, deliveryruntime.RouteKey{
					UID:       route.UID,
					NodeID:    route.NodeID,
					BootID:    route.BootID,
					SessionID: route.SessionID,
				})
			}
		}
		if len(expanded) == 0 {
			if done {
				return out, cursor, true, nil
			}
			continue
		}

		remaining := limit - len(out)
		if len(expanded) <= remaining {
			out = append(out, expanded...)
			continue
		}
		out = append(out, expanded[:remaining]...)
		resolveToken.pending = append(resolveToken.pending[:0], expanded[remaining:]...)
		return out, cursor, false, nil
	}
	return out, cursor, false, nil
}

type localDeliveryPush struct {
	online        online.Registry
	localNodeID   uint64
	gatewayBootID uint64
}

func (p localDeliveryPush) Push(_ context.Context, cmd deliveryruntime.PushCommand) (deliveryruntime.PushResult, error) {
	frame := buildRealtimeRecvPacket(cmd.Envelope, recipientUIDForRoutes(cmd.Routes))
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
	frame := buildRealtimeRecvPacket(cmd.Envelope, recipientUIDForRoutes(cmd.Routes))
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
	local       routeAcker
	remoteAcks  *deliveryruntime.AckIndex
	notifier    deliveryOwnerNotifier
}

func (r ackRouting) AckRoute(ctx context.Context, cmd message.RouteAckCommand) error {
	if r.remoteAcks != nil {
		if binding, ok := r.remoteAcks.Lookup(cmd.SessionID, cmd.MessageID); ok {
			if binding.OwnerNodeID != 0 && binding.OwnerNodeID != r.localNodeID {
				if r.notifier == nil {
					return errRemoteAckNotifierRequired
				}
				if err := r.notifier.NotifyAck(ctx, binding.OwnerNodeID, cmd); err != nil {
					return err
				}
				r.remoteAcks.Remove(cmd.SessionID, cmd.MessageID)
				return nil
			}
			r.remoteAcks.Remove(cmd.SessionID, cmd.MessageID)
		}
	}
	if r.local == nil {
		return nil
	}
	return r.local.AckRoute(ctx, cmd)
}

type offlineRouting struct {
	localNodeID uint64
	local       sessionCloser
	remoteAcks  *deliveryruntime.AckIndex
	notifier    deliveryOwnerNotifier
}

func (r offlineRouting) SessionClosed(ctx context.Context, cmd message.SessionClosedCommand) error {
	var err error
	if r.remoteAcks != nil {
		ownerBindings := make(map[uint64][]deliveryruntime.AckBinding)
		for _, binding := range r.remoteAcks.LookupSession(cmd.SessionID) {
			if binding.OwnerNodeID == 0 || binding.OwnerNodeID == r.localNodeID {
				r.remoteAcks.Remove(binding.SessionID, binding.MessageID)
				continue
			}
			ownerBindings[binding.OwnerNodeID] = append(ownerBindings[binding.OwnerNodeID], binding)
		}
		for ownerNodeID, bindings := range ownerBindings {
			if r.notifier == nil {
				err = errors.Join(err, errRemoteOfflineNotifierRequired)
				continue
			}
			notifyErr := r.notifier.NotifyOffline(ctx, ownerNodeID, cmd)
			err = errors.Join(err, notifyErr)
			if notifyErr != nil {
				continue
			}
			for _, binding := range bindings {
				r.remoteAcks.Remove(binding.SessionID, binding.MessageID)
			}
		}
	}
	if r.local != nil {
		err = errors.Join(err, r.local.SessionClosed(ctx, cmd))
	}
	return err
}

type routeAcker interface {
	AckRoute(ctx context.Context, cmd message.RouteAckCommand) error
}

type sessionCloser interface {
	SessionClosed(ctx context.Context, cmd message.SessionClosedCommand) error
}

type deliveryOwnerNotifier interface {
	NotifyAck(ctx context.Context, nodeID uint64, cmd message.RouteAckCommand) error
	NotifyOffline(ctx context.Context, nodeID uint64, cmd message.SessionClosedCommand) error
}

type committedSubmitter interface {
	SubmitCommitted(ctx context.Context, msg channellog.Message) error
}

func recipientUIDForRoutes(routes []deliveryruntime.RouteKey) string {
	if len(routes) == 0 {
		return ""
	}
	return routes[0].UID
}

func buildRealtimeRecvPacket(msg channellog.Message, recipientUID string) *wkframe.RecvPacket {
	framer := msg.Framer
	framer.FrameType = wkframe.RECV

	packet := &wkframe.RecvPacket{
		Framer:      framer,
		Setting:     msg.Setting,
		MsgKey:      msg.MsgKey,
		Expire:      msg.Expire,
		MessageID:   int64(msg.MessageID),
		MessageSeq:  msg.MessageSeq,
		ClientMsgNo: msg.ClientMsgNo,
		StreamNo:    msg.StreamNo,
		StreamId:    msg.StreamID,
		StreamFlag:  msg.StreamFlag,
		Timestamp:   msg.Timestamp,
		ChannelID:   msg.ChannelID,
		ChannelType: msg.ChannelType,
		Topic:       msg.Topic,
		FromUID:     msg.FromUID,
		Payload:     append([]byte(nil), msg.Payload...),
		ClientSeq:   msg.ClientSeq,
	}
	if msg.ChannelType == wkframe.ChannelTypePerson && recipientUID != "" {
		packet.ChannelID = msg.FromUID
		packet.ChannelType = wkframe.ChannelTypePerson
	}
	return packet
}
