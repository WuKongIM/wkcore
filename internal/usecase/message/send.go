package message

import (
	"context"
	"errors"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func (a *App) Send(ctx context.Context, cmd SendCommand) (SendResult, error) {
	if cmd.FromUID == "" {
		return SendResult{}, ErrUnauthenticatedSender
	}

	if cmd.ChannelType != wkframe.ChannelTypePerson && cmd.ChannelType != wkframe.ChannelTypeGroup {
		return SendResult{Reason: wkframe.ReasonNotSupportChannelType}, nil
	}
	if cmd.ChannelType == wkframe.ChannelTypePerson {
		channelID, err := runtimechannelid.NormalizePersonChannel(cmd.FromUID, cmd.ChannelID)
		if err != nil {
			return SendResult{}, err
		}
		cmd.ChannelID = channelID
	}

	if a.cluster == nil {
		return SendResult{}, ErrClusterRequired
	}

	return a.sendDurable(ctx, cmd)
}

func (a *App) sendDurable(ctx context.Context, cmd SendCommand) (SendResult, error) {
	draft := buildDurableMessage(cmd, a.now())
	result, err := sendWithMetaRefreshRetry(ctx, a.cluster, a.refresher, channellog.AppendRequest{
		ChannelID:             cmd.ChannelID,
		ChannelType:           cmd.ChannelType,
		Message:               draft,
		SupportsMessageSeqU64: supportsMessageSeqU64(cmd.ProtocolVersion),
		ExpectedChannelEpoch:  cmd.ExpectedChannelEpoch,
		ExpectedLeaderEpoch:   cmd.ExpectedLeaderEpoch,
	})
	if err != nil {
		return SendResult{}, err
	}

	sendResult := SendResult{
		MessageID:  int64(result.MessageID),
		MessageSeq: result.MessageSeq,
		Reason:     wkframe.ReasonSuccess,
	}

	if a.dispatcher != nil {
		_ = a.dispatcher.SubmitCommitted(ctx, result.Message)
	}
	return sendResult, nil
}

func buildDurableMessage(cmd SendCommand, now time.Time) channellog.Message {
	return channellog.Message{
		Framer:      cmd.Framer,
		Setting:     cmd.Setting,
		MsgKey:      cmd.MsgKey,
		Expire:      cmd.Expire,
		ClientSeq:   cmd.ClientSeq,
		ClientMsgNo: cmd.ClientMsgNo,
		StreamNo:    cmd.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   cmd.ChannelID,
		ChannelType: cmd.ChannelType,
		Topic:       cmd.Topic,
		FromUID:     cmd.FromUID,
		Payload:     append([]byte(nil), cmd.Payload...),
	}
}

func (a *App) deliverPerson(ctx context.Context, cmd SendCommand, msgID int64, msgSeq uint64) error {
	frame := buildPersonRecvPacket(cmd, msgID, msgSeq, a.now())
	if a.recipients == nil {
		recipients := a.online.ConnectionsByUID(cmd.ChannelID)
		if len(recipients) == 0 {
			return nil
		}
		return a.delivery.Deliver(recipients, frame)
	}

	endpoints, lookupErr := a.recipients.EndpointsByUID(ctx, cmd.ChannelID)
	if lookupErr != nil {
		return lookupErr
	}
	if len(endpoints) == 0 {
		return nil
	}

	localRecipients := make([]online.OnlineConn, 0, len(endpoints))
	type remoteKey struct {
		nodeID uint64
		bootID uint64
	}
	remoteRecipients := make(map[remoteKey][]uint64)

	for _, endpoint := range endpoints {
		if a.localNodeID != 0 && endpoint.NodeID == a.localNodeID {
			if conn, ok := a.localRecipient(cmd.ChannelID, endpoint); ok {
				localRecipients = append(localRecipients, conn)
			}
			continue
		}
		key := remoteKey{nodeID: endpoint.NodeID, bootID: endpoint.BootID}
		remoteRecipients[key] = append(remoteRecipients[key], endpoint.SessionID)
	}

	var err error
	if len(localRecipients) > 0 {
		err = errors.Join(err, a.delivery.Deliver(localRecipients, frame))
	}
	if a.remote == nil {
		return err
	}
	for key, sessionIDs := range remoteRecipients {
		err = errors.Join(err, a.remote.DeliverRemote(ctx, RemoteDeliveryCommand{
			NodeID:     key.nodeID,
			UID:        cmd.ChannelID,
			BootID:     key.bootID,
			SessionIDs: append([]uint64(nil), sessionIDs...),
			Frame:      frame,
		}))
	}
	return err
}

func (a *App) localRecipient(uid string, endpoint Endpoint) (online.OnlineConn, bool) {
	if a.localBootID != 0 && endpoint.BootID != a.localBootID {
		return online.OnlineConn{}, false
	}
	conn, ok := a.online.Connection(endpoint.SessionID)
	if !ok {
		return online.OnlineConn{}, false
	}
	if conn.UID != uid || conn.State != online.LocalRouteStateActive {
		return online.OnlineConn{}, false
	}
	return conn, true
}

func buildPersonRecvPacket(cmd SendCommand, msgID int64, msgSeq uint64, now time.Time) *wkframe.RecvPacket {
	framer := cmd.Framer
	framer.FrameType = wkframe.RECV

	return &wkframe.RecvPacket{
		Framer:      framer,
		Setting:     cmd.Setting,
		MsgKey:      cmd.MsgKey,
		Expire:      cmd.Expire,
		MessageID:   msgID,
		MessageSeq:  msgSeq,
		ClientMsgNo: cmd.ClientMsgNo,
		StreamNo:    cmd.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   cmd.FromUID,
		ChannelType: wkframe.ChannelTypePerson,
		Topic:       cmd.Topic,
		FromUID:     cmd.FromUID,
		Payload:     cmd.Payload,
		ClientSeq:   cmd.ClientSeq,
	}
}

func supportsMessageSeqU64(version uint8) bool {
	return version == 0 || version > wkframe.LegacyMessageSeqVersion
}
