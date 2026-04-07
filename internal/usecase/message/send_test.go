package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

var fixedSendNow = time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

func TestSendRejectsUnauthenticatedSender(t *testing.T) {
	app := New(Options{Now: fixedNowFn})

	result, err := app.Send(context.Background(), SendCommand{
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
	})

	require.ErrorIs(t, err, ErrUnauthenticatedSender)
	require.Equal(t, SendResult{}, result)
}

func TestSendReturnsUnsupportedChannelType(t *testing.T) {
	app := New(Options{Now: fixedNowFn})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "group-1",
		ChannelType: 99,
		ClientSeq:   12,
		ClientMsgNo: "m3",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonNotSupportChannelType, result.Reason)
	require.Zero(t, result.MessageID)
	require.Zero(t, result.MessageSeq)
}

func TestSendReturnsClusterRequiredWhenClusterNotConfigured(t *testing.T) {
	app := New(Options{Now: fixedNowFn})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
	})

	require.ErrorIs(t, err, ErrClusterRequired)
	require.Equal(t, SendResult{}, result)
}

func TestSendReturnsSuccessAfterDurableWriteAndSubmitsCommittedEnvelope(t *testing.T) {
	dispatcher := &recordingCommittedDispatcher{}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 99, MessageSeq: 7}},
		},
	}
	app := New(Options{
		Now:                 fixedNowFn,
		Cluster:             cluster,
		CommittedDispatcher: dispatcher,
	})

	result, err := app.Send(context.Background(), SendCommand{
		Framer:      wkframe.Framer{NoPersist: true, RedDot: true, SyncOnce: true},
		Setting:     wkframe.SettingReceiptEnabled,
		MsgKey:      "k1",
		Expire:      60,
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Topic:       "chat",
		Payload:     []byte("hi"),
		ClientSeq:   9,
		ClientMsgNo: "m1",
		StreamNo:    "stream-1",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Equal(t, int64(99), result.MessageID)
	require.Equal(t, uint64(7), result.MessageSeq)
	require.Len(t, cluster.sendRequests, 1)
	require.Equal(t, "u2@u1", cluster.sendRequests[0].ChannelID)
	require.Equal(t, wkframe.Framer{NoPersist: true, RedDot: true, SyncOnce: true}, cluster.sendRequests[0].Message.Framer)
	require.Equal(t, wkframe.SettingReceiptEnabled, cluster.sendRequests[0].Message.Setting)
	require.Equal(t, "k1", cluster.sendRequests[0].Message.MsgKey)
	require.Equal(t, uint32(60), cluster.sendRequests[0].Message.Expire)
	require.Equal(t, uint64(9), cluster.sendRequests[0].Message.ClientSeq)
	require.Equal(t, "m1", cluster.sendRequests[0].Message.ClientMsgNo)
	require.Equal(t, "stream-1", cluster.sendRequests[0].Message.StreamNo)
	require.Equal(t, int32(fixedSendNow.Unix()), cluster.sendRequests[0].Message.Timestamp)
	require.Equal(t, "chat", cluster.sendRequests[0].Message.Topic)
	require.Equal(t, "u1", cluster.sendRequests[0].Message.FromUID)
	require.Equal(t, []byte("hi"), cluster.sendRequests[0].Message.Payload)
	require.Len(t, dispatcher.calls, 1)
	require.Equal(t, CommittedMessageEnvelope{
		ChannelID:   "u2@u1",
		ChannelType: wkframe.ChannelTypePerson,
		MessageID:   99,
		MessageSeq:  7,
		SenderUID:   "u1",
		ClientMsgNo: "m1",
		Topic:       "chat",
		Payload:     []byte("hi"),
		Framer:      wkframe.Framer{NoPersist: true, RedDot: true, SyncOnce: true},
		Setting:     wkframe.SettingReceiptEnabled,
		MsgKey:      "k1",
		Expire:      60,
		StreamNo:    "stream-1",
		ClientSeq:   9,
	}, dispatcher.calls[0])
}

func TestSendRecanonicalizesPrecomposedPersonChannelBeforeDurableWrite(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 88, MessageSeq: 12}},
		},
	}
	app := New(Options{
		Now:     fixedNowFn,
		Cluster: cluster,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u1@u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Len(t, cluster.sendRequests, 1)
	require.Equal(t, "u2@u1", cluster.sendRequests[0].ChannelID)
}

func TestSendRejectsThirdPartyPrecomposedPersonChannel(t *testing.T) {
	cluster := &fakeChannelCluster{}
	app := New(Options{
		Now:     fixedNowFn,
		Cluster: cluster,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u3@u4",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
	})

	require.Error(t, err)
	require.Equal(t, SendResult{}, result)
	require.Empty(t, cluster.sendRequests)
}

func TestSendReturnsSuccessWhenCommittedSubmitFails(t *testing.T) {
	dispatcher := &recordingCommittedDispatcher{err: errors.New("queue full")}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 101, MessageSeq: 5}},
		},
	}
	app := New(Options{
		Now:                 fixedNowFn,
		Cluster:             cluster,
		CommittedDispatcher: dispatcher,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   14,
		ClientMsgNo: "m5",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Equal(t, int64(101), result.MessageID)
	require.Equal(t, uint64(5), result.MessageSeq)
	require.Len(t, cluster.sendRequests, 1)
	require.Len(t, dispatcher.calls, 1)
}

func TestSendDoesNotPerformSynchronousDeliveryAfterDurableWrite(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
				{SessionID: 99, UID: "u2"},
			},
		},
	}
	delivery := &recordingDelivery{}
	remote := &recordingRemoteDelivery{}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 601, MessageSeq: 22}},
		},
	}
	recipients := fakeRecipientDirectory{
		endpointsByUID: map[string][]Endpoint{
			"u2": {
				{NodeID: 1, BootID: 11, SessionID: 2},
				{NodeID: 2, BootID: 22, SessionID: 8},
			},
		},
	}
	app := New(Options{
		Now:                 fixedNowFn,
		Cluster:             cluster,
		Online:              reg,
		Delivery:            delivery,
		Recipients:          recipients,
		RemoteDelivery:      remote,
		LocalNodeID:         1,
		LocalBootID:         11,
		CommittedDispatcher: &recordingCommittedDispatcher{},
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   31,
		ClientMsgNo: "m-remote",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Empty(t, delivery.calls)
	require.Empty(t, remote.calls)
}

func TestSendRetriesOnceAfterRefreshingMeta(t *testing.T) {
	dispatcher := &recordingCommittedDispatcher{}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{err: channellog.ErrStaleMeta},
			{result: channellog.SendResult{MessageID: 201, MessageSeq: 7}},
		},
	}
	refresher := &fakeMetaRefresher{
		metas: []channellog.ChannelMeta{{
			ChannelID:    "u2@u1",
			ChannelType:  wkframe.ChannelTypePerson,
			ChannelEpoch: 11,
			LeaderEpoch:  3,
		}},
	}
	app := New(Options{
		Now:                 fixedNowFn,
		Cluster:             cluster,
		MetaRefresher:       refresher,
		CommittedDispatcher: dispatcher,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   21,
		ClientMsgNo: "m6",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Equal(t, int64(201), result.MessageID)
	require.Equal(t, uint64(7), result.MessageSeq)
	require.Len(t, refresher.keys, 1)
	require.Equal(t, "u2@u1", refresher.keys[0].ChannelID)
	require.Len(t, cluster.appliedMetas, 1)
	require.Equal(t, uint64(11), cluster.appliedMetas[0].ChannelEpoch)
	require.Len(t, cluster.sendRequests, 2)
	require.Zero(t, cluster.sendRequests[0].ExpectedChannelEpoch)
	require.Equal(t, uint64(11), cluster.sendRequests[1].ExpectedChannelEpoch)
	require.Equal(t, uint64(3), cluster.sendRequests[1].ExpectedLeaderEpoch)
	require.Equal(t, []CommittedMessageEnvelope{{
		ChannelID:   "u2@u1",
		ChannelType: wkframe.ChannelTypePerson,
		MessageID:   201,
		MessageSeq:  7,
		SenderUID:   "u1",
		ClientMsgNo: "m6",
		Payload:     []byte("hi"),
		ClientSeq:   21,
	}}, dispatcher.calls)
}

func TestSendDurablePersonPropagatesRequestContextToClusterAndMetaRefresh(t *testing.T) {
	type ctxKey string

	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{err: channellog.ErrStaleMeta},
			{result: channellog.SendResult{MessageID: 401, MessageSeq: 19}},
		},
	}
	refresher := &fakeMetaRefresher{
		metas: []channellog.ChannelMeta{{
			ChannelID:    "u2@u1",
			ChannelType:  wkframe.ChannelTypePerson,
			ChannelEpoch: 12,
			LeaderEpoch:  4,
		}},
	}
	app := New(Options{
		Now:           fixedNowFn,
		Cluster:       cluster,
		MetaRefresher: refresher,
	})

	ctx := context.WithValue(context.Background(), ctxKey("request"), "durable-send")
	result, err := app.Send(ctx, SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientMsgNo: "m9",
		Payload:     []byte("hi"),
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Len(t, cluster.sendContexts, 2)
	require.Same(t, ctx, cluster.sendContexts[0])
	require.Same(t, ctx, cluster.sendContexts[1])
	require.Len(t, refresher.refreshContexts, 1)
	require.Same(t, ctx, refresher.refreshContexts[0])
}

func TestSendDurablePersonReturnsContextCanceled(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendFn: func(ctx context.Context, _ channellog.SendRequest) (channellog.SendResult, error) {
			<-ctx.Done()
			return channellog.SendResult{}, ctx.Err()
		},
	}
	app := New(Options{
		Now:     fixedNowFn,
		Cluster: cluster,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := app.Send(ctx, SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientMsgNo: "m10",
		Payload:     []byte("hi"),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, SendResult{}, result)
	require.Len(t, cluster.sendContexts, 1)
	require.Same(t, ctx, cluster.sendContexts[0])
}

func TestSendReturnsProtocolUpgradeRequiredWhenClusterRejectsLegacyClient(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{err: channellog.ErrProtocolUpgradeRequired},
		},
	}
	delivery := &recordingDelivery{}
	app := New(Options{
		Now:     fixedNowFn,
		Cluster: cluster,
		Online: &fakeRegistry{
			byUID: map[string][]online.OnlineConn{
				"u2": {
					{SessionID: 2, UID: "u2"},
				},
			},
		},
		Delivery: delivery,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:       "u1",
		ChannelID:       "u2",
		ChannelType:     wkframe.ChannelTypePerson,
		Payload:         []byte("hi"),
		ClientSeq:       23,
		ClientMsgNo:     "m8",
		ProtocolVersion: wkframe.LegacyMessageSeqVersion,
	})

	require.ErrorIs(t, err, channellog.ErrProtocolUpgradeRequired)
	require.Equal(t, SendResult{}, result)
	require.Empty(t, delivery.calls)
}

func TestNewPreservesInjectedCollaborators(t *testing.T) {
	identities := &fakeIdentityStore{}
	channels := &fakeChannelStore{}
	cluster := &fakeChannelCluster{}
	refresher := &fakeMetaRefresher{}
	reg := &fakeRegistry{}
	delivery := &recordingDelivery{}
	dispatcher := &recordingCommittedDispatcher{}
	acks := &recordingDeliveryAck{}
	offline := &recordingDeliveryOffline{}

	app := New(Options{
		IdentityStore:       identities,
		ChannelStore:        channels,
		Cluster:             cluster,
		MetaRefresher:       refresher,
		Online:              reg,
		Delivery:            delivery,
		CommittedDispatcher: dispatcher,
		DeliveryAck:         acks,
		DeliveryOffline:     offline,
		LocalBootID:         9,
		Now:                 fixedNowFn,
	})

	require.Same(t, identities, app.identities)
	require.Same(t, channels, app.channels)
	require.Same(t, cluster, app.cluster)
	require.Same(t, refresher, app.refresher)
	require.Same(t, reg, app.online)
	require.Same(t, delivery, app.delivery)
	require.Same(t, dispatcher, app.dispatcher)
	require.Same(t, acks, app.deliveryAck)
	require.Same(t, offline, app.deliveryOffline)
	require.Equal(t, uint64(9), app.localBootID)
	require.Same(t, reg, app.OnlineRegistry())
}

func fixedNowFn() time.Time {
	return fixedSendNow
}

type fakeRegistry struct {
	byUID map[string][]online.OnlineConn
}

func (f *fakeRegistry) Register(conn online.OnlineConn) error {
	if f.byUID == nil {
		f.byUID = make(map[string][]online.OnlineConn)
	}
	f.byUID[conn.UID] = append(f.byUID[conn.UID], conn)
	return nil
}

func (f *fakeRegistry) Unregister(sessionID uint64) {
	for uid, conns := range f.byUID {
		filtered := conns[:0]
		for _, conn := range conns {
			if conn.SessionID != sessionID {
				filtered = append(filtered, conn)
			}
		}
		if len(filtered) == 0 {
			delete(f.byUID, uid)
			continue
		}
		f.byUID[uid] = filtered
	}
}

func (f *fakeRegistry) MarkClosing(uint64) (online.OnlineConn, bool) {
	return online.OnlineConn{}, false
}

func (f *fakeRegistry) Connection(sessionID uint64) (online.OnlineConn, bool) {
	for _, conns := range f.byUID {
		for _, conn := range conns {
			if conn.SessionID == sessionID {
				return conn, true
			}
		}
	}
	return online.OnlineConn{}, false
}

func (f *fakeRegistry) ConnectionsByUID(uid string) []online.OnlineConn {
	conns := f.byUID[uid]
	out := make([]online.OnlineConn, len(conns))
	copy(out, conns)
	return out
}

func (f *fakeRegistry) ActiveConnectionsByGroup(uint64) []online.OnlineConn {
	return nil
}

func (f *fakeRegistry) ActiveGroups() []online.GroupSnapshot {
	return nil
}

type deliveryCall struct {
	recipients []online.OnlineConn
	frame      wkframe.Frame
}

type recordingDelivery struct {
	calls []deliveryCall
}

func (d *recordingDelivery) Deliver(recipients []online.OnlineConn, frame wkframe.Frame) error {
	copiedRecipients := make([]online.OnlineConn, len(recipients))
	copy(copiedRecipients, recipients)
	d.calls = append(d.calls, deliveryCall{recipients: copiedRecipients, frame: frame})
	return nil
}

type failingDelivery struct {
	err error
}

func (d failingDelivery) Deliver([]online.OnlineConn, wkframe.Frame) error {
	return d.err
}

type fakeRecipientDirectory struct {
	endpointsByUID map[string][]Endpoint
	err            error
}

func (f fakeRecipientDirectory) EndpointsByUID(_ context.Context, uid string) ([]Endpoint, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]Endpoint(nil), f.endpointsByUID[uid]...), nil
}

type recordingRemoteDelivery struct {
	calls []RemoteDeliveryCommand
}

func (d *recordingRemoteDelivery) DeliverRemote(_ context.Context, cmd RemoteDeliveryCommand) error {
	copied := cmd
	copied.SessionIDs = append([]uint64(nil), cmd.SessionIDs...)
	d.calls = append(d.calls, copied)
	return nil
}

type recordingCommittedDispatcher struct {
	calls []CommittedMessageEnvelope
	err   error
}

func (d *recordingCommittedDispatcher) SubmitCommitted(_ context.Context, env CommittedMessageEnvelope) error {
	copied := env
	copied.Payload = append([]byte(nil), env.Payload...)
	d.calls = append(d.calls, copied)
	return d.err
}

type failingRemoteDelivery struct {
	err error
}

func (d failingRemoteDelivery) DeliverRemote(context.Context, RemoteDeliveryCommand) error {
	return d.err
}

func requireRecvPacket(t *testing.T, frame wkframe.Frame) *wkframe.RecvPacket {
	t.Helper()

	recv, ok := frame.(*wkframe.RecvPacket)
	require.True(t, ok, "expected *wkframe.RecvPacket, got %T", frame)
	return recv
}

type fakeIdentityStore struct{}

func (*fakeIdentityStore) GetUser(context.Context, string) (metadb.User, error) {
	return metadb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (metadb.Channel, error) {
	return metadb.Channel{}, nil
}

type fakeChannelClusterSendReply struct {
	result channellog.SendResult
	err    error
}

type fakeChannelCluster struct {
	appliedMetas []channellog.ChannelMeta
	sendRequests []channellog.SendRequest
	sendContexts []context.Context
	sendReplies  []fakeChannelClusterSendReply
	sendFn       func(context.Context, channellog.SendRequest) (channellog.SendResult, error)
	applyErr     error
}

func (f *fakeChannelCluster) ApplyMeta(meta channellog.ChannelMeta) error {
	f.appliedMetas = append(f.appliedMetas, meta)
	return f.applyErr
}

func (f *fakeChannelCluster) Send(ctx context.Context, req channellog.SendRequest) (channellog.SendResult, error) {
	f.sendContexts = append(f.sendContexts, ctx)
	f.sendRequests = append(f.sendRequests, req)
	if f.sendFn != nil {
		return f.sendFn(ctx, req)
	}
	if len(f.sendReplies) == 0 {
		return channellog.SendResult{}, nil
	}
	reply := f.sendReplies[0]
	f.sendReplies = f.sendReplies[1:]
	return reply.result, reply.err
}

type fakeMetaRefresher struct {
	keys            []channellog.ChannelKey
	refreshContexts []context.Context
	metas           []channellog.ChannelMeta
	errs            []error
}

func (f *fakeMetaRefresher) RefreshChannelMeta(ctx context.Context, key channellog.ChannelKey) (channellog.ChannelMeta, error) {
	f.keys = append(f.keys, key)
	f.refreshContexts = append(f.refreshContexts, ctx)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return channellog.ChannelMeta{}, err
		}
	}
	if len(f.metas) == 0 {
		return channellog.ChannelMeta{}, nil
	}
	meta := f.metas[0]
	f.metas = f.metas[1:]
	return meta, nil
}
