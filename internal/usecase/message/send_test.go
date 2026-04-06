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
		ChannelType: wkframe.ChannelTypeGroup,
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

func TestSendDeliversDurablePersonMessage(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
				{SessionID: 3, UID: "u2"},
			},
		},
	}
	delivery := &recordingDelivery{}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 99, MessageSeq: 7}},
		},
	}
	app := New(Options{
		Now:      fixedNowFn,
		Cluster:  cluster,
		Online:   reg,
		Delivery: delivery,
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   9,
		ClientMsgNo: "m1",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Equal(t, int64(99), result.MessageID)
	require.Equal(t, uint64(7), result.MessageSeq)
	require.Len(t, cluster.sendRequests, 1)
	require.Len(t, delivery.calls, 1)
	require.Len(t, delivery.calls[0].recipients, 2)

	recv := requireRecvPacket(t, delivery.calls[0].frame)
	require.Equal(t, "u1", recv.FromUID)
	require.Equal(t, "u1", recv.ChannelID)
	require.Equal(t, wkframe.ChannelTypePerson, recv.ChannelType)
	require.Equal(t, int64(99), recv.MessageID)
	require.Equal(t, uint64(7), recv.MessageSeq)
	require.Equal(t, []byte("hi"), recv.Payload)
	require.Equal(t, uint64(9), recv.ClientSeq)
	require.Equal(t, "m1", recv.ClientMsgNo)
	require.Equal(t, int32(fixedSendNow.Unix()), recv.Timestamp)
}

func TestSendReturnsSuccessWhenRecipientOfflineOnLocalNode(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 301, MessageSeq: 15}},
		},
	}
	app := New(Options{
		Now:     fixedNowFn,
		Cluster: cluster,
		Online:  &fakeRegistry{},
	})

	result, err := app.Send(context.Background(), SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkframe.ChannelTypePerson,
		ClientSeq:   11,
		ClientMsgNo: "m2",
	})

	require.NoError(t, err)
	require.Equal(t, wkframe.ReasonSuccess, result.Reason)
	require.Equal(t, int64(301), result.MessageID)
	require.Equal(t, uint64(15), result.MessageSeq)
	require.Len(t, cluster.sendRequests, 1)
}

func TestSendReturnsSuccessWhenPostCommitDeliveryFails(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
				{SessionID: 3, UID: "u2"},
			},
		},
	}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channellog.SendResult{MessageID: 101, MessageSeq: 5}},
		},
	}
	app := New(Options{
		Now:      fixedNowFn,
		Cluster:  cluster,
		Online:   reg,
		Delivery: failingDelivery{err: errors.New("boom")},
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
}

func TestSendRetriesOnceAfterRefreshingMeta(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
			},
		},
	}
	delivery := &recordingDelivery{}
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{err: channellog.ErrStaleMeta},
			{result: channellog.SendResult{MessageID: 201, MessageSeq: 7}},
		},
	}
	refresher := &fakeMetaRefresher{
		metas: []channellog.ChannelMeta{{
			ChannelID:    "u2",
			ChannelType:  wkframe.ChannelTypePerson,
			ChannelEpoch: 11,
			LeaderEpoch:  3,
		}},
	}
	app := New(Options{
		Now:           fixedNowFn,
		Cluster:       cluster,
		MetaRefresher: refresher,
		Online:        reg,
		Delivery:      delivery,
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
	require.Equal(t, "u2", refresher.keys[0].ChannelID)
	require.Len(t, cluster.appliedMetas, 1)
	require.Equal(t, uint64(11), cluster.appliedMetas[0].ChannelEpoch)
	require.Len(t, cluster.sendRequests, 2)
	require.Zero(t, cluster.sendRequests[0].ExpectedChannelEpoch)
	require.Equal(t, uint64(11), cluster.sendRequests[1].ExpectedChannelEpoch)
	require.Equal(t, uint64(3), cluster.sendRequests[1].ExpectedLeaderEpoch)
	require.Len(t, delivery.calls, 1)

	recv := requireRecvPacket(t, delivery.calls[0].frame)
	require.Equal(t, int64(201), recv.MessageID)
	require.Equal(t, uint64(7), recv.MessageSeq)
	require.Equal(t, "u1", recv.FromUID)
	require.Equal(t, []byte("hi"), recv.Payload)
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
			ChannelID:    "u2",
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

	app := New(Options{
		IdentityStore: identities,
		ChannelStore:  channels,
		Cluster:       cluster,
		MetaRefresher: refresher,
		Online:        reg,
		Delivery:      delivery,
		Now:           fixedNowFn,
	})

	require.Same(t, identities, app.identities)
	require.Same(t, channels, app.channels)
	require.Same(t, cluster, app.cluster)
	require.Same(t, refresher, app.refresher)
	require.Same(t, reg, app.online)
	require.Same(t, delivery, app.delivery)
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
