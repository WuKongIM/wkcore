package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	"github.com/stretchr/testify/require"
)

var fixedSendNow = time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

func TestSendRejectsUnauthenticatedSender(t *testing.T) {
	app := New(Options{Now: fixedNowFn})

	result, err := app.Send(SendCommand{
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
	})

	require.ErrorIs(t, err, ErrUnauthenticatedSender)
	require.Equal(t, SendResult{}, result)
}

func TestSendReturnsUnsupportedChannelType(t *testing.T) {
	app := New(Options{Now: fixedNowFn})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "group-1",
		ChannelType: wkpacket.ChannelTypeGroup,
		ClientSeq:   12,
		ClientMsgNo: "m3",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonNotSupportChannelType, result.Reason)
	require.Zero(t, result.MessageID)
	require.Zero(t, result.MessageSeq)
}

func TestSendDeliversLocalPersonMessage(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
				{SessionID: 3, UID: "u2"},
			},
		},
	}
	delivery := &recordingDelivery{}
	seq := &recordingSequenceAllocator{messageID: 99, sequence: 7}
	app := New(Options{
		Now:      fixedNowFn,
		Online:   reg,
		Delivery: delivery,
		Sequence: seq,
	})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   9,
		ClientMsgNo: "m1",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonSuccess, result.Reason)
	require.Equal(t, int64(99), result.MessageID)
	require.Equal(t, uint64(7), result.MessageSeq)
	require.Equal(t, []string{"u2"}, seq.channelKeys)
	require.Len(t, delivery.calls, 1)
	require.Len(t, delivery.calls[0].recipients, 2)

	recv := requireRecvPacket(t, delivery.calls[0].frame)
	require.Equal(t, "u1", recv.FromUID)
	require.Equal(t, "u1", recv.ChannelID)
	require.Equal(t, wkpacket.ChannelTypePerson, recv.ChannelType)
	require.Equal(t, int64(99), recv.MessageID)
	require.Equal(t, uint64(7), recv.MessageSeq)
	require.Equal(t, []byte("hi"), recv.Payload)
	require.Equal(t, uint64(9), recv.ClientSeq)
	require.Equal(t, "m1", recv.ClientMsgNo)
	require.Equal(t, int32(fixedSendNow.Unix()), recv.Timestamp)
}

func TestSendReturnsUserNotOnNodeWhenRecipientOffline(t *testing.T) {
	app := New(Options{
		Now:    fixedNowFn,
		Online: &fakeRegistry{},
	})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		ClientSeq:   11,
		ClientMsgNo: "m2",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonUserNotOnNode, result.Reason)
	require.Zero(t, result.MessageID)
	require.Zero(t, result.MessageSeq)
}

func TestSendReturnsSystemErrorWhenDeliveryFails(t *testing.T) {
	reg := &fakeRegistry{
		byUID: map[string][]online.OnlineConn{
			"u2": {
				{SessionID: 2, UID: "u2"},
				{SessionID: 3, UID: "u2"},
			},
		},
	}
	app := New(Options{
		Now:      fixedNowFn,
		Online:   reg,
		Delivery: failingDelivery{err: errors.New("boom")},
		Sequence: &recordingSequenceAllocator{messageID: 101, sequence: 5},
	})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   14,
		ClientMsgNo: "m5",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonSystemError, result.Reason)
	require.Equal(t, int64(101), result.MessageID)
	require.Equal(t, uint64(5), result.MessageSeq)
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
			{err: channelcluster.ErrStaleMeta},
			{result: channelcluster.SendResult{MessageID: 201, MessageSeq: 7}},
		},
	}
	refresher := &fakeMetaRefresher{
		metas: []channelcluster.ChannelMeta{{
			ChannelID:    "u2",
			ChannelType:  wkpacket.ChannelTypePerson,
			ChannelEpoch: 11,
			LeaderEpoch:  3,
		}},
	}
	app := New(Options{
		Now:           fixedNowFn,
		ClusterPort:   cluster,
		MetaRefresher: refresher,
		Online:        reg,
		Delivery:      delivery,
	})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   21,
		ClientMsgNo: "m6",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonSuccess, result.Reason)
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

func TestSendClusterSuccessDoesNotRequireLocalRecipient(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{result: channelcluster.SendResult{MessageID: 301, MessageSeq: 15}},
		},
	}
	delivery := &recordingDelivery{}
	app := New(Options{
		Now:         fixedNowFn,
		ClusterPort: cluster,
		Online:      &fakeRegistry{},
		Delivery:    delivery,
	})

	result, err := app.Send(SendCommand{
		SenderUID:   "u1",
		ChannelID:   "u2",
		ChannelType: wkpacket.ChannelTypePerson,
		Payload:     []byte("hi"),
		ClientSeq:   22,
		ClientMsgNo: "m7",
	})

	require.NoError(t, err)
	require.Equal(t, wkpacket.ReasonSuccess, result.Reason)
	require.Equal(t, int64(301), result.MessageID)
	require.Equal(t, uint64(15), result.MessageSeq)
	require.Empty(t, delivery.calls)
	require.Len(t, cluster.sendRequests, 1)
}

func TestSendReturnsProtocolUpgradeRequiredWhenClusterRejectsLegacyClient(t *testing.T) {
	cluster := &fakeChannelCluster{
		sendReplies: []fakeChannelClusterSendReply{
			{err: channelcluster.ErrProtocolUpgradeRequired},
		},
	}
	delivery := &recordingDelivery{}
	app := New(Options{
		Now:         fixedNowFn,
		ClusterPort: cluster,
		Online: &fakeRegistry{
			byUID: map[string][]online.OnlineConn{
				"u2": {
					{SessionID: 2, UID: "u2"},
				},
			},
		},
		Delivery: delivery,
	})

	result, err := app.Send(SendCommand{
		SenderUID:       "u1",
		ChannelID:       "u2",
		ChannelType:     wkpacket.ChannelTypePerson,
		Payload:         []byte("hi"),
		ClientSeq:       23,
		ClientMsgNo:     "m8",
		ProtocolVersion: wkpacket.LegacyMessageSeqVersion,
	})

	require.ErrorIs(t, err, channelcluster.ErrProtocolUpgradeRequired)
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
	seq := &recordingSequenceAllocator{}

	app := New(Options{
		IdentityStore: identities,
		ChannelStore:  channels,
		ClusterPort:   cluster,
		MetaRefresher: refresher,
		Online:        reg,
		Delivery:      delivery,
		Sequence:      seq,
		Now:           fixedNowFn,
	})

	require.Same(t, identities, app.identities)
	require.Same(t, channels, app.channels)
	require.Same(t, cluster, app.cluster)
	require.Same(t, refresher, app.refresher)
	require.Same(t, reg, app.online)
	require.Same(t, delivery, app.delivery)
	require.Same(t, seq, app.sequence)
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

type deliveryCall struct {
	recipients []online.OnlineConn
	frame      wkpacket.Frame
}

type recordingDelivery struct {
	calls []deliveryCall
}

func (d *recordingDelivery) Deliver(recipients []online.OnlineConn, frame wkpacket.Frame) error {
	copiedRecipients := make([]online.OnlineConn, len(recipients))
	copy(copiedRecipients, recipients)
	d.calls = append(d.calls, deliveryCall{recipients: copiedRecipients, frame: frame})
	return nil
}

type failingDelivery struct {
	err error
}

func (d failingDelivery) Deliver([]online.OnlineConn, wkpacket.Frame) error {
	return d.err
}

type recordingSequenceAllocator struct {
	messageID   int64
	sequence    uint32
	channelKeys []string
}

func (a *recordingSequenceAllocator) NextMessageID() int64 {
	return a.messageID
}

func (a *recordingSequenceAllocator) NextChannelSequence(channelKey string) uint32 {
	a.channelKeys = append(a.channelKeys, channelKey)
	return a.sequence
}

func requireRecvPacket(t *testing.T, frame wkpacket.Frame) *wkpacket.RecvPacket {
	t.Helper()

	recv, ok := frame.(*wkpacket.RecvPacket)
	require.True(t, ok, "expected *wkpacket.RecvPacket, got %T", frame)
	return recv
}

type fakeIdentityStore struct{}

func (*fakeIdentityStore) GetUser(context.Context, string) (wkdb.User, error) {
	return wkdb.User{}, nil
}

type fakeChannelStore struct{}

func (*fakeChannelStore) GetChannel(context.Context, string, int64) (wkdb.Channel, error) {
	return wkdb.Channel{}, nil
}

type fakeChannelClusterSendReply struct {
	result channelcluster.SendResult
	err    error
}

type fakeChannelCluster struct {
	appliedMetas []channelcluster.ChannelMeta
	sendRequests []channelcluster.SendRequest
	sendReplies  []fakeChannelClusterSendReply
	applyErr     error
}

func (f *fakeChannelCluster) ApplyMeta(meta channelcluster.ChannelMeta) error {
	f.appliedMetas = append(f.appliedMetas, meta)
	return f.applyErr
}

func (f *fakeChannelCluster) Send(_ context.Context, req channelcluster.SendRequest) (channelcluster.SendResult, error) {
	f.sendRequests = append(f.sendRequests, req)
	if len(f.sendReplies) == 0 {
		return channelcluster.SendResult{}, nil
	}
	reply := f.sendReplies[0]
	f.sendReplies = f.sendReplies[1:]
	return reply.result, reply.err
}

type fakeMetaRefresher struct {
	keys  []channelcluster.ChannelKey
	metas []channelcluster.ChannelMeta
	errs  []error
}

func (f *fakeMetaRefresher) RefreshChannelMeta(_ context.Context, key channelcluster.ChannelKey) (channelcluster.ChannelMeta, error) {
	f.keys = append(f.keys, key)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return channelcluster.ChannelMeta{}, err
		}
	}
	if len(f.metas) == 0 {
		return channelcluster.ChannelMeta{}, nil
	}
	meta := f.metas[0]
	f.metas = f.metas[1:]
	return meta, nil
}
