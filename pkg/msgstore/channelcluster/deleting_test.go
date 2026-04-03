package channelcluster

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/isr"
)

func TestDeletingFencesNewSendAndFetchRequests(t *testing.T) {
	env := newDeletingEnv(t)
	env.applyDeletingMeta(t)

	_, err := env.cluster.Send(context.Background(), testSendRequest())
	if !errors.Is(err, ErrChannelDeleting) {
		t.Fatalf("expected ErrChannelDeleting from Send, got %v", err)
	}

	_, err = env.cluster.Fetch(context.Background(), FetchRequest{
		Key: env.key, FromSeq: 1, Limit: 1, MaxBytes: 128,
	})
	if !errors.Is(err, ErrChannelDeleting) {
		t.Fatalf("expected ErrChannelDeleting from Fetch, got %v", err)
	}
}

func TestInFlightSendReturnsDeletingWhenFenceWinsBeforeCommit(t *testing.T) {
	env := newDeletingEnv(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	env.group.appendFn = func(records []isr.Record) (isr.CommitResult, error) {
		started <- struct{}{}
		<-release

		base := uint64(len(env.log.records))
		for _, record := range records {
			env.log.records = append(env.log.records, LogRecord{
				Offset:  uint64(len(env.log.records)),
				Payload: append([]byte(nil), record.Payload...),
			})
		}
		env.group.state.HW = uint64(len(env.log.records))
		return isr.CommitResult{
			BaseOffset:   base,
			NextCommitHW: env.group.state.HW,
			RecordCount:  len(records),
		}, nil
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := env.cluster.Send(context.Background(), testSendRequest())
		errCh <- err
	}()

	<-started
	env.applyDeletingMeta(t)
	close(release)

	if err := <-errCh; !errors.Is(err, ErrChannelDeleting) {
		t.Fatalf("expected ErrChannelDeleting, got %v", err)
	}
}

type deletingEnv struct {
	cluster *cluster
	group   *fakeGroupHandle
	log     *fakeMessageLog
	key     ChannelKey
	meta    ChannelMeta
}

func newDeletingEnv(t *testing.T) *deletingEnv {
	t.Helper()

	sendEnv := newSendEnv(t)
	return &deletingEnv{
		cluster: sendEnv.cluster,
		group:   sendEnv.group,
		log:     sendEnv.log,
		key: ChannelKey{
			ChannelID:   sendEnv.meta.ChannelID,
			ChannelType: sendEnv.meta.ChannelType,
		},
		meta: sendEnv.meta,
	}
}

func (e *deletingEnv) applyDeletingMeta(t *testing.T) {
	t.Helper()

	meta := e.meta
	meta.ChannelEpoch++
	meta.Status = ChannelStatusDeleting
	if err := e.cluster.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	e.meta = meta
}
