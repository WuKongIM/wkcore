package log

import (
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/cockroachdb/pebble/v2"
)

const (
	defaultCommitCoordinatorFlushWindow = 200 * time.Microsecond
	defaultCommitCoordinatorQueueSize   = 1024
)

type commitRequest struct {
	channelKey isr.ChannelKey
	build      func(*pebble.Batch) error
	publish    func() error
	done       chan error
}

type commitCoordinator struct {
	db          *pebble.DB
	flushWindow time.Duration
	commit      func(*pebble.Batch) error

	requests  chan commitRequest
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

func newCommitCoordinator(db *pebble.DB) *commitCoordinator {
	c := &commitCoordinator{
		db:          db,
		flushWindow: defaultCommitCoordinatorFlushWindow,
		commit: func(batch *pebble.Batch) error {
			return batch.Commit(pebble.Sync)
		},
		requests: make(chan commitRequest, defaultCommitCoordinatorQueueSize),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	go c.run()
	return c
}

func (c *commitCoordinator) submit(req commitRequest) error {
	if c == nil || c.db == nil {
		return ErrInvalidArgument
	}
	if req.build == nil {
		return ErrInvalidArgument
	}
	if req.done == nil {
		req.done = make(chan error, 1)
	}

	select {
	case <-c.doneCh:
		return ErrInvalidArgument
	case c.requests <- req:
	}

	select {
	case err := <-req.done:
		return err
	case <-c.doneCh:
		return ErrInvalidArgument
	}
}

func (c *commitCoordinator) close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.stopCh)
		<-c.doneCh
	})
}

func (c *commitCoordinator) run() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.requests:
			batch := c.collectBatch(req)
			batch.commit(c.db, c.commit)
		}
	}
}

func (c *commitCoordinator) collectBatch(first commitRequest) commitBatch {
	batch := commitBatch{requests: []commitRequest{first}}
	if c.flushWindow <= 0 {
		for {
			select {
			case req := <-c.requests:
				batch.requests = append(batch.requests, req)
			default:
				return batch
			}
		}
	}

	timer := time.NewTimer(c.flushWindow)
	defer timer.Stop()

	for {
		select {
		case req := <-c.requests:
			batch.requests = append(batch.requests, req)
		case <-timer.C:
			return batch
		case <-c.stopCh:
			return batch
		}
	}
}
