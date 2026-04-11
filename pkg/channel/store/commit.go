package store

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/cockroachdb/pebble/v2"
)

const (
	defaultCommitCoordinatorFlushWindow = 200 * time.Microsecond
	defaultCommitCoordinatorQueueSize   = 1024
)

type commitRequest struct {
	channelKey channel.ChannelKey
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
		return channel.ErrInvalidArgument
	}
	if req.build == nil {
		return channel.ErrInvalidArgument
	}
	if req.done == nil {
		req.done = make(chan error, 1)
	}

	select {
	case <-c.doneCh:
		return channel.ErrInvalidArgument
	case c.requests <- req:
	}

	select {
	case err := <-req.done:
		return err
	case <-c.doneCh:
		return channel.ErrInvalidArgument
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

type commitBatch struct {
	requests []commitRequest
}

func (b commitBatch) commit(db *pebble.DB, commit func(*pebble.Batch) error) {
	if db == nil || commit == nil {
		b.completeAll(channel.ErrInvalidArgument)
		return
	}

	writeBatch := db.NewBatch()
	defer writeBatch.Close()

	for _, req := range b.requests {
		if req.build == nil {
			b.completeAll(channel.ErrInvalidArgument)
			return
		}
		if err := req.build(writeBatch); err != nil {
			b.completeAll(err)
			return
		}
	}
	if err := commit(writeBatch); err != nil {
		b.completeAll(err)
		return
	}

	for _, req := range b.requests {
		var err error
		if req.publish != nil {
			err = req.publish()
		}
		req.done <- err
	}
}

func (b commitBatch) completeAll(err error) {
	for _, req := range b.requests {
		req.done <- err
	}
}

func (s *ChannelStore) StoreApplyFetch(req channel.ApplyFetchStoreRequest) (uint64, error) {
	base, err := s.leoWithError()
	if err != nil {
		return 0, err
	}
	committed, err := s.readCommittedBatchForApplyFetch(base, req)
	if err != nil {
		return 0, err
	}
	return s.applyFetchedRecords(req.Records, committed, req.Checkpoint)
}

func (s *ChannelStore) applyFetchedRecords(records []channel.Record, committed []appliedMessage, checkpoint *channel.Checkpoint) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	base, err := s.leoLocked()
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if len(records) == 0 && checkpoint == nil {
		s.mu.Unlock()
		return base, nil
	}
	s.writeInProgress.Store(true)
	s.mu.Unlock()
	defer s.writeInProgress.Store(false)

	nextLEO := base + uint64(len(records))
	if coordinator := s.commitCoordinator(); coordinator != nil {
		err := coordinator.submit(commitRequest{
			channelKey: s.key,
			build: func(writeBatch *pebble.Batch) error {
				return s.writeApplyFetchedRecords(writeBatch, base, records, committed, checkpoint)
			},
			publish: func() error {
				s.recordDurableCommit()
				s.mu.Lock()
				s.leo.Store(nextLEO)
				s.loaded.Store(true)
				s.mu.Unlock()
				return nil
			},
		})
		if err != nil {
			return 0, err
		}
		return nextLEO, nil
	}

	batch := s.engine.db.NewBatch()
	defer batch.Close()

	if err := s.writeApplyFetchedRecords(batch, base, records, committed, checkpoint); err != nil {
		return 0, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	s.recordDurableCommit()
	s.mu.Lock()
	s.leo.Store(nextLEO)
	s.loaded.Store(true)
	s.mu.Unlock()
	return nextLEO, nil
}

func (s *ChannelStore) writeApplyFetchedRecords(writeBatch *pebble.Batch, base uint64, records []channel.Record, committed []appliedMessage, checkpoint *channel.Checkpoint) error {
	for i, record := range records {
		key := encodeLogRecordKey(s.key, base+uint64(i))
		value := append([]byte(nil), record.Payload...)
		if err := writeBatch.Set(key, value, pebble.NoSync); err != nil {
			return err
		}
	}
	for _, msg := range committed {
		if err := writeBatch.Set(encodeIdempotencyKey(s.key, msg.key), encodeIdempotencyEntry(msg.entry), pebble.NoSync); err != nil {
			return err
		}
	}
	if checkpoint != nil {
		if err := s.writeCheckpoint(writeBatch, *checkpoint); err != nil {
			return err
		}
	}
	return nil
}

func (s *ChannelStore) readCommittedBatchForApplyFetch(base uint64, req channel.ApplyFetchStoreRequest) ([]appliedMessage, error) {
	prevHW, err := s.loadCheckpointHW()
	if err != nil {
		return nil, err
	}
	if req.Checkpoint == nil || req.Checkpoint.HW <= prevHW {
		return nil, nil
	}

	nextHW := req.Checkpoint.HW
	batch := make([]appliedMessage, 0, int(nextHW-prevHW))

	existingUpper := minUint64(nextHW, base)
	if existingUpper > prevHW {
		records, err := s.readOffsets(prevHW, spanLimit(existingUpper-prevHW), maxLogScanLimit())
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			msg, ok, err := appliedMessageFromLogRecord(s.id, record)
			if err != nil {
				return nil, err
			}
			if ok {
				batch = append(batch, msg)
			}
		}
	}

	newUpper := minUint64(nextHW, base+uint64(len(req.Records)))
	start := maxUint64(prevHW, base)
	for offset := start; offset < newUpper; offset++ {
		record := LogRecord{
			Offset:  offset,
			Payload: req.Records[offset-base].Payload,
		}
		msg, ok, err := appliedMessageFromLogRecord(s.id, record)
		if err != nil {
			return nil, err
		}
		if ok {
			batch = append(batch, msg)
		}
	}
	return batch, nil
}

func (s *ChannelStore) loadCheckpointHW() (uint64, error) {
	checkpoint, err := s.LoadCheckpoint()
	if errors.Is(err, channel.ErrEmptyState) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint.HW, nil
}

func appliedMessageFromLogRecord(channelID channel.ChannelID, record LogRecord) (appliedMessage, bool, error) {
	messageID, fromUID, clientMsgNo, err := decodeIdempotencyFields(record.Payload)
	if err != nil {
		return appliedMessage{}, false, err
	}
	if clientMsgNo == "" {
		return appliedMessage{}, false, nil
	}
	return appliedMessage{
		key: channel.IdempotencyKey{
			ChannelID:   channelID,
			FromUID:     fromUID,
			ClientMsgNo: clientMsgNo,
		},
		entry: channel.IdempotencyEntry{
			MessageID:  messageID,
			MessageSeq: record.Offset + 1,
			Offset:     record.Offset,
		},
	}, true, nil
}

func decodeIdempotencyFields(payload []byte) (uint64, string, string, error) {
	const (
		messageCodecVersion byte = 1
		messageHeaderSize        = 45
	)

	if len(payload) < messageHeaderSize {
		return 0, "", "", io.ErrUnexpectedEOF
	}
	if payload[0] != messageCodecVersion {
		return 0, "", "", channel.ErrCorruptValue
	}

	messageID := binary.BigEndian.Uint64(payload[1:9])
	pos := messageHeaderSize

	_, pos, err := readSizedBytesView(payload, pos) // msgKey
	if err != nil {
		return 0, "", "", err
	}
	clientMsgNo, pos, err := readSizedBytesView(payload, pos)
	if err != nil {
		return 0, "", "", err
	}
	_, pos, err = readSizedBytesView(payload, pos) // streamNo
	if err != nil {
		return 0, "", "", err
	}
	_, pos, err = readSizedBytesView(payload, pos) // channelID
	if err != nil {
		return 0, "", "", err
	}
	_, pos, err = readSizedBytesView(payload, pos) // topic
	if err != nil {
		return 0, "", "", err
	}
	fromUID, _, err := readSizedBytesView(payload, pos)
	if err != nil {
		return 0, "", "", err
	}
	return messageID, string(fromUID), string(clientMsgNo), nil
}

func readSizedBytesView(payload []byte, pos int) ([]byte, int, error) {
	if len(payload)-pos < 4 {
		return nil, pos, io.ErrUnexpectedEOF
	}
	size := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if len(payload)-pos < size {
		return nil, pos, io.ErrUnexpectedEOF
	}
	return payload[pos : pos+size], pos + size, nil
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func spanLimit(span uint64) int {
	if span > uint64(maxLogScanLimit()) {
		return maxLogScanLimit()
	}
	return int(span)
}
