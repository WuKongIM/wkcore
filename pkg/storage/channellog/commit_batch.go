package channellog

import "github.com/cockroachdb/pebble/v2"

type commitBatch struct {
	requests []commitRequest
}

func (b commitBatch) commit(db *pebble.DB, commit func(*pebble.Batch) error) {
	if db == nil {
		b.completeAll(ErrInvalidArgument)
		return
	}
	if commit == nil {
		b.completeAll(ErrInvalidArgument)
		return
	}

	writeBatch := db.NewBatch()
	defer writeBatch.Close()

	for _, req := range b.requests {
		if req.build == nil {
			b.completeAll(ErrInvalidArgument)
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
