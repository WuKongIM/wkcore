package replica

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

type pooledRecordBuffer struct {
	records []channel.Record
}

var appendRequestPool = sync.Pool{
	New: func() any {
		return &appendRequest{}
	},
}

var appendWaiterPool = sync.Pool{
	New: func() any {
		return &appendWaiter{ch: make(chan appendCompletion, 1)}
	},
}

var mergedRecordBufferPool = sync.Pool{
	New: func() any {
		return &pooledRecordBuffer{records: make([]channel.Record, 0, 64)}
	},
}

func acquireAppendRequest() *appendRequest {
	return appendRequestPool.Get().(*appendRequest)
}

func releaseAppendRequest(req *appendRequest) {
	if req == nil {
		return
	}
	req.ctx = nil
	req.batch = nil
	req.byteCount = 0
	req.waiter = nil
	appendRequestPool.Put(req)
}

func acquireAppendWaiter() *appendWaiter {
	waiter := appendWaiterPool.Get().(*appendWaiter)
	waiter.target = 0
	waiter.result = channel.CommitResult{}
	for {
		select {
		case <-waiter.ch:
		default:
			return waiter
		}
	}
}

func releaseAppendWaiter(waiter *appendWaiter) {
	if waiter == nil {
		return
	}
	waiter.target = 0
	waiter.result = channel.CommitResult{}
	appendWaiterPool.Put(waiter)
}

func acquireMergedRecordBuffer() *pooledRecordBuffer {
	buf := mergedRecordBufferPool.Get().(*pooledRecordBuffer)
	buf.records = buf.records[:0]
	return buf
}

func releaseMergedRecordBuffer(buf *pooledRecordBuffer) {
	if buf == nil {
		return
	}
	if cap(buf.records) > 4096 {
		buf.records = make([]channel.Record, 0, 64)
	} else {
		buf.records = buf.records[:0]
	}
	mergedRecordBufferPool.Put(buf)
}
