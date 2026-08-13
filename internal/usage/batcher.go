package usage

import (
	"sync/atomic"
	"time"
)

const (
	usageBatchQueueCapacity = 4096
	usageBatchMaxRecords    = 256
)

type preparedUsageRecord struct {
	apiName   string
	modelName string
	detail    RequestDetail
}

type usageBatchItem struct {
	record  preparedUsageRecord
	barrier chan struct{}
}

type usageBatcher struct {
	owner *RequestStatistics
	queue chan usageBatchItem

	queuedRecords  atomic.Int64
	processed      atomic.Uint64
	batches        atomic.Uint64
	flushes        atomic.Uint64
	flushWaitNanos atomic.Uint64
}

// UsageBatchRuntimeSnapshot describes the bounded production aggregation queue.
type UsageBatchRuntimeSnapshot struct {
	QueueDepth       int64  `json:"queue_depth"`
	QueueCapacity    int    `json:"queue_capacity"`
	ProcessedRecords uint64 `json:"processed_records"`
	Batches          uint64 `json:"batches"`
	Flushes          uint64 `json:"flushes"`
	FlushWaitNanos   uint64 `json:"flush_wait_nanos"`
}

func newUsageBatcher(owner *RequestStatistics) *usageBatcher {
	batcher := &usageBatcher{
		owner: owner,
		queue: make(chan usageBatchItem, usageBatchQueueCapacity),
	}
	go batcher.run()
	return batcher
}

func (b *usageBatcher) enqueue(record preparedUsageRecord) {
	if b == nil {
		return
	}
	b.queuedRecords.Add(1)
	b.queue <- usageBatchItem{record: record}
}

func (b *usageBatcher) flush() {
	if b == nil {
		return
	}
	startedAt := time.Now()
	done := make(chan struct{})
	b.queue <- usageBatchItem{barrier: done}
	<-done
	b.flushes.Add(1)
	b.flushWaitNanos.Add(uint64(time.Since(startedAt)))
}

func (b *usageBatcher) snapshot() UsageBatchRuntimeSnapshot {
	if b == nil {
		return UsageBatchRuntimeSnapshot{}
	}
	return UsageBatchRuntimeSnapshot{
		QueueDepth:       b.queuedRecords.Load(),
		QueueCapacity:    cap(b.queue),
		ProcessedRecords: b.processed.Load(),
		Batches:          b.batches.Load(),
		Flushes:          b.flushes.Load(),
		FlushWaitNanos:   b.flushWaitNanos.Load(),
	}
}

func (b *usageBatcher) run() {
	batch := make([]preparedUsageRecord, 0, usageBatchMaxRecords)
	for {
		item := <-b.queue
		if item.barrier != nil {
			close(item.barrier)
			continue
		}
		batch = append(batch[:0], item.record)
		var barrier chan struct{}
	collect:
		for len(batch) < usageBatchMaxRecords {
			select {
			case next := <-b.queue:
				if next.barrier != nil {
					barrier = next.barrier
					break collect
				}
				batch = append(batch, next.record)
			default:
				break collect
			}
		}
		b.owner.recordPreparedBatch(batch)
		b.queuedRecords.Add(-int64(len(batch)))
		b.processed.Add(uint64(len(batch)))
		b.batches.Add(1)
		if barrier != nil {
			close(barrier)
		}
	}
}

func (s *RequestStatistics) flushPending() {
	if s != nil && s.batch != nil {
		s.batch.flush()
	}
}

// BatchRuntimeSnapshot returns bounded queue metrics for the shared store.
func BatchRuntimeSnapshot() UsageBatchRuntimeSnapshot {
	if defaultRequestStatistics == nil || defaultRequestStatistics.batch == nil {
		return UsageBatchRuntimeSnapshot{}
	}
	return defaultRequestStatistics.batch.snapshot()
}
