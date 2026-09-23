package main

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// eventQueue is a bounded FIFO queue of events awaiting forwarding to GC.
// It decouples "CE's send succeeded" from "GC has received it yet" so RE can
// keep accepting from CE during short GC outages. When full, the oldest
// queued event is dropped to make room — telemetry favors freshness over
// history, unlike a transaction log. Drops are counted, never silent.
//
// On shutdown, pop drains any remaining items before honoring ctx
// cancellation, so a graceful stop flushes the queue instead of discarding it.
type eventQueue struct {
	mu       sync.Mutex
	items    []*telemetryv1.TelemetryEvent
	capacity int
	notify   chan struct{}

	dropped   atomic.Uint64
	forwarded atomic.Uint64
}

func newEventQueue(capacity int) *eventQueue {
	return &eventQueue{
		capacity: capacity,
		notify:   make(chan struct{}, 1),
	}
}

// push enqueues event, dropping the oldest queued event first if full.
func (q *eventQueue) push(event *telemetryv1.TelemetryEvent) {
	q.mu.Lock()
	if len(q.items) >= q.capacity {
		dropped := q.items[0]
		q.items = q.items[1:]
		q.dropped.Add(1)
		slog.Warn("RE queue full; dropping oldest event",
			"event_id", dropped.EventId, "node_id", dropped.NodeId)
	}
	q.items = append(q.items, event)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// pop blocks until an event is available. It only reports ctx cancellation
// once the queue is fully drained, so shutdown flushes remaining events.
func (q *eventQueue) pop(ctx context.Context) (*telemetryv1.TelemetryEvent, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			event := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			q.forwarded.Add(1)
			return event, true
		}
		q.mu.Unlock()

		if ctx.Err() != nil {
			return nil, false
		}

		select {
		case <-q.notify:
		case <-ctx.Done():
		}
	}
}

func (q *eventQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *eventQueue) droppedCount() uint64 {
	return q.dropped.Load()
}

func (q *eventQueue) forwardedCount() uint64 {
	return q.forwarded.Load()
}
