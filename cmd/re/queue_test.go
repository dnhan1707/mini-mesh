package main

import (
	"context"
	"testing"
	"time"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

func TestEventQueue_PushPopOrder(t *testing.T) {
	q := newEventQueue(10)
	q.push(&telemetryv1.TelemetryEvent{EventId: "1"})
	q.push(&telemetryv1.TelemetryEvent{EventId: "2"})

	ctx := context.Background()

	event, ok := q.pop(ctx)
	if !ok || event.EventId != "1" {
		t.Fatalf("expected event 1 first, got %+v (ok=%v)", event, ok)
	}

	event, ok = q.pop(ctx)
	if !ok || event.EventId != "2" {
		t.Fatalf("expected event 2 second, got %+v (ok=%v)", event, ok)
	}
}

func TestEventQueue_DropsOldestWhenFull(t *testing.T) {
	q := newEventQueue(2)
	q.push(&telemetryv1.TelemetryEvent{EventId: "1"})
	q.push(&telemetryv1.TelemetryEvent{EventId: "2"})
	q.push(&telemetryv1.TelemetryEvent{EventId: "3"}) // should drop "1"

	if got := q.droppedCount(); got != 1 {
		t.Errorf("expected 1 dropped event, got %d", got)
	}

	ctx := context.Background()
	event, ok := q.pop(ctx)
	if !ok || event.EventId != "2" {
		t.Fatalf("expected oldest surviving event to be 2, got %+v (ok=%v)", event, ok)
	}
}

func TestEventQueue_PopBlocksUntilPush(t *testing.T) {
	q := newEventQueue(10)
	ctx := context.Background()

	done := make(chan *telemetryv1.TelemetryEvent, 1)
	go func() {
		event, _ := q.pop(ctx)
		done <- event
	}()

	time.Sleep(20 * time.Millisecond) // ensure pop is blocked waiting
	q.push(&telemetryv1.TelemetryEvent{EventId: "delayed"})

	select {
	case event := <-done:
		if event.EventId != "delayed" {
			t.Errorf("expected delayed event, got %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not return after push")
	}
}

func TestEventQueue_DrainsBeforeHonoringCancellation(t *testing.T) {
	q := newEventQueue(10)
	q.push(&telemetryv1.TelemetryEvent{EventId: "1"})
	q.push(&telemetryv1.TelemetryEvent{EventId: "2"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before draining starts

	event, ok := q.pop(ctx)
	if !ok || event.EventId != "1" {
		t.Fatalf("expected queued event to still be returned despite cancelled ctx, got %+v (ok=%v)", event, ok)
	}

	event, ok = q.pop(ctx)
	if !ok || event.EventId != "2" {
		t.Fatalf("expected second queued event, got %+v (ok=%v)", event, ok)
	}

	_, ok = q.pop(ctx)
	if ok {
		t.Fatal("expected pop to report done once queue is empty and ctx is cancelled")
	}
}

func TestEventQueue_ForwardedCount(t *testing.T) {
	q := newEventQueue(10)
	q.push(&telemetryv1.TelemetryEvent{EventId: "1"})
	q.push(&telemetryv1.TelemetryEvent{EventId: "2"})

	ctx := context.Background()
	q.pop(ctx)
	q.pop(ctx)

	if got := q.forwardedCount(); got != 2 {
		t.Errorf("expected forwardedCount 2, got %d", got)
	}
}
