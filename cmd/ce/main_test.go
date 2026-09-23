package main

import (
	"testing"
)

func TestCollectEvent(t *testing.T) {
	event, err := collectEvent("test-node")
	if err != nil {
		t.Fatalf("collectEvent returned error: %v", err)
	}

	if event.NodeId != "test-node" {
		t.Errorf("expected NodeId %q, got %q", "test-node", event.NodeId)
	}
	if event.EventId == "" {
		t.Error("expected non-empty EventId")
	}
	if event.CollectedAt == nil {
		t.Error("expected CollectedAt to be set")
	}
	if event.Metrics == nil {
		t.Fatal("expected Metrics to be set")
	}
	if event.Metrics.MemoryTotalBytes == 0 {
		t.Error("expected non-zero MemoryTotalBytes")
	}
}
