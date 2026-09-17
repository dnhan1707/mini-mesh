package main

import (
	"testing"
	"time"
)

func TestCollectMetrics(t *testing.T) {
	expectedNodeID := "test-ce-node"

	// 1. Execute the function we want to test
	payload, err := collectMetrics(expectedNodeID)

	// 2. Validate no errors occurred
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// 3. Validate the payload exists
	if payload == nil {
		t.Fatal("Expected payload to not be nil")
	}

	// 4. Validate the static data
	if payload.NodeId != expectedNodeID {
		t.Errorf("Expected NodeId '%s', got '%s'", expectedNodeID, payload.NodeId)
	}

	// 5. Validate the timestamp is recent (within the last 2 seconds)
	now := time.Now().Unix()
	if payload.Timestamp > now || payload.Timestamp < now-2 {
		t.Errorf("Timestamp %d is outside expected range", payload.Timestamp)
	}

	// 6. Validate hardware metrics are within logical boundaries
	if payload.CpuPercent < 0 || payload.CpuPercent > 100 {
		t.Errorf("CPU percentage %.2f is outside valid bounds (0-100)", payload.CpuPercent)
	}

	if payload.MemPercent < 0 || payload.MemPercent > 100 {
		t.Errorf("Memory percentage %.2f is outside valid bounds (0-100)", payload.MemPercent)
	}

	if payload.TotalMemMb == 0 {
		t.Error("Total memory should be greater than 0")
	}
}
