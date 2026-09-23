package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// reServer implements TelemetryIngestServer for CE connections, enqueuing
// each received event for a separate goroutine to forward on to GC.
type reServer struct {
	telemetryv1.UnimplementedTelemetryIngestServer

	queue *eventQueue
}

func (s *reServer) StreamTelemetry(stream telemetryv1.TelemetryIngest_StreamTelemetryServer) error {
	var count uint32
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&telemetryv1.StreamTelemetryResponse{
				EventsReceived: count,
			})
		}
		if err != nil {
			return err
		}

		s.queue.push(event)
		count++

		slog.Info("received telemetry event from CE",
			"event_id", event.EventId,
			"node_id", event.NodeId,
			"queue_depth", s.queue.len(),
		)
	}
}

func (s *reServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (s *reServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// RE has no hard dependency to be "ready" — it queues events even while
	// GC is unreachable — so readiness just reflects the process being up.
	w.Write([]byte("ready"))
}

// handleQueueDebug exposes basic forwarding counters ahead of real metrics
// (Phase 6 adds Prometheus); useful now for observing queue behavior by hand.
func (s *reServer) handleQueueDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]uint64{
		"queue_depth":     uint64(s.queue.len()),
		"dropped_total":   s.queue.droppedCount(),
		"forwarded_total": s.queue.forwardedCount(),
	})
}
