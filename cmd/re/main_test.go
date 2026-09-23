package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// newTestServer starts a real reServer on a loopback TCP port and returns a
// connected client, the server (for state assertions), and a cleanup func.
func newTestServer(t *testing.T) (telemetryv1.TelemetryIngestClient, *reServer, func()) {
	t.Helper()

	srv := &reServer{queue: newEventQueue(10)}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)
	go grpcServer.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	return telemetryv1.NewTelemetryIngestClient(conn), srv, func() {
		conn.Close()
		grpcServer.Stop()
	}
}

func TestStreamTelemetry_EnqueuesReceivedEvents(t *testing.T) {
	client, srv, cleanup := newTestServer(t)
	defer cleanup()

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	events := []*telemetryv1.TelemetryEvent{
		{EventId: "1", NodeId: "node-a"},
		{EventId: "2", NodeId: "node-a"},
	}
	for _, event := range events {
		if err := stream.Send(event); err != nil {
			t.Fatalf("failed to send event: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("failed to close stream: %v", err)
	}
	if resp.EventsReceived != uint32(len(events)) {
		t.Errorf("expected %d events received, got %d", len(events), resp.EventsReceived)
	}

	if got := srv.queue.len(); got != len(events) {
		t.Errorf("expected %d events queued, got %d", len(events), got)
	}

	first, ok := srv.queue.pop(context.Background())
	if !ok || first.EventId != "1" {
		t.Errorf("expected first queued event to be %q, got %+v (ok=%v)", "1", first, ok)
	}
}

func TestStreamTelemetry_DropsOldestWhenQueueFull(t *testing.T) {
	srv := &reServer{queue: newEventQueue(1)}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := telemetryv1.NewTelemetryIngestClient(conn)
	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	for _, id := range []string{"1", "2"} {
		if err := stream.Send(&telemetryv1.TelemetryEvent{EventId: id}); err != nil {
			t.Fatalf("failed to send event %s: %v", id, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("failed to close stream: %v", err)
	}

	if got := srv.queue.droppedCount(); got != 1 {
		t.Errorf("expected 1 dropped event, got %d", got)
	}

	event, ok := srv.queue.pop(context.Background())
	if !ok || event.EventId != "2" {
		t.Errorf("expected surviving event to be %q, got %+v (ok=%v)", "2", event, ok)
	}
}

func TestHandleHealthzAndReadyz(t *testing.T) {
	srv := &reServer{queue: newEventQueue(10)}

	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"healthz", srv.handleHealthz},
		{"readyz", srv.handleReadyz},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.handler(w, httptest.NewRequest(http.MethodGet, "/"+tc.name, nil))

			if w.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d", w.Code)
			}
		})
	}
}

func TestHandleQueueDebug(t *testing.T) {
	srv := &reServer{queue: newEventQueue(10)}
	srv.queue.push(&telemetryv1.TelemetryEvent{EventId: "1"})

	w := httptest.NewRecorder()
	srv.handleQueueDebug(w, httptest.NewRequest(http.MethodGet, "/debug/queue", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"queue_depth":1`) {
		t.Errorf("expected response to report queue_depth 1, got %s", w.Body.String())
	}
}
