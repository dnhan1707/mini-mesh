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
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// newTestServer starts a real gcServer on an in-memory (bufconn) listener and
// returns a connected client, the server (for state assertions), and cleanup.
func newTestServer(t *testing.T) (telemetryv1.TelemetryIngestClient, *gcServer, func()) {
	t.Helper()

	srv := &gcServer{latest: make(map[string]*telemetryv1.TelemetryEvent)}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)
	go grpcServer.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	return telemetryv1.NewTelemetryIngestClient(conn), srv, func() {
		conn.Close()
		grpcServer.Stop()
	}
}

func TestStreamTelemetry_StoresLatestEventPerNode(t *testing.T) {
	client, srv, cleanup := newTestServer(t)
	defer cleanup()

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	events := []*telemetryv1.TelemetryEvent{
		{EventId: "1", NodeId: "node-a", CollectedAt: timestamppb.Now(), Metrics: &telemetryv1.SystemMetrics{CpuPercent: 10}},
		{EventId: "2", NodeId: "node-a", CollectedAt: timestamppb.Now(), Metrics: &telemetryv1.SystemMetrics{CpuPercent: 20}},
		{EventId: "3", NodeId: "node-b", CollectedAt: timestamppb.Now(), Metrics: &telemetryv1.SystemMetrics{CpuPercent: 30}},
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

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if got := srv.latest["node-a"].EventId; got != "2" {
		t.Errorf("expected node-a's latest event to be %q, got %q", "2", got)
	}
	if got := srv.latest["node-b"].EventId; got != "3" {
		t.Errorf("expected node-b's latest event to be %q, got %q", "3", got)
	}
}

func TestHandleHealthzAndReadyz(t *testing.T) {
	srv := &gcServer{latest: make(map[string]*telemetryv1.TelemetryEvent)}

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

func TestHandleNodes(t *testing.T) {
	srv := &gcServer{latest: map[string]*telemetryv1.TelemetryEvent{
		"node-a": {EventId: "1", NodeId: "node-a", Metrics: &telemetryv1.SystemMetrics{CpuPercent: 42}},
	}}

	w := httptest.NewRecorder()
	srv.handleNodes(w, httptest.NewRequest(http.MethodGet, "/nodes", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"nodeId":"node-a"`) {
		t.Errorf("expected response to contain node-a, got %s", w.Body.String())
	}
}

func TestHandleNode(t *testing.T) {
	srv := &gcServer{latest: map[string]*telemetryv1.TelemetryEvent{
		"node-a": {EventId: "1", NodeId: "node-a", Metrics: &telemetryv1.SystemMetrics{CpuPercent: 42}},
	}}

	t.Run("found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nodes/node-a", nil)
		req.SetPathValue("id", "node-a")
		w := httptest.NewRecorder()

		srv.handleNode(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nodes/missing", nil)
		req.SetPathValue("id", "missing")
		w := httptest.NewRecorder()

		srv.handleNode(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", w.Code)
		}
	})
}
