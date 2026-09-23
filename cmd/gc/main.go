package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// gracefulShutdownTimeout bounds how long we wait for RE's in-flight stream
// to close on its own before forcing the connection shut. RE only closes
// when its own context is cancelled, so without this timeout a still-running
// RE would hang GC's shutdown indefinitely.
const gracefulShutdownTimeout = 5 * time.Second

type gcServer struct {
	telemetryv1.UnimplementedTelemetryIngestServer

	mu     sync.Mutex
	latest map[string]*telemetryv1.TelemetryEvent
}

func (s *gcServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (s *gcServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// GC has no external dependencies yet, so "ready" == "process is up".
	w.Write([]byte("ready"))
}

func (s *gcServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	snapshot := make(map[string]*telemetryv1.TelemetryEvent, len(s.latest))
	for id, event := range s.latest {
		snapshot[id] = event
	}
	s.mu.Unlock()

	out := make(map[string]json.RawMessage, len(snapshot))
	for id, event := range snapshot {
		b, err := protojson.Marshal(event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out[id] = b
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *gcServer) handleNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")

	s.mu.Lock()
	event, ok := s.latest[nodeID]
	s.mu.Unlock()

	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	b, err := protojson.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func (s *gcServer) StreamTelemetry(stream telemetryv1.TelemetryIngest_StreamTelemetryServer) error {
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

		s.mu.Lock()
		s.latest[event.NodeId] = event
		s.mu.Unlock()
		count++

		slog.Info("received telemetry event",
			"event_id", event.EventId,
			"node_id", event.NodeId,
			"cpu_percent", event.Metrics.CpuPercent,
		)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	srv := &gcServer{latest: make(map[string]*telemetryv1.TelemetryEvent)}
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)
	mux.HandleFunc("GET /readyz", srv.handleReadyz)
	mux.HandleFunc("GET /nodes", srv.handleNodes)
	mux.HandleFunc("GET /nodes/{id}", srv.handleNode)
	httpServer := &http.Server{Addr: ":8080", Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		slog.Info("shutting down gracefully...")
		stopGRPCServer(grpcServer, gracefulShutdownTimeout)
		httpServer.Shutdown(context.Background())
	}()

	go func() {
		slog.Info("GC HTTP listening", "addr", ":8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("GC gRPC listening", "addr", ":50051")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// stopGRPCServer attempts a graceful stop, but forces the server down if
// in-flight RPCs (e.g. a RE stream that never half-closes) don't finish in time.
func stopGRPCServer(grpcServer *grpc.Server, timeout time.Duration) {
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(timeout):
		slog.Warn("graceful stop timed out waiting for in-flight streams; forcing stop")
		grpcServer.Stop()
	}
}
