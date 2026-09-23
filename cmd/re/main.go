package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"mini-mesh/internal/reconnect"
	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second

	queueCapacity = 500

	// defaultGCAddrs mirrors CE's RE_ADDRS pattern: a comma-separated,
	// ordered-by-preference list (today just one GC).
	defaultGCAddrs = "localhost:50051"

	// gracefulShutdownTimeout bounds how long we wait for CE's in-flight
	// stream to close on its own before forcing the connection shut. CE only
	// closes when its own context is cancelled, so without this timeout a
	// still-running CE would hang RE's shutdown indefinitely.
	gracefulShutdownTimeout = 5 * time.Second
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	queue := newEventQueue(queueCapacity)
	srv := &reServer{queue: queue}

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)
	mux.HandleFunc("GET /readyz", srv.handleReadyz)
	mux.HandleFunc("GET /debug/queue", srv.handleQueueDebug)
	httpServer := &http.Server{Addr: ":8081", Handler: mux}

	gcAddrs := parseAddrs(envOrDefault("GC_ADDRS", defaultGCAddrs))
	forwardDone := make(chan struct{})
	go func() {
		reconnect.Run(ctx, gcAddrs, &queueSource{queue: queue}, reconnect.Config{
			InitialBackoff: initialBackoff,
			MaxBackoff:     maxBackoff,
		}, slog.Default())
		close(forwardDone)
	}()

	go func() {
		<-ctx.Done()
		slog.Info("shutting down gracefully...")
		stopGRPCServer(grpcServer, gracefulShutdownTimeout)
		httpServer.Shutdown(context.Background())
	}()

	go func() {
		slog.Info("RE HTTP listening", "addr", ":8081")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("RE gRPC listening", "addr", ":50052")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}

	// Let the forwarding goroutine drain any remaining queued events before exiting.
	<-forwardDone
}

// stopGRPCServer attempts a graceful stop, but forces the server down if
// in-flight RPCs (e.g. a CE stream that never half-closes) don't finish in time.
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

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		if p := strings.TrimSpace(p); p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}

// queueSource adapts eventQueue to reconnect.Source.
type queueSource struct {
	queue *eventQueue
}

func (s *queueSource) Next(ctx context.Context) (*telemetryv1.TelemetryEvent, bool) {
	return s.queue.pop(ctx)
}
