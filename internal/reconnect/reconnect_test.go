package reconnect

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// countingServer records how many telemetry events it receives.
type countingServer struct {
	telemetryv1.UnimplementedTelemetryIngestServer

	mu       sync.Mutex
	received int
}

func (s *countingServer) StreamTelemetry(stream telemetryv1.TelemetryIngest_StreamTelemetryServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&telemetryv1.StreamTelemetryResponse{})
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.received++
		s.mu.Unlock()
	}
}

func (s *countingServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// alwaysFailServer rejects every stream attempt, simulating a fully
// unreachable/down upstream.
type alwaysFailServer struct {
	telemetryv1.UnimplementedTelemetryIngestServer
}

func (alwaysFailServer) StreamTelemetry(telemetryv1.TelemetryIngest_StreamTelemetryServer) error {
	return status.Error(codes.Unavailable, "simulated failure")
}

// flakyServer fails the first stream attempt, then behaves like countingServer.
type flakyServer struct {
	telemetryv1.UnimplementedTelemetryIngestServer

	mu       sync.Mutex
	attempts int
	received int
}

func (s *flakyServer) StreamTelemetry(stream telemetryv1.TelemetryIngest_StreamTelemetryServer) error {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()

	if attempt == 1 {
		return status.Error(codes.Unavailable, "simulated failure")
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&telemetryv1.StreamTelemetryResponse{})
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.received++
		s.mu.Unlock()
	}
}

func (s *flakyServer) snapshot() (attempts, received int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts, s.received
}

// tickerSource emits a synthetic event on every tick, for use as a test Source.
type tickerSource struct {
	ticker *time.Ticker
}

func (s *tickerSource) Next(ctx context.Context) (*telemetryv1.TelemetryEvent, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case <-s.ticker.C:
		return &telemetryv1.TelemetryEvent{EventId: "test-event", NodeId: "test-node"}, true
	}
}

// startServer starts srv on a real loopback TCP port and returns its address
// plus a cleanup func.
func startServer(t *testing.T, srv telemetryv1.TelemetryIngestServer) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	telemetryv1.RegisterTelemetryIngestServer(grpcServer, srv)
	go grpcServer.Serve(lis)

	return lis.Addr().String(), grpcServer.Stop
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNextBackoff(t *testing.T) {
	cases := []struct {
		name    string
		current time.Duration
		limit   time.Duration
		want    time.Duration
	}{
		{"doubles under limit", time.Second, 30 * time.Second, 2 * time.Second},
		{"caps at limit", 20 * time.Second, 30 * time.Second, 30 * time.Second},
		{"caps exactly at limit boundary", 15 * time.Second, 30 * time.Second, 30 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextBackoff(tc.current, tc.limit); got != tc.want {
				t.Errorf("nextBackoff(%v, %v) = %v, want %v", tc.current, tc.limit, got, tc.want)
			}
		})
	}
}

func TestRun_SendsEventsUntilCancelled(t *testing.T) {
	srv := &countingServer{}
	addr, stop := startServer(t, srv)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	src := &tickerSource{ticker: time.NewTicker(5 * time.Millisecond)}
	defer src.ticker.Stop()

	done := make(chan struct{})
	go func() {
		Run(ctx, []string{addr}, src, Config{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, testLogger())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond) // allow several ticks to fire
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if srv.count() == 0 {
		t.Error("expected server to receive at least one event")
	}
}

func TestRun_RetriesAfterFailure(t *testing.T) {
	srv := &flakyServer{}
	addr, stop := startServer(t, srv)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	src := &tickerSource{ticker: time.NewTicker(5 * time.Millisecond)}
	defer src.ticker.Stop()

	done := make(chan struct{})
	go func() {
		Run(ctx, []string{addr}, src, Config{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, testLogger())
		close(done)
	}()

	time.Sleep(150 * time.Millisecond) // allow initial failure + retry + a few ticks
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	attempts, received := srv.snapshot()
	if attempts < 2 {
		t.Errorf("expected at least 2 connection attempts (initial failure + retry), got %d", attempts)
	}
	if received == 0 {
		t.Error("expected server to receive at least one event after reconnecting")
	}
}

func TestRun_FailsOverToNextAddress(t *testing.T) {
	downSrv := &alwaysFailServer{}
	downAddr, stopDown := startServer(t, downSrv)
	defer stopDown()

	upSrv := &countingServer{}
	upAddr, stopUp := startServer(t, upSrv)
	defer stopUp()

	ctx, cancel := context.WithCancel(context.Background())
	src := &tickerSource{ticker: time.NewTicker(5 * time.Millisecond)}
	defer src.ticker.Stop()

	done := make(chan struct{})
	go func() {
		// downAddr is listed first (the "closest" candidate) but is always down.
		Run(ctx, []string{downAddr, upAddr}, src, Config{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, testLogger())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if upSrv.count() == 0 {
		t.Error("expected events to reach the second address after the first failed")
	}
}
