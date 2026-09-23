package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/types/known/timestamppb"

	"mini-mesh/internal/reconnect"
	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

const (
	tickInterval   = 2 * time.Second
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second

	// defaultREAddrs is a comma-separated, ordered-by-preference list of RE
	// addresses; CE tries them in order, simulating "closest available RE".
	defaultREAddrs = "localhost:50052"
)

func main() {
	nodeID, err := os.Hostname()
	if err != nil {
		nodeID = "unknown-node"
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("node_id", nodeID))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reAddrs := parseAddrs(envOrDefault("RE_ADDRS", defaultREAddrs))

	src := &tickerSource{nodeID: nodeID, ticker: time.NewTicker(tickInterval)}
	defer src.ticker.Stop()

	reconnect.Run(ctx, reAddrs, src, reconnect.Config{InitialBackoff: initialBackoff, MaxBackoff: maxBackoff}, slog.Default())
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseAddrs splits a comma-separated address list, trimming whitespace and
// dropping empty entries.
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

// tickerSource collects a fresh telemetry event on every tick.
type tickerSource struct {
	nodeID string
	ticker *time.Ticker
}

func (s *tickerSource) Next(ctx context.Context) (*telemetryv1.TelemetryEvent, bool) {
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-s.ticker.C:
			event, err := collectEvent(s.nodeID)
			if err != nil {
				slog.Warn("failed to collect metrics", "error", err)
				continue
			}
			return event, true
		}
	}
}

func collectEvent(nodeID string) (*telemetryv1.TelemetryEvent, error) {
	percentages, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	vm, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	return &telemetryv1.TelemetryEvent{
		EventId:     fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()),
		NodeId:      nodeID,
		CollectedAt: timestamppb.Now(),
		Metrics: &telemetryv1.SystemMetrics{
			CpuPercent:       percentages[0],
			MemoryUsedBytes:  vm.Used,
			MemoryTotalBytes: vm.Total,
		},
	}, nil
}
