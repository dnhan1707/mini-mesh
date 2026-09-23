// Package reconnect provides a shared "connect, stream, reconnect with
// exponential backoff" engine used by both CE (talking to RE) and RE
// (talking to GC), including ordered-address failover for CE's
// "try the closest available upstream" behavior.
package reconnect

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	telemetryv1 "mini-mesh/pb/telemetry/v1"
)

// Source produces the next event to forward. Next blocks until an event is
// ready or ctx is done, in which case ok is false.
type Source interface {
	Next(ctx context.Context) (event *telemetryv1.TelemetryEvent, ok bool)
}

// Config controls reconnect/backoff timing for Run.
type Config struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// Run streams events from src to the first reachable address in addrs,
// re-trying the whole list (starting from addrs[0] again) with exponential
// backoff whenever every address is unreachable or the active stream breaks.
// It returns once ctx is cancelled and any final in-flight stream has been
// closed gracefully.
func Run(ctx context.Context, addrs []string, src Source, cfg Config, logger *slog.Logger) {
	backoff := cfg.InitialBackoff

	for ctx.Err() == nil {
		progressed := false

		for _, addr := range addrs {
			sentAny, err := attempt(ctx, addr, src, logger)
			if err == nil {
				return // ctx cancelled; attempt already closed out gracefully
			}
			if sentAny {
				progressed = true
			}
			logger.Warn("connection attempt failed", "addr", addr, "error", err)
		}

		if progressed {
			backoff = cfg.InitialBackoff
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}

		backoff = nextBackoff(backoff, cfg.MaxBackoff)
	}
}

// nextBackoff doubles the current backoff, capped at limit.
func nextBackoff(current, limit time.Duration) time.Duration {
	next := current * 2
	if next > limit {
		return limit
	}
	return next
}

// attempt dials addr and streams events from src until the stream breaks or
// ctx is done.
func attempt(ctx context.Context, addr string, src Source, logger *slog.Logger) (bool, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn.Close()

	client := telemetryv1.NewTelemetryIngestClient(conn)
	return runStream(ctx, client, src, addr, logger)
}

// runStream opens one stream and forwards events from src until it breaks or
// ctx is cancelled. Returns whether any event was sent (used to decide
// whether to reset backoff after real progress).
func runStream(ctx context.Context, client telemetryv1.TelemetryIngestClient, src Source, addr string, logger *slog.Logger) (bool, error) {
	// Detach the RPC from ctx's cancellation so CloseAndRecv below can still
	// complete gracefully; ctx.Done() is instead used to end the send loop.
	stream, err := client.StreamTelemetry(context.WithoutCancel(ctx))
	if err != nil {
		return false, err
	}

	sentAny := false
	for {
		event, ok := src.Next(ctx)
		if !ok {
			resp, err := stream.CloseAndRecv()
			if err == nil {
				logger.Info("upstream acknowledged events before shutdown", "addr", addr, "events_received", resp.EventsReceived)
			}
			return sentAny, nil
		}

		if err := stream.Send(event); err != nil {
			return sentAny, err
		}
		sentAny = true
		logger.Info("forwarded telemetry event", "addr", addr, "event_id", event.EventId, "node_id", event.NodeId)
	}
}
