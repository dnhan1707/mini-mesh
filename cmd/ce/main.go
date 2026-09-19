package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"mini-mesh/internal/config"

	// This is your generated contract code!
	pb "mini-mesh/pb/minimesh/v1"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}
	nodeID := fmt.Sprintf("ce-%s", hostname)
	serverAddr := config.REAddress()

	log.Printf("Starting CE daemon on node: %s\n", nodeID)

	// We are dialing the RE server here
	// insecure.NewCredentials() means we are NOT using TLS/HTTPs yet
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to RE server: %v", err)
	}
	defer conn.Close()

	// We wrap the raw connection in our generated client interface
	client := pb.NewTelemetryServiceClient(conn)
	ticker := time.NewTicker(config.CEStreamTicker)
	defer ticker.Stop()

	attempt := 0

	for {
		log.Printf("Attempting to open telemetry stream...")
		stream, err := client.StreamTelemetry(context.Background())
		if err != nil {
			wait := calculateWait(attempt)
			log.Printf("Failed to open stream. Retrying in %v... Error: %v\n", wait, err)
			time.Sleep(wait)

			// Cap attempts to prevent integer overflow if it runs for months
			if attempt < config.MaxRetryAttempts {
				attempt++
			}
			continue
		}

		log.Println("Successfully established stream to RE!")
		attempt = 0 // Reset attempts on success!

		streamAlive := true
		for streamAlive {
			<-ticker.C // wait for 5 second tick
			payload, err := collectMetrics(nodeID)
			if err != nil {
				log.Printf("Failed to collect metrics: %v\n", err)
				continue
			}

			// If Send fails, the pipe is broken.
			// We break the inner loop to force the outer loop to create a new stream.
			if err := stream.Send(payload); err != nil {
				log.Printf("Connection to RE lost. Dropping stream: %v\n", err)
				streamAlive = false
			} else {
				log.Printf("Sent metrics -> CPU: %.1f%% | RAM: %dMB", payload.CpuPercent, payload.UsedMemMb)
			}
		}
	}
}

func collectMetrics(nodeID string) (*pb.TelemetryPayload, error) {
	cpuPercentages, err := cpu.Percent(config.CPUReadInterval, false)
	if err != nil {
		return nil, fmt.Errorf("Failed to read CPU: %w", err)
	}
	// Grab RAM usage
	vMem, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to read memory: %w", err)
	}

	return &pb.TelemetryPayload{
		NodeId:     nodeID,
		Timestamp:  time.Now().Unix(),
		CpuPercent: cpuPercentages[0],
		TotalMemMb: vMem.Total / 1024 / 1024,
		UsedMemMb:  vMem.Used / 1024 / 1024,
		MemPercent: vMem.UsedPercent,
	}, nil
}

func calculateWait(attempt int) time.Duration {
	baseWait := time.Duration(1<<attempt) * config.RetryBaseDelay
	if baseWait > config.MaxRetryDelay {
		return config.MaxRetryDelay
	}
	jitter := time.Duration(rand.Intn(int(config.RetryJitterMax/time.Millisecond))) * time.Millisecond
	return baseWait + jitter
}
