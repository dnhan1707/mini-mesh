package main

import (
	"context"
	"fmt"
	"log"
	"time"

	// This is your generated contract code!
	pb "mini-mesh/pb/minimesh/v1"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	nodeID := "ce-local-laptop"
	serverAddr := "localhost:50051"

	log.Printf("Starting CE daemon on node: %s\n", nodeID)

	// We are dialing the RE server here
	// insecure.NewCredentials() means we are NOT using TLS/HTTPs yet
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to RE server: %v", err)
	}
	defer conn.Close()

	// We wrap the raw connection in our generated client interface
	client := pb.NewTelemetryServiceClient(conn)

	// context.Background() tells Go this process has no expiration date.
	// We call the StreamTelemetry function to open the persistent HTTP/2 pipe.
	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		log.Fatalf("Failed to open telemetry stream: %v", err)
	}
	log.Println("Successfully opened telemetry stream to RE.")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		payload, err := collectMetrics(nodeID)
		if err != nil {
			log.Printf("Failed to collect metrics: %v", err)
			continue
		}

		// stream.Send() takes our generated struct, encodes it to binary,
		// and shoves it down the open gRPC pipe.
		if err := stream.Send(payload); err != nil {
			log.Printf("Failed to send payload: %v\n", err)
		} else {
			log.Printf("Sent metrics -> CPU: %.1f%% | RAM: %dMB",
				payload.CpuPercent, payload.UsedMemMb)
		}
	}
}

func collectMetrics(nodeID string) (*pb.TelemetryPayload, error) {
	cpuPercentages, err := cpu.Percent(500*time.Millisecond, false)
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
