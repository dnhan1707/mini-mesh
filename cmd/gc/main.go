package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"mini-mesh/internal/config"
	pb "mini-mesh/pb/minimesh/v1"

	"google.golang.org/grpc"
)

// gcServer implements the gRPC server interface and holds the global state.
type gcServer struct {
	pb.UnimplementedTelemetryServiceServer

	// A thread-safe map to store the latest metrics for each node.
	// In a real SRE environment, this would be a time-series database like Prometheus.
	mu       sync.RWMutex
	nodeData map[string]*pb.TelemetryPayload
}

// StreamTelemetry handles incoming data from the Regional Edges.
func (s *gcServer) StreamTelemetry(stream pb.TelemetryService_StreamTelemetryServer) error {
	log.Println("New Regional Edge (RE) connected to Global Controller!")

	for {
		payload, err := stream.Recv()
		if err == io.EOF {
			log.Println("RE disconnected gracefully.")
			return nil
		}
		if err != nil {
			log.Printf("Stream error from RE: %v", err)
			return err
		}

		// Update our global state map safely
		s.mu.Lock()
		s.nodeData[payload.NodeId] = payload
		s.mu.Unlock()

		log.Printf("[GC Ingest] Updated state for node: %s", payload.NodeId)
	}
}

// printState is a simple background loop to visualize the GC's internal state.
// This simulates what a frontend dashboard would query.
func (s *gcServer) printState() {
	ticker := time.NewTicker(config.GCStateTicker)
	for range ticker.C {
		s.mu.RLock() // Use a Read Lock since we are just viewing the data
		fmt.Println("\n--- GLOBAL CONTROLLER STATE ---")
		fmt.Printf("Active Nodes: %d\n", len(s.nodeData))

		for nodeID, data := range s.nodeData {
			fmt.Printf("  -> %s: CPU %.1f%%, RAM %dMB\n", nodeID, data.CpuPercent, data.UsedMemMb)
		}
		fmt.Println("-------------------------------")
		s.mu.RUnlock()
	}
}

func main() {
	port := config.GCListenAddress()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("GC failed to listen: %v", err)
	}

	server := &gcServer{
		nodeData: make(map[string]*pb.TelemetryPayload),
	}

	// Start the background state printer
	go server.printState()

	grpcServer := grpc.NewServer()
	pb.RegisterTelemetryServiceServer(grpcServer, server)

	log.Printf("Global Controller (GC) listening on %s", port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("GC failed to serve: %v", err)
	}
}
