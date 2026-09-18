package main

import (
	"context"
	"io"
	"log"
	"mini-mesh/internal/config"
	pb "mini-mesh/pb/minimesh/v1"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// regionalEdgeServer implements the generated gRPC server interface
type regionalEdgeServer struct {
	pb.UnimplementedTelemetryServiceServer
	gcClient pb.TelemetryServiceClient
}

func (s *regionalEdgeServer) StreamTelemetry(ceStream pb.TelemetryService_StreamTelemetryServer) error {
	log.Println("New CE connected! Opening proxy stream to GC...")

	// Open a new outbound pipe to the GC specifically for this incoming CE
	gcStream, err := s.gcClient.StreamTelemetry(context.Background())
	if err != nil {
		log.Printf("Failed to open stream to GC: %v", err)
		return err
	}

	// the infinite loop to read from the pipe
	for {
		payload, err := ceStream.Recv()

		// io.EOF means the CE gracefully closed the connection
		if err == io.EOF {
			log.Println("CE disconnected gracefully.")
			return nil
		}
		// Any other error means the network dropped unexpectedly
		if err != nil {
			log.Printf("Stream error: %v", err)
			return err
		}

		log.Printf("[RE Proxied] Node: %s | CPU: %.1f%%", payload.NodeId, payload.CpuPercent)

		if err := gcStream.Send(payload); err != nil {
			log.Printf("Failed to forward to GC: %v", err)
			return err
		}
	}
}

func main() {
	gcAddr := config.GCAddress()
	gcConn, err := grpc.NewClient(gcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to GC: %v", err)
	}
	defer gcConn.Close()

	// Initialize the client
	gcClient := pb.NewTelemetryServiceClient(gcConn)

	// Act as a SERVER to CE
	rePort := config.REListenAddress()
	listener, err := net.Listen("tcp", rePort)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", rePort, err)
	}

	grpcServer := grpc.NewServer()

	//Inject the GC client into our server struct
	pb.RegisterTelemetryServiceServer(grpcServer, &regionalEdgeServer{
		gcClient: gcClient,
	})
	log.Printf("Regional Edge (RE) forwarding to GC at %s, listening for CEs on %s", gcAddr, rePort)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
