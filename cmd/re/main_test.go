package main

import (
	"context"
	"io"
	"testing"

	pb "mini-mesh/pb/minimesh/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type fakeGCClient struct {
	stream *fakeGCClientStream
}

func (f *fakeGCClient) StreamTelemetry(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[pb.TelemetryPayload, pb.TelemetryResponse], error) {
	return f.stream, nil
}

type fakeGCClientStream struct {
	sent []*pb.TelemetryPayload
	ctx  context.Context
}

func (f *fakeGCClientStream) Send(req *pb.TelemetryPayload) error {
	f.sent = append(f.sent, req)
	return nil
}

func (f *fakeGCClientStream) CloseAndRecv() (*pb.TelemetryResponse, error) {
	return &pb.TelemetryResponse{}, nil
}

func (f *fakeGCClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *fakeGCClientStream) Trailer() metadata.MD {
	return nil
}

func (f *fakeGCClientStream) CloseSend() error {
	return nil
}

func (f *fakeGCClientStream) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeGCClientStream) SendMsg(m any) error {
	return nil
}

func (f *fakeGCClientStream) RecvMsg(m any) error {
	return nil
}

type fakeCEServerStream struct {
	payloads []*pb.TelemetryPayload
	idx      int
	ctx      context.Context
}

func (f *fakeCEServerStream) Recv() (*pb.TelemetryPayload, error) {
	if f.idx >= len(f.payloads) {
		return nil, io.EOF
	}
	payload := f.payloads[f.idx]
	f.idx++
	return payload, nil
}

func (f *fakeCEServerStream) SendAndClose(*pb.TelemetryResponse) error {
	return nil
}

func (f *fakeCEServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (f *fakeCEServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (f *fakeCEServerStream) SetTrailer(metadata.MD) {}

func (f *fakeCEServerStream) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeCEServerStream) SendMsg(m any) error {
	return nil
}

func (f *fakeCEServerStream) RecvMsg(m any) error {
	return nil
}

func TestRegionalEdgeServer_StreamTelemetry_ProxiesPayloadToGC(t *testing.T) {
	payload := &pb.TelemetryPayload{
		NodeId:     "ce-node-1",
		CpuPercent: 42.5,
	}

	gcStream := &fakeGCClientStream{}
	server := &regionalEdgeServer{
		gcClient: &fakeGCClient{stream: gcStream},
	}
	ceStream := &fakeCEServerStream{payloads: []*pb.TelemetryPayload{payload}}

	err := server.StreamTelemetry(ceStream)
	if err != nil {
		t.Fatalf("expected no error proxying payload, got %v", err)
	}

	if len(gcStream.sent) != 1 {
		t.Fatalf("expected 1 payload forwarded, got %d", len(gcStream.sent))
	}

	forwarded := gcStream.sent[0]
	if forwarded.NodeId != payload.NodeId {
		t.Fatalf("expected NodeId %q, got %q", payload.NodeId, forwarded.NodeId)
	}
	if forwarded.CpuPercent != payload.CpuPercent {
		t.Fatalf("expected CPU %.2f, got %.2f", payload.CpuPercent, forwarded.CpuPercent)
	}
}

func TestRegionalEdgeServer_StreamTelemetry_StopsOnEOF(t *testing.T) {
	gcStream := &fakeGCClientStream{}
	server := &regionalEdgeServer{
		gcClient: &fakeGCClient{stream: gcStream},
	}
	ceStream := &fakeCEServerStream{}

	err := server.StreamTelemetry(ceStream)
	if err != nil {
		t.Fatalf("expected EOF to be handled gracefully, got %v", err)
	}

	if len(gcStream.sent) != 0 {
		t.Fatalf("expected no forwarded payloads after EOF, got %d", len(gcStream.sent))
	}
}
