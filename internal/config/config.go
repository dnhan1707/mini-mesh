package config

import (
	"fmt"
	"net"
	"strconv"
)

const (
	Host = "localhost"

	// Port numbers used by the mesh services.
	CEPort = 50051
	REPort = 50051
	GCPort = 50052
)

func CEAddress() string {
	return net.JoinHostPort(Host, strconv.Itoa(CEPort))
}

func REAddress() string {
	return net.JoinHostPort(Host, strconv.Itoa(REPort))
}

func GCAddress() string {
	return net.JoinHostPort(Host, strconv.Itoa(GCPort))
}

func REListenAddress() string {
	return fmt.Sprintf(":%d", REPort)
}
