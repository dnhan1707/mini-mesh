package config

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	Host = "localhost"

	CEPort = 50051
	REPort = 50051
	GCPort = 50052

	MaxRetryAttempts = 6
	MaxRetryDelay    = 60 * time.Second
	RetryBaseDelay   = time.Second
	RetryJitterMax   = 1000 * time.Millisecond

	CEStreamTicker    = 5 * time.Second
	GCStateTicker     = 10 * time.Second
	CPUReadInterval   = 500 * time.Millisecond
	MetricsSendWindow = 5 * time.Second
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

func GCListenAddress() string {
	return fmt.Sprintf(":%d", GCPort)
}
