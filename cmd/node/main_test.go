package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestStartServingGatesGRPCOnStartFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	server := node.NewRateLimiterServer(node.Config{})
	err = startServing(context.Background(), server, grpc.NewServer(), lis)
	require.Error(t, err)

	conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
	}
	require.Error(t, err)
}
