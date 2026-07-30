package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
	"google.golang.org/grpc"
)

func main() {
	nodeID := getEnv("NODE_ID", "node-1")
	listenAddr := getEnv("LISTEN_ADDR", ":50051")
	metricsAddr := getEnv("METRICS_ADDR", ":9090")
	peers := getEnv("PEERS", "")
	peerAddrs := parsePeers(peers)
	softThresholdRatio, err := strconv.ParseFloat(getEnv("SOFT_THRESHOLD_RATIO", "0.70"), 64)
	if err != nil {
		log.Fatalf("invalid SOFT_THRESHOLD_RATIO: %v", err)
	}

	cfg := node.Config{
		NodeID:             nodeID,
		ListenAddr:         listenAddr,
		MetricsAddr:        metricsAddr,
		Peers:              peerAddrs,
		EpochSize:          1 * time.Second,
		WindowSize:         60 * time.Second,
		GossipInterval:     500 * time.Millisecond,
		SuspectTimeout:     1 * time.Second,
		PingTimeout:        2 * time.Second,
		Fanout:             3,
		RateLimit:          1000,
		SoftThresholdRatio: softThresholdRatio,
	}

	server := node.NewRateLimiterServer(cfg)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRateLimiterServer(grpcServer, server)
	server.SetGRPCServer(grpcServer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := startServing(ctx, server, grpcServer, lis); err != nil {
		log.Fatalf("Server start error: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	server.Stop()
	grpcServer.GracefulStop()
}

func startServing(ctx context.Context, server *node.RateLimiterServer, grpcServer *grpc.Server, lis net.Listener) error {
	if err := server.Start(ctx); err != nil {
		_ = lis.Close()
		return err
	}
	go util.SafeGo("grpc-serve", func() {
		log.Printf("Starting RateLimiter server on %s", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	})
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parsePeers(peers string) []string {
	if peers == "" {
		return []string{}
	}
	var result []string
	for _, p := range splitPeers(peers) {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func splitPeers(s string) []string {
	var result []string
	current := ""
	for _, c := range s {
		if c == ',' {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
