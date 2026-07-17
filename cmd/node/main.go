package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
)

func main() {
	nodeID := getEnv("NODE_ID", "node-1")
	listenAddr := getEnv("LISTEN_ADDR", ":50051")
	metricsAddr := getEnv("METRICS_ADDR", ":9090")
	peers := getEnv("PEERS", "")
	peerAddrs := parsePeers(peers)

	cfg := node.Config{
		NodeID:           nodeID,
		ListenAddr:       listenAddr,
		MetricsAddr:      metricsAddr,
		Peers:            peerAddrs,
		EpochSize:        1 * time.Second,
		WindowSize:       60 * time.Second,
		GossipInterval:   500 * time.Millisecond,
		SuspectTimeout:   1 * time.Second,
		PingTimeout:      2 * time.Second,
		Fanout:           3,
		RateLimit:        1000,
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

	go func() {
		if err := server.Start(ctx); err != nil {
			log.Printf("Server start error: %v", err)
		}
	}()

	go func() {
		log.Printf("Starting RateLimiter server on %s", cfg.ListenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	metricsServer := node.NewMetricsServer(cfg.NodeID, cfg.MetricsAddr)
	if err := metricsServer.Start(); err != nil {
		log.Printf("Metrics server start error: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	server.Stop()
	grpcServer.GracefulStop()
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	metricsServer.Stop(shutdownCtx)
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