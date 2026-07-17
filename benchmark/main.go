package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const redisTokenBucketScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local current = redis.call('GET', key)
if current == false then
    redis.call('SET', key, limit - cost, 'PX', window)
    return {1, limit - cost}
end

current = tonumber(current)
if current >= cost then
    redis.call('DECRBY', key, cost)
    return {1, current - cost}
else
    return {0, current}
end
`

type BenchmarkClient struct {
	redisLimiter *RedisLimiter
	grpcClients  []*GRPCClient
	mode         string
	clientIDs    []string
}

type RedisLimiter struct {
	client *redis.Client
	script *redis.Script
	limit  int64
	window time.Duration
}

func NewRedisLimiter(addr string, limit int64, window time.Duration) *RedisLimiter {
	client := redis.NewClient(&redis.Options{Addr: addr})
	script := redis.NewScript(redisTokenBucketScript)
	return &RedisLimiter{client: client, script: script, limit: limit, window: window}
}

func (r *RedisLimiter) Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error) {
	key := fmt.Sprintf("ratelimit:%s", clientID)
	result, err := r.script.Run(ctx, r.client, []string{key}, r.limit, r.window.Milliseconds(), cost, time.Now().UnixMilli()).Slice()
	if err != nil {
		return false, 0, err
	}
	allowed := result[0].(int64) == 1
	remaining := result[1].(int64)
	return allowed, remaining, nil
}

func (r *RedisLimiter) Close() error {
	return r.client.Close()
}

type GRPCClient struct {
	client pb.RateLimiterClient
	conn   *grpc.ClientConn
}

func NewGRPCClient(addr string) (*GRPCClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCClient{client: pb.NewRateLimiterClient(conn), conn: conn}, nil
}

func (c *GRPCClient) Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error) {
	resp, err := c.client.Allow(ctx, &pb.AllowRequest{ClientId: clientID, Cost: cost})
	if err != nil {
		return false, 0, err
	}
	return resp.Allowed, resp.Remaining, nil
}

func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

type RoundRobinLimiter struct {
	limiters []interface {
		Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error)
		Close() error
	}
	index int
	mu    sync.Mutex
}

func (r *RoundRobinLimiter) Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error) {
	r.mu.Lock()
	limiter := r.limiters[r.index%len(r.limiters)]
	r.index++
	r.mu.Unlock()
	return limiter.Allow(ctx, clientID, cost)
}

func (r *RoundRobinLimiter) Close() error {
	var lastErr error
	for _, l := range r.limiters {
		if err := l.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

type BenchmarkResult struct {
	Total     int64
	Allowed   int64
	Denied    int64
	Errors    int64
	Latencies []float64
	StartTime time.Time
	EndTime   time.Time
	mu        sync.Mutex
}

func (r *BenchmarkResult) Add(allowed bool, latency float64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Total++
	r.Latencies = append(r.Latencies, latency)
	if err != nil {
		r.Errors++
	} else if allowed {
		r.Allowed++
	} else {
		r.Denied++
	}
}

func (r *BenchmarkResult) Percentile(p float64) float64 {
	if len(r.Latencies) == 0 {
		return 0
	}
	sorted := make([]float64, len(r.Latencies))
	copy(sorted, r.Latencies)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (r *BenchmarkResult) Print(name string, duration time.Duration) {
	avgLatency := 0.0
	if len(r.Latencies) > 0 {
		for _, l := range r.Latencies {
			avgLatency += l
		}
		avgLatency /= float64(len(r.Latencies))
	}
	secs := duration.Seconds()
	fmt.Printf("\n=== %s Benchmark Results ===\n", name)
	fmt.Printf("Duration: %.2fs\n", secs)
	fmt.Printf("Total requests: %d\n", r.Total)
	fmt.Printf("Allowed: %d (%.2f%%)\n", r.Allowed, float64(r.Allowed)/float64(r.Total)*100)
	fmt.Printf("Denied: %d\n", r.Denied)
	fmt.Printf("Errors: %d\n", r.Errors)
	fmt.Printf("Throughput: %.2f req/s\n", float64(r.Total)/secs)
	fmt.Printf("Avg latency: %.2f ms\n", avgLatency)
	fmt.Printf("P50 latency: %.2f ms\n", r.Percentile(0.50))
	fmt.Printf("P99 latency: %.2f ms\n", r.Percentile(0.99))
	fmt.Printf("P99.9 latency: %.2f ms\n", r.Percentile(0.999))
}

func NewBenchmarkClient(mode, redisAddr, nodeAddrs string, limit int64, window time.Duration, numClients int) (*BenchmarkClient, error) {
	clientIDs := make([]string, numClients)
	for i := 0; i < numClients; i++ {
		clientIDs[i] = fmt.Sprintf("bench-client-%d", i)
	}

	bc := &BenchmarkClient{mode: mode, clientIDs: clientIDs}

	if mode == "redis" || mode == "both" {
		bc.redisLimiter = NewRedisLimiter(redisAddr, limit, window)
	}

	if mode == "grpc" || mode == "both" {
		addresses := splitAddresses(nodeAddrs)
		for _, addr := range addresses {
			client, err := NewGRPCClient(addr)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
			}
			bc.grpcClients = append(bc.grpcClients, client)
		}
	}

	return bc, nil
}

func (bc *BenchmarkClient) Close() {
	if bc.redisLimiter != nil {
		bc.redisLimiter.Close()
	}
	for _, c := range bc.grpcClients {
		c.Close()
	}
}

func (bc *BenchmarkClient) getLimiter() interface {
	Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error)
	Close() error
} {
	switch bc.mode {
	case "redis":
		return bc.redisLimiter
	case "grpc":
		return &RoundRobinLimiter{limiters: toInterfaceSlice(bc.grpcClients)}
	case "both":
		limiters := []interface {
			Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error)
			Close() error
		}{bc.redisLimiter}
		for _, c := range bc.grpcClients {
			limiters = append(limiters, c)
		}
		return &RoundRobinLimiter{limiters: limiters}
	default:
		return bc.redisLimiter
	}
}

func toInterfaceSlice(clients []*GRPCClient) []interface {
	Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error)
	Close() error
} {
	result := make([]interface {
		Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error)
		Close() error
	}, len(clients))
	for i, c := range clients {
		result[i] = c
	}
	return result
}

func (bc *BenchmarkClient) RunBenchmark(ctx context.Context, rps int, duration time.Duration, cost int64) *BenchmarkResult {
	limiter := bc.getLimiter()
	result := &BenchmarkResult{StartTime: time.Now(), Latencies: make([]float64, 0, rps*int(duration.Seconds()))}

	var wg sync.WaitGroup
	requestCh := make(chan struct{}, rps*2)

	// Request generator
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(rps))
		defer ticker.Stop()
		deadline := time.Now().Add(duration)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Now().After(deadline) {
					return
				}
				select {
				case requestCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	// Workers
	numWorkers := 10
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for range requestCh {
				clientID := bc.clientIDs[workerID%len(bc.clientIDs)]
				start := time.Now()
				allowed, _, err := limiter.Allow(ctx, clientID, cost)
				latency := time.Since(start).Seconds() * 1000
				result.Add(allowed, latency, err)
			}
		}(i)
	}

	wg.Wait()
	result.EndTime = time.Now()
	return result
}

func splitAddresses(s string) []string {
	var result []string
	current := ""
	for _, c := range s {
		if c == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func main() {
	mode := flag.String("mode", "redis", "Benchmark mode: redis, grpc, or both")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	nodeAddrs := flag.String("nodes", "localhost:50051,localhost:50052,localhost:50053", "Comma-separated gRPC node addresses")
	limit := flag.Int64("limit", 10000, "Rate limit per window")
	window := flag.Duration("window", 60*time.Second, "Window size")
	cost := flag.Int64("cost", 1, "Cost per request")
	rps := flag.Int("rps", 1000, "Requests per second")
	duration := flag.Duration("duration", 30*time.Second, "Benchmark duration")
	clients := flag.Int("clients", 100, "Number of unique clients")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Interrupted")
		cancel()
	}()

	client, err := NewBenchmarkClient(*mode, *redisAddr, *nodeAddrs, *limit, *window, *clients)
	if err != nil {
		log.Fatalf("Failed to create benchmark client: %v", err)
	}
	defer client.Close()

	log.Printf("Starting benchmark: mode=%s, rps=%d, duration=%v, clients=%d", *mode, *rps, *duration, *clients)
	result := client.RunBenchmark(ctx, *rps, *duration, *cost)
	result.Print(strings.Title(*mode), *duration)
}