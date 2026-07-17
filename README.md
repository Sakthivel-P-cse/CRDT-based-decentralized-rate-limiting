# CRDT-based Decentralized Rate Limiter

A production-grade, decentralized rate limiter built with CRDTs (Conflict-free Replicated Data Types) and gossip protocols. Provides high availability and partition tolerance (AP in CAP) with eventual consistency.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Request                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RateLimiterServer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  gRPC API   │  │ EpochWindow │  │      CRDT State         │  │
│  │  (Allow)    │──▶│ (Sliding)   │──▶│  G-Counter per Epoch    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
         ┌─────────────────┐       ┌─────────────────┐
         │  SWIM Membership│       │  Delta Gossip   │
         │  (Failure Det)  │       │  (State Sync)   │
         └─────────────────┘       └─────────────────┘
                    │                       │
                    └───────────┬───────────┘
                                ▼
                    ┌─────────────────────┐
                    │   Peer Nodes        │
                    │  (gRPC Mesh)        │
                    └─────────────────────┘
```

## Features

- **CRDT-based**: G-Counter per epoch for mergeable state
- **Decentralized**: No single point of failure, no Redis required
- **Partition Tolerant**: AP system - serves requests during partitions
- **SWIM Membership**: Failure detection with suspicion mechanism
- **Delta Gossip**: Efficient state synchronization
- **gRPC API**: Language-agnostic client support
- **Observability**: Prometheus metrics, structured logging
- **Benchmarking**: Redis baseline comparison included

## Quick Start

### With Docker Compose (3-node cluster)

```bash
docker-compose up --build
```

This starts:
- 3 rate limiter nodes (ports 50051, 50052, 50053)
- Redis for baseline comparison (port 6379)

### Local Development

```bash
# Install Go 1.26+
go mod download
go build ./cmd/node/

# Run single node
NODE_ID=node-1 ./rate-limiter

# Run with peers
NODE_ID=node-1 PEERS=localhost:50052,localhost:50053 ./rate-limiter
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Unique node identifier |
| `LISTEN_ADDR` | `:50051` | gRPC listen address |
| `PEERS` | `` | Comma-separated peer addresses |
| `EPOCH_SIZE` | `1s` | Epoch duration |
| `WINDOW_SIZE` | `60s` | Sliding window size |
| `GOSSIP_INTERVAL` | `500ms` | Gossip tick interval |
| `SUSPECT_TIMEOUT` | `1s` | Failure suspicion timeout |
| `PING_TIMEOUT` | `2s` | Ping request timeout |
| `FANOUT` | `3` | Gossip fanout |
| `RATE_LIMIT` | `1000` | Requests per window |

## gRPC API

```protobuf
service RateLimiter {
  rpc Allow(AllowRequest) returns (AllowResponse);
  rpc Ping(PingRequest) returns (PingResponse);
  rpc GossipState(GossipRequest) returns (GossipResponse);
}

message AllowRequest {
  string client_id = 1;
  int64 cost = 2;
}

message AllowResponse {
  bool allowed = 1;
  int64 remaining = 2;
}
```

### Client Example (Go)

```go
conn, _ := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := pb.NewRateLimiterClient(conn)

resp, _ := client.Allow(ctx, &pb.AllowRequest{
    ClientId: "user-123",
    Cost:     1,
})
fmt.Printf("Allowed: %v, Remaining: %d\n", resp.Allowed, resp.Remaining)
```

## Benchmarking

### Redis Baseline
```bash
go run ./benchmark -mode=redis -redis=localhost:6379 -rps=10000 -duration=30s
```

### CRDT Cluster
```bash
go run ./benchmark -mode=grpc -nodes=localhost:50051,localhost:50052,localhost:50053 -rps=10000 -duration=30s
```

### Partition Test
```bash
go test -v ./node -run TestNetworkPartition
```

## Metrics (Prometheus)

Expose metrics on `:9090/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `ratelimit_allow_total` | Counter | Total allow requests by result |
| `ratelimit_decision_latency_seconds` | Histogram | Decision latency |
| `gossip_round_latency_seconds` | Histogram | Gossip round latency |
| `gossip_convergence_seconds` | Histogram | Convergence time after heal |
| `ratelimit_estimated_overshoot` | Gauge | Estimated overshoot ratio |

## Testing

```bash
# Unit tests with race detector
go test -race ./...

# Specific packages
go test -race ./crdt/...
go test -race ./gossip/...
```

## Design Decisions

### CRDT Choice: G-Counter
- **Grow-only**: Natural fit for request counting
- **Commutative/Associative/Idempotent**: Mathematically proven merge
- **Delta-friendly**: Only send increments since last sync

### Sliding Window with Epochs
- Epoch = 1 second, Window = 60 epochs (1 minute)
- Automatic GC of expired epochs
- O(epochs) memory per client

### SWIM-lite Membership
- Direct ping with indirect fallback
- Suspicion mechanism reduces false positives
- Incarnation numbers handle restarts

### Gossip Protocol
- Fanout = 3 (configurable)
- Delta state with periodic full sync
- Per-peer epoch tracking for efficiency

## CAP Trade-offs

| Property | Behavior |
|----------|----------|
| **Consistency** | Eventual (bounded overshoot) |
| **Availability** | Full during partitions |
| **Partition Tolerance** | Yes |

**Overshoot Bound**: `max_overshoot ≈ (num_nodes - 1) × local_request_rate × gossip_interval`

Example: 5 nodes, 500ms gossip interval, 50 req/s local rate per node → max 100 requests overshoot globally before convergence.

## Future Work

- [ ] LWW-Register for client metadata
- [ ] Raft-based strong consistency mode
- [ ] Kubernetes operator
- [ ] Multi-datacenter replication
- [ ] Adaptive gossip intervals
- [ ] Client-side rate limit hints

## License

MIT