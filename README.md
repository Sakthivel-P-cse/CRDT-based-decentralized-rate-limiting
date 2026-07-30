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
- **SWIM Membership**: Failure detection with suspicion mechanism and membership piggybacking
- **Delta Gossip**: Efficient state synchronization with retry-on-failure
- **Proportional Sliding Window**: Oldest epoch weighted by actual time overlap
- **gRPC API**: Language-agnostic client support
- **Observability**: Prometheus metrics, structured logging
- **Benchmarking**: Comparison vs Redis (see fairness note below)

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
NODE_ID=node-1 LISTEN_ADDR=:50051 METRICS_ADDR=:9091 PEERS=localhost:50052,localhost:50053 go run ./cmd/node/main.go
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
| `SOFT_THRESHOLD_RATIO` | `0.70` | Utilization ratio where probabilistic rejection begins |

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
  Priority priority = 3;
}

message AllowResponse {
  bool allowed = 1;
  int64 remaining = 2;
  string reason = 3;
  google.protobuf.Duration retry_after = 4;
}
```

### Client Example (Go)

```go
conn, _ := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := pb.NewRateLimiterClient(conn)

resp, _ := client.Allow(ctx, &pb.AllowRequest{
    ClientId: "user-123",
    Cost:     1,
    Priority: pb.Priority_PRIORITY_STANDARD,
})
fmt.Printf("Allowed: %v, Remaining: %d\n", resp.Allowed, resp.Remaining)
```

## Gradual Degradation

V2 keeps decisions local and adds four ways to reduce overload progressively:

- Priority tiering protects Critical requests longest, sheds Best Effort traffic from 70% utilization, and begins shedding Standard traffic from 90%.
- Probabilistic soft rejection starts at 70% of the current adaptive budget by default and increases linearly toward the hard limit, smoothing aggregate rejection instead of creating one abrupt boundary.
- AIMD adaptive budgets halve when merged gossip shows that a node contributed to an overshoot in the previous epoch, then regrow by 5% per clean epoch up to the original configured limit.
- Adaptive gossip shortens dissemination intervals as utilization rises, with a 20% floor that improves convergence without allowing a gossip storm.

Rejected requests report their exact decision reason and a Retry-After duration based on the current adaptive gossip interval, four convergence rounds, and a 50ms safety margin. These mechanisms preserve the system's original availability-first philosophy: every node still decides independently during partitions, and the added controls reduce overload and overshoot rather than replacing availability with strict coordination.

## Benchmarking

### Redis Baseline

> **⚠️ Fairness Note**: The Redis baseline uses a **fixed-window** centralized limiter (not a sliding window). The CRDT cluster uses a **proportional sliding window**. These are different algorithms with different accuracy characteristics. The Redis comparison shows operational trade-offs (SPOF, network path, algorithmic differences), NOT an apples-to-apples algorithmic comparison. Latency differences reflect both the algorithm AND the network path.

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

| `gossip_payload_processing_seconds` | Histogram | Latency of gossip payload processing |
| `ratelimit_estimated_overshoot` | Gauge | Estimated overshoot ratio |
| `ratelimit_active_clients` | Gauge | Number of active clients |

All metrics are wired to actual call sites and recorded at runtime.

## Testing

```bash
# Unit tests with race detector
go test -race ./...

# Specific packages
go test -race ./crdt/...
go test -race ./gossip/...

# Transitive membership discovery (Phase 1)
go test -race ./gossip/ -run TestTransitiveMembershipDiscovery -v

# GetAll race safety + shuffle uniformity (Phase 3)
go test -race ./gossip/ -run TestMembership_GetAll_Race -v
go test -race ./gossip/ -run TestMembership_ShuffleUniform -v

# Proportional sliding window semantics (Phase 4)
go test -race ./crdt/ -run TestEpochWindow_ProportionalOldestBucket -v

# Accuracy benchmark — no fudge factor (Phase 5)
go test -timeout 120s ./benchmark/ -run TestOvershootValidation -v
```

## Design Decisions

### CRDT Choice: G-Counter
- **Grow-only**: Natural fit for request counting
- **Commutative/Associative/Idempotent**: Mathematically proven merge
- **Delta-friendly**: Only send increments since last sync (with retry on failure)

### Proportional Sliding Window with Epochs
- Epoch = configurable (default 1s), Window = configurable (default 60s)
- Oldest bucket scaled by `overlap_fraction = (epoch_end − window_start) / epoch_size`
- This implements true proportional sliding-window semantics, not fixed-epoch overcounting
- Automatic GC of expired epochs; O(epochs) memory per client

### SWIM Membership with Piggybacking
- Direct ping with indirect fallback for failure detection
- **Membership digest piggybacked on every Ping/Ack payload** for transitive discovery:
  - Node A seeds only B; B seeds only C → after 1-2 ping rounds, A learns about C
  - No separate gossip RPC for membership; uses existing ping traffic
- Incarnation-number gating prevents stale gossip from resurrecting dead nodes
- Gossip dissemination targets Alive+Suspect peers (not only Alive), ensuring recovery after partitions

### Gossip Protocol
- Fanout = 3 (configurable), targeting Alive+Suspect peers
- Delta state with periodic full sync; only advances per-peer clock on *successful* send
- Per-goroutine context deadline (3× tick interval) to prevent goroutine leaks on network stalls

## CAP Trade-offs

| Property | Behavior |
|----------|----------|
| **Consistency** | Eventual (bounded overshoot) |
| **Availability** | Full during partitions |
| **Partition Tolerance** | Yes |

### Overshoot Bound

The formula verified by `TestOvershootValidation` is:

```
max_overshoot ≤ N × local_request_rate × gossip_interval × convergence_rounds
```

Where:
- `N` = number of nodes
- `local_request_rate` = requests per second per node (at the limit boundary)
- `gossip_interval` = gossip tick period
- `convergence_rounds` = 4 for the benchmark's N=3, fanout=2 configuration

**Example** (3 nodes, fanout=2, gossip_interval=500ms, 50 req/s per node):
```
max_overshoot ≤ 3 × 50 × 0.5 × 4 = 300 requests globally
```

The formula accounts for each node acting independently during four gossip rounds before full convergence.

> **Note**: The proportional sliding-window implementation means the *effective* request rate counted by each node at the limit boundary is the weighted sum over the window — which correctly accounts for epoch boundaries and avoids both over- and under-counting.

## Future Work

- [ ] LWW-Register for client metadata
- [ ] Raft-based strong consistency mode
- [ ] Kubernetes operator
- [ ] Multi-datacenter replication
- [ ] Client-side rate limit hints

## License

MIT
