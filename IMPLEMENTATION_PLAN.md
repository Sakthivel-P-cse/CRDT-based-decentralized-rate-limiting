# CRDT-Based Decentralized Rate Limiter - Implementation Plan

## Project Overview
Build a production-grade CRDT-based decentralized rate limiter in Go with gRPC, Docker Compose, and comprehensive benchmarking against a Redis baseline.

**Tech Stack**: Go 1.26, gRPC/Protobuf, Docker Compose, Redis (baseline), golangci-lint, go test -race

---

## Phase 1: Single-Node Baseline (Week 1) - ✅ DONE

### 1.1 Proto Definition (`proto/ratelimiter.proto`)
- [x] Define `AllowRequest(client_id: string, cost: int64) -> AllowResponse(allowed: bool, remaining: int64)`
- [x] Define epoch window configuration (epoch size, window size)
- [x] Generate Go code: `protoc --go_out=. --go-grpc_out=. proto/ratelimiter.proto`

### 1.2 Epoch Window (`crdt/epoch_window.go`)
- [x] `EpochWindow` struct with epoch size (ms), window size (epochs)
- [x] `Add(count int64)` - add count to current epoch bucket
- [x] `AddAtEpoch(count int64, epoch int64)` - add count to specific epoch (for testing/gossip)
- [x] `Value() int64` - sum across active window
- [x] `AdvanceEpoch(currentEpoch int64)` - GC expired epochs (sliding window)
- [x] Thread-safe with `sync.RWMutex`

### 1.3 Node Server (`node/server.go`, `node/main.go`, `node/config.go`)
- [x] `Config`: nodeID, listenAddr, epochSizeMs, windowEpochs, defaultLimit, gossipInterval, peerAddrs
- [x] `RateLimiterServer` implementing `Allow` RPC
- [x] Local in-memory `map[string]*EpochWindow` per client_id
- [x] gRPC server with reflection for debugging
- [x] Graceful shutdown (signal handling)

### 1.4 Tests (`crdt/epoch_window_test.go`, `node/server_test.go`)
- [x] Window boundary edge cases (exact boundary, burst-then-idle)
- [x] Epoch expiry/GC correctness
- [x] Concurrent access race detection (`go test -race`)
- [x] gRPC integration test with test server

---

## Phase 2: CRDT Counter + Local Merge (Week 1-2) - ✅ DONE

### 2.1 G-Counter (`crdt/counter.go`)
- [x] `GCounter` struct: `map[string]uint64` (nodeID -> count)
- [x] `Increment(nodeID string)` - increment local node's slot
- [x] `Add(nodeID string, count uint64)` - add to node's slot
- [x] `Merge(other *GCounter)` - element-wise max (commutative, associative, idempotent)
- [x] `Value() uint64` - sum of all slots
- [x] `Clone() *GCounter` - deep copy for delta calculation
- [x] `DeltaSince(other *GCounter) *GCounter` - compute delta

### 2.2 Epoch Window with CRDT (`crdt/epoch_window.go` refactor)
- [x] Replace `int64` bucket with `*GCounter`
- [x] `Add(nodeID string, count int64)`
- [x] `AddAtEpoch(nodeID string, count int64, epoch int64)`
- [x] `Merge(other *EpochWindow)` - merge per-epoch GCounters
- [x] `Value() uint64` - sum across window
- [x] `DeltaSince(lastEpoch int64) *EpochWindow` - delta for gossip

### 2.3 Property-Based Tests (`crdt/counter_test.go`)
- [x] Use `github.com/leanovate/gopter` for property testing
- [x] Test merge commutativity: `Merge(a, b) == Merge(b, a)`
- [x] Test associativity: `Merge(Merge(a,b),c) == Merge(a,Merge(b,c))`
- [x] Test idempotency: `Merge(a,a) == a`
- [x] Test with random merge orderings (1000+ iterations)
- [x] Run with `-race` flag

### 2.4 Policy Layer (`limiter/policy.go`, `limiter/policy_test.go`)
- [ ] `Policy`: limit, epochSizeMs, windowEpochs, costPerRequest
- [ ] `Allow(clientID string, cost int64) (allowed bool, remaining int64)`
- [ ] Per-client `EpochWindow` management with LRU eviction
- [ ] Unit tests: limit enforcement, burst handling, cost weighting

---

## Phase 3a: Gossip Transport Only (Week 2) - ✅ DONE

### 3a.1 Proto Extensions (`proto/ratelimiter.proto`)
- [x] Add gossip messages: Ping, Ack, StateDelta, FullState
- [x] Add `Gossip` service with bidirectional streaming RPC

### 3a.2 Transport Layer (`gossip/transport.go`)
- [x] `Transport` interface: `Send(ctx, peerAddr, msg)`, `Receive() <-chan Msg`
- [x] gRPC implementation using bidirectional streaming
- [x] Connection pooling per peer
- [x] Retry with exponential backoff
- [x] Health check / liveness probe endpoint

### 3a.3 Test (`gossip/transport_test.go`)
- [x] Two hardcoded nodes exchange Ping/Ack
- [x] Verify message delivery with `-race`
- [x] Connection recovery after temporary network blip
- [x] Concurrent bidirectional messaging

---

## Phase 3b: Membership (SWIM-lite) (Week 2-3) - ✅ DONE

### 3b.1 Membership State (`gossip/membership.go`)
- [x] `Member`: NodeID, Address, Incarnation, Status (Alive/Suspect/Dead), LastSeen
- [x] `Membership` struct with `sync.RWMutex` protected map
- [x] `Add(node)`, `Update(node)`, `Remove(nodeID)`, `GetAlive() []Member`
- [x] `MarkSuspect(nodeID)`, `ConfirmDead(nodeID)`, `Revive(nodeID)`

### 3b.2 SWIM Protocol Loop
- [x] Periodic tick (configurable interval, default 200ms)
- [x] Select random k peers (k=3) from alive members
- [x] Send Ping, await Ack with timeout
- [x] On timeout: send indirect ping via another k peers
- [x] Suspect → Dead transition after suspicion timeout (default 2*interval)
- [x] Incarnation number for detecting restarts

### 3b.3 Join/Leave
- [x] Bootstrap: contact seed peers, merge membership
- [x] Voluntary leave: broadcast Leave, remove self
- [x] Rejoin with incremented incarnation

### 3b.4 Tests (`gossip/membership_test.go`)
- [x] `-race` test: concurrent ping/ack, suspect→dead transitions
- [x] Partition simulation: split cluster, verify both sides detect failure
- [x] Rejoin after partition: incarnation increment, clean merge
- [x] Property: no split-brain (max 1 incarnation per nodeID)

---

## Phase 3c: CRDT Dissemination (Week 3)

### 3c.1 Delta Gossip (`gossip/dissemination.go`)
- [ ] `Disseminator` struct with membership reference, transport, local state
- [ ] Track `lastSentEpoch` per peer for delta calculation
- [ ] On tick: select random fanout (3-5) from alive peers
- [ ] Send `StateDelta` containing epochs > `lastSentEpoch[peer]`
- [ ] On receive: `Merge(delta)`, update `lastSentEpoch[sender]`
- [ ] Periodic full-state sync (every N deltas) for safety

### 3c.2 Integration (`node/server.go`)
- [ ] Wire disseminator into server lifecycle
- [ ] `Allow` RPC reads from local CRDT state (no blocking)
- [ ] Background goroutine for dissemination tick

### 3c.3 Docker Compose (`docker-compose.yml`)
```yaml
services:
  node1: build: .; environment: NODE_ID=node1, PEERS=node2:50051,node3:50051
  node2: build: .; environment: NODE_ID=node2, PEERS=node1:50051,node3:50051
  node3: build: .; environment: NODE_ID=node3, PEERS=node1:50051,node2:50051
  redis: image: redis:7-alpine
```

### 3c.4 E2E Test (`gossip/dissemination_test.go` + integration test)
- [ ] Start 3-node cluster via docker-compose
- [ ] Send concurrent traffic to different nodes
- [ ] Verify convergence: all nodes report similar counts within bound
- [ ] Measure convergence time (gossip intervals)

---

## Phase 4: Partition Tolerance & Failure Handling (Week 3-4)

### 4.1 Partition Simulation
- [ ] `scripts/partition_test.go`: Use Docker network commands or Linux `tc`/`iptables`
- [ ] Split cluster into 2 partitions for configurable duration
- [ ] Automated test harness: start traffic → partition → heal → verify convergence

### 4.2 Partition Behavior Verification
- [ ] Each partition continues serving requests (availability)
- [ ] Local limits enforced independently (per-partition limit = global limit)
- [ ] No request blocked due to partition

### 4.3 Convergence Test (`benchmark/partition_test.go`)
- [ ] Measure: time from partition heal → all nodes consistent
- [ ] Measure: max overshoot during partition
- [ ] Document as CAP tradeoff: AP during partition, eventual consistency after

---

## Phase 5: Benchmarking vs Redis Baseline (Week 4)

### 5.1 Redis Baseline (`baseline/redis_limiter.go`)
- [ ] Token bucket in Redis (Lua script for atomicity)
- [ ] gRPC-compatible `Allow` interface
- [ ] Simple, no clustering - single Redis instance

### 5.2 Latency Benchmark (`benchmark/latency_test.go`)
- [ ] CRDT: local decision latency (p50, p99, p999)
- [ ] Redis: round-trip latency (p50, p99, p999)
- [ ] Concurrent load: 100, 1000, 10000 RPS
- [ ] Report: added latency overhead of CRDT vs Redis

### 5.3 Accuracy Benchmark (`benchmark/accuracy_test.go`)
- [ ] Parameters: gossip interval (100ms, 500ms, 1s), nodes (3, 5, 10)
- [ ] Metric: max overshoot = max(observed - limit) / limit
- [ ] Compare measured vs theoretical bound: `overshoot ≤ (gossip_interval / epoch_size) * limit * nodes`
- [ ] Table output for README

### 5.4 Partition Benchmark (`benchmark/partition_test.go`)
- [ ] Partition duration vs convergence time
- [ ] Overshoot during partition

### 5.5 Report Generator (`benchmark/report.go`)
- [ ] Generate Markdown tables for README
- [ ] JSON output for CI artifact

---

## Phase 6: Observability, CI, Docs, Deploy (Week 4-5)

### 6.1 Metrics (`node/metrics.go`)
- [ ] Prometheus metrics:
  - `ratelimit_allow_total{result="allowed|denied", node}`
  - `ratelimit_decision_latency_seconds{node}`
  - `gossip_round_latency_seconds{node}`
  - `gossip_convergence_seconds{node}` (after partition heal)
  - `ratelimit_estimated_overshoot{node}`

### 6.2 CI Pipeline (`.github/workflows/ci.yml`)
```yaml
jobs:
  lint: golangci-lint run ./...
  test: go test -race -count=1 ./...
  build: go build ./...
```

### 6.3 README.md
- [ ] Architecture diagram (Mermaid)
- [ ] CRDT/Gossip design rationale
- [ ] Accuracy bound formula + empirical validation table
- [ ] Benchmark results vs Redis (latency/accuracy/availability)
- [ ] Partition behavior + CAP tradeoff documentation
- [ ] Quickstart: `docker-compose up`
- [ ] Future work section

### 6.4 Load Test Script (`scripts/load_test.go`)
- [ ] Configurable: target nodes, RPS, duration, client ID distribution
- [ ] Report: throughput, latency percentiles, error rate
- [ ] Can target CRDT nodes or Redis baseline

### 6.5 Dockerfile
- [ ] Multi-stage build (builder → runtime)
- [ ] Non-root user
- [ ] Health check endpoint

---

## Dependencies (go.mod)

```go
module github.com/sakthivel-p-cse/crdt-rate-limiter

go 1.26

require (
    github.com/leanovate/gopter v1.1.0        // property testing
    github.com/prometheus/client_golang v1.19.0
    github.com/redis/go-redis/v9 v9.3.0
    github.com/stretchr/testify v1.9.0
    go.uber.org/zap v1.26.0
    google.golang.org/grpc v1.62.0
    google.golang.org/protobuf v1.33.0
    gopkg.in/yaml.v3 v3.0.1
)
```

---

## Milestones & Checkpoints

| Milestone | Deliverable | Validation |
|-----------|-------------|------------|
| M1 | Single-node gRPC rate limiter with epoch window | `go test ./... -race` passes |
| M2 | CRDT G-Counter + property tests | Property tests pass, merge laws verified |
| M3a | Two nodes exchange gossip messages | Transport test passes |
| M3b | 3-node cluster detects join/leave/failure | Membership test passes with `-race` |
| M3c | 3-node cluster converges under load | E2E test shows bounded overshoot |
| M4 | Partition simulation + convergence measurement | `benchmark/partition_test.go` produces metrics |
| M5 | Benchmark report vs Redis | README tables generated |
| M6 | CI green, docs complete, docker-compose up works | All checks pass |

---

## Risk Areas & Mitigations

| Risk | Mitigation |
|------|------------|
| Gossip concurrency bugs | Phase 3 split into 3a/3b/3c; `-race` at each step; property tests on merge |
| Partition split-brain | Document AP choice explicitly; test both partitions serve; verify convergence |
| CRDT merge correctness | Property-based testing (commutativity, associativity, idempotency) |
| Clock drift / epoch sync | Use logical epochs (counter), not wall clock; monotonic increment |
| Redis baseline complexity | Keep minimal - single instance, Lua script token bucket only |

---

## Clarifying Questions for User

1. **Go module path**: Use `github.com/sakthivel-p-cse/crdt-rate-limiter` or different?
2. **gRPC port**: Default 50051 for all nodes (different host ports in docker-compose)?
3. **Default limits**: 1000 req/epoch? 100ms epoch? 10-epoch window? (need sensible defaults)
4. **Gossip interval**: 200ms default? Fanout 3?
5. **Benchmark duration**: 30s per run? 60s?
6. **Partition simulation**: Prefer Docker network manipulation (`docker network disconnect`) or Linux `tc`/`iptables`?
7. **Metrics endpoint**: `/metrics` on separate port (e.g., 9090) or same gRPC port?
8. **CI runner**: GitHub Actions (ubuntu-latest) - any specific requirements?

---

## Next Steps

Once you confirm the plan and answer clarifying questions, I'll start with **Phase 1**:
1. Initialize `go.mod`
2. Create `proto/ratelimiter.proto`
3. Generate Go code
4. Implement `crdt/epoch_window.go` (local version)
5. Implement `node/server.go` + `node/main.go` + `node/config.go`
6. Write tests

Let me know if you want any adjustments to the plan or if I should proceed with Phase 1!
