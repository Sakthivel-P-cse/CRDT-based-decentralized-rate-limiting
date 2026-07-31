# CRDT Rate Limiter V2 Context

## Completed implementation

- Added priority tiers: Critical, Standard, and Best Effort.
- Added tier shedding at 70%, 90%, and 100% utilization bands.
- Added probabilistic soft rejection using `RejectProbability` and `math/rand/v2`.
- Added thread-safe AIMD adaptive budgets with 50% decrease, 5% clean-epoch growth, 10% minimum, and original-limit maximum.
- Added adaptive gossip intervals with a 20% floor and utilization-change timer rescheduling.
- Added shared `gossip.ConvergenceRounds` constant and duration-based Retry-After responses.
- Extended protobuf requests/responses with priority, rejection reason, and Retry-After duration.
- Added `SOFT_THRESHOLD_RATIO` executable configuration and README documentation.
- Added focused decision tests and `tests/gradual_degradation_test.go` covering partition priority behavior, traffic-ramp smoothness, and failure/recovery AIMD behavior.

## Important behavior

- Allow decisions remain local and non-blocking.
- Accepted local counts are stored per client, epoch, and node ID.
- Gossip sends deltas newer than each peer's last successful send timestamp; every tenth round sends a full sync.
- Gossip selects up to three random reachable peers and sends concurrently through gRPC.
- Incoming counts merge through per-node CRDT maximums, making duplicate gossip idempotent.
- Retry-After is based on the node-wide maximum utilization, current adaptive gossip interval, four convergence rounds, and a 50ms safety margin.

## Verification

- Baseline `go test ./...` passed before changes.
- Final `go test ./...` passed.
- Final uncached `go test -race -count=1 ./...` passed.
- `go vet ./...` passed.
- `git diff --check` passed.
- Strict comment-syntax grep across all touched files returned no results.
- Fresh independent review approved the current implementation with no blocker or important findings.

## Workspace notes

- Work is on `agent/crdt-rate-limiter-closeout`.
- Changes are uncommitted because `.git` is read-only in this environment.
- Remote fetch and publication were blocked by the environment's Git permissions.
- Existing `codex-session.txt` was preserved; this file is the project handoff context.
