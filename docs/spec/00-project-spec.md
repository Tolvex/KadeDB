# KadeDB Project Spec (SSOT)

Created: 2026-07-27

## Purpose

Fix the "what is correct" answers for product behavior that isn't already pinned down by
existing code, tests, or docs. Currently scoped to the decisions needed for Plans.md Phase 1
(Distributed Scalability) — see `TODO.md` (#5) for background on why this is still unimplemented.

## Users And Workflows

- Operators deploying KadeDB across multiple nodes for higher throughput or larger datasets
  than a single node can hold.
- Existing single-node users must be unaffected: distributed mode is additive, not a rewrite of
  the single-node storage engine (`InMemoryRelationalStorage` etc. in `cpp/src/core/storage.cpp`).

## Core Rules

- (Open — see Open Decisions)

## Data And Contracts

- (Open — see Open Decisions)

## Non-Goals

- Multi-region / geo-replication (out of scope for the first distributed milestone).
- Automatic online resharding (a static shard count is acceptable for the first milestone).

## Open Decisions

| Decision | Options | Status |
|----------|---------|--------|
| Sharding strategy | hash-based vs range-based | Undecided — resolve in Task 1.1-spike |
| Replication model | leader-follower vs multi-leader | Undecided — resolve in Task 1.1-spike |
| Consensus protocol | Raft (self-implemented) vs external coordinator (e.g. etcd) | Undecided — resolve in Task 1.1-spike |

## Links

- `TODO.md` (#5 Distributed Scalability)
- `Plans.md` (Phase 1)
