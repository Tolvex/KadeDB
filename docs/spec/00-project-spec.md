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

- Exactly one leader owns writes for a given shard at any time; followers are read replicas
  until explicitly promoted during failover. Multi-leader writes are not supported.
- Relational and time-series tables are sharded by range (not hash). Document storage and graph
  storage are **not sharded** in the first milestone — both remain single-node.
- Cluster membership and leader election are delegated to an external coordinator (etcd), not to
  a self-implemented consensus protocol. Per-shard data replication (WAL shipping from leader to
  followers) is implemented directly in KadeDB, independent of etcd.

## Data And Contracts

- Each shard is a leader-follower group; the write-ahead log format and the leader→follower
  replication RPC are new additions at the C++ core / Rust services boundary. The concrete wire
  format is **not yet defined** — Task 1.2 must specify it before Task 1.5 (data replication) can
  start.
- Cluster membership/leader-election data (which node is leader for which shard) lives in etcd,
  keyed by shard id; KadeDB's own storage layer does not need to be etcd-aware beyond the Rust
  services layer that queries/watches it.

## Non-Goals

- Multi-region / geo-replication (out of scope for the first distributed milestone).
- Automatic online resharding (a static shard count is acceptable for the first milestone).
- (Multi-leader replication and graph/document sharding are already excluded — see Core Rules
  above; not restated here to avoid two sources of truth that can drift apart.)

## Open Decisions

Task 1.1-spike recommends the approach below; **Task 1.2 still has to finalize the concrete
design** (see its DoD in `Plans.md`) — the "Chosen" column is a direction, not a completed design.

| Decision | Options | Chosen (Task 1.1-spike recommendation) |
|----------|---------|-----------------------------------------|
| Sharding strategy | hash-based vs range-based | Range-based (relational/time-series only; document & graph unsharded) |
| Replication model | leader-follower vs multi-leader | Leader-follower per shard |
| Consensus protocol | Raft (self-implemented) vs external coordinator (e.g. etcd) | External coordinator (etcd) for membership/leader-election; WAL replication is still hand-rolled |

## Validation Notes (Task 1.1-spike, 2026-07-27)

Findings are grounded in the current codebase, not generic distributed-systems advice:

- **No existing networking, persistence, or WAL code.** A repo-wide search
  (`grep -rniE "write.ahead|\bwal\b|persist|network|socket|grpc|tcp"` over `cpp/include` and
  `cpp/src`) returns zero matches. Replication and consensus are greenfield — there is nothing to
  extend, which is why a self-implemented Raft (leader election, log truncation, snapshotting,
  split-vote handling) is high risk relative to team size and isn't KadeDB's core differentiator
  (multi-model storage/query is). An external coordinator trades an operational dependency (an
  etcd cluster) for far less custom consensus code, and the Rust `services/` layer already has
  mature etcd client crates available.
- **Storage is already single-writer.** `cpp/include/kadedb/storage.h` (`InMemoryRelationalStorage`,
  `InMemoryDocumentStorage`) and `cpp/src/core/timeseries_storage.cpp`/`graph_storage.cpp` all guard
  every mutation with one `std::mutex` per store (`storage.h:316`, `:348`; `lock_guard<std::mutex>`
  throughout `timeseries_storage.cpp`/`graph_storage.cpp`). Leader-follower is a direct extension of
  this: one leader still owns the mutex-protected store; followers replay a replicated log. Multi-leader
  would require conflict resolution (CRDTs/vector clocks) that doesn't exist anywhere in the code and
  would change today's single-node write semantics — rejected for the first milestone.
  See also `docs/design-notes.md` on `Row` vs `RowShallow` copy semantics, which any WAL/replication
  encoding (Task 1.2) will need to account for.
- **Range-based sharding matches existing query shapes.** KadeQL and the storage layer already
  optimize ordered/range access: `InMemoryTimeSeriesStorage::rangeQuery`, `TIME_BUCKET`/`FIRST`/`LAST`
  aggregation, and `WHERE timestamp BETWEEN ... AND ...`. Range-based sharding lets one shard answer
  a bounded key/time range without a scatter-gather fan-out; hash-based sharding would turn every
  such query into an all-shards fan-out. Document storage (keyed by opaque document id) and graph
  storage (adjacency-based, no natural range key) don't fit this model — both are excluded from
  sharding in the first milestone rather than forcing a poor-fit partitioning scheme.

**Follow-ups for later tasks (not resolved here):**

- Task 1.2 must pick the concrete etcd client crate/API and define the WAL wire format and the
  leader→follower replication RPC.
- Task 1.5 must decide the failover detection mechanism built on etcd leases (e.g. lease TTL
  expiry triggering election) — not designed here beyond "etcd provides the primitive."

## Links

- `TODO.md` (#5 Distributed Scalability)
- `Plans.md` (Phase 1)
