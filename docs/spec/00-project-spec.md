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
  format and RPC schema are **finalized in Task 1.2** — see "Task 1.2 Design" below.
- Cluster membership/leader-election data (which node is leader for which shard) lives in etcd,
  keyed by shard id; KadeDB's own storage layer does not need to be etcd-aware beyond the Rust
  services layer that queries/watches it.

## Non-Goals

- Multi-region / geo-replication (out of scope for the first distributed milestone).
- Automatic online resharding (a static shard count is acceptable for the first milestone).
- (Multi-leader replication and graph/document sharding are already excluded — see Core Rules
  above; not restated here to avoid two sources of truth that can drift apart.)
- Mixed-endianness clusters in the first milestone: WAL entry *payloads* reuse
  `serialization.h`'s existing host-native-byte-order primitives (see "Task 1.2 Design" below), so
  node pools are assumed to share a homogeneous architecture. Only the WAL entry header is made
  wire-endian-safe.

## Open Decisions

Task 1.1-spike recommended the approach below as a direction; **Task 1.2 finalizes it into a
concrete design** (see its DoD in `Plans.md` and the "Task 1.2 Design" section below).

| Decision | Options | Chosen |
|----------|---------|--------|
| Sharding strategy | hash-based vs range-based | Range-based (relational/time-series only; document & graph unsharded) |
| Replication model | leader-follower vs multi-leader | Leader-follower per shard |
| Consensus protocol | Raft (self-implemented) vs external coordinator (e.g. etcd) | External coordinator (etcd) for membership/leader-election; WAL replication is still hand-rolled |
| WAL wire format | new ad-hoc codec vs reuse `serialization.h` framing | Reuse `bin::writeValue`'s per-field encoding for payloads, wrapped in a new WAL-specific header (see below) |
| Replication RPC | REST polling vs gRPC streaming | New tonic/prost streaming RPC (`ReplicationService.StreamWal`), alongside the existing `QueryService` |
| etcd client crate | `etcd-client` vs `etcd-rs` vs hand-rolled gRPC client | `etcd-client` (0.14.x line) |

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

- ~~Task 1.2 must pick the concrete etcd client crate/API and define the WAL wire format and the
  leader→follower replication RPC.~~ Done — see "Task 1.2 Design" below.
- Task 1.5 must decide the failover detection mechanism built on etcd leases (e.g. lease TTL
  expiry triggering election) — not designed here beyond "etcd provides the primitive."

## Task 1.2 Design (2026-07-29)

Finalizes the WAL wire format, leader→follower replication RPC schema, and etcd client choice left
open by Task 1.1-spike. Grounded in the actual repo (not generic distributed-systems advice), and
reviewed with the harness advisor before being recorded here; corrections from that review are
folded in below.

### WAL entry wire format

A WAL entry is a fixed header followed by an entry-type-specific payload:

```
WalEntryHeader (all multi-byte fields little-endian ON THE WIRE, explicitly — see note below):
  uint32 magic          // 0x4B57414C ('KWAL'); distinct from serialization_constants::MAGIC
                         // ('KDBV') so WAL framing is never confused with the general
                         // value/row/document persistence format
  uint8  version        // WAL format version, starts at 1
  uint64 shard_id
  uint64 sequence       // monotonic, gap-free per-shard LSN, starts at 1
  uint64 term           // leader term/epoch at time of append
  uint8  entry_type     // see WalEntryType below
  uint32 payload_len
  uint32 crc32c         // checksum over payload bytes only

payload: bytes[payload_len]  // see per-entry_type encoding below
```

Entry types and payload encoding — payloads reuse `bin::writeValue` (the per-cell encoder in
`cpp/src/core/serialization.cpp`, which deliberately omits the MAGIC/VERSION header) rather than
`bin::writeRow`/`bin::writeDocument`, because those two already emit their own MAGIC+VERSION header
per call; nesting that inside a WAL entry that already carries its own header/version/checksum
would duplicate framing and create two version fields to keep in sync:

- `RELATIONAL_ROW_UPSERT`: `table_name` (length-prefixed string), primary key column values
  (count + `writeValue` each), then the full **resulting** row — column count + `writeValue`
  per cell with an is-null byte, matching the per-cell layout `bin::writeRow` uses internally
  minus its header.
- `RELATIONAL_ROW_DELETE`: `table_name`, primary key column values (`writeValue` each).
- `TIME_SERIES_POINT_APPEND`: `table_name`, resolved point (`timestamp` + `writeValue` per
  column) — time-series is append-only, so no update/delete entry type is needed for it.
- `NO_OP`: empty payload; periodic keepalive so followers can advance `sequence` and detect
  leader liveness independent of etcd lease TTL.

**No document/graph entry types**: per Core Rules, document and graph storage are unsharded and
remain single-node in the first milestone — there is no leader-follower group, and therefore no
WAL replication, for either. Only relational and time-series storage (the two that are sharded)
produce WAL entries. If a later milestone replicates document storage for HA (still single-shard,
just no longer single-copy), that adds `DOCUMENT_UPSERT`/`DOCUMENT_DELETE` entry types at that
point — not needed now and deliberately left out to avoid contradicting the "remains single-node"
Core Rule.

**UPDATE/DELETE entries are physical, not logical**: they carry the row/document identity plus
the post-mutation resulting state, not the original predicate + assignment. A logical entry would
require the follower to have byte-identical pre-mutation state to re-resolve the predicate, which
breaks under any replication lag or partial-apply scenario. The C++ core resolves the predicate
against the mutex-held live state at commit time and emits the already-resolved values — the WAL
producer must build these from `Value::clone()`/`Row::toRowDeep()`-style deep copies (per
`docs/design-notes.md`'s `Row` vs `RowShallow` distinction) rather than holding onto `RowShallow`'s
aliased `shared_ptr` cells past the mutex-protected critical section that generates the entry.

**Endianness note**: `serialization.h`'s existing `writeU8`/`writeU32`/`writeI64`/`writeF64` write
host-native byte order via `reinterpret_cast` with no `htole`/`letoh` conversion (confirmed in
`cpp/src/core/serialization.cpp`) — fine for today's single-node persistence (same host reads what
it wrote), but not safe to reuse verbatim for the WAL header once entries cross the network. The
WAL header's multi-byte fields (`magic`, `shard_id`, `sequence`, `term`, `payload_len`, `crc32c`)
must be written in explicit little-endian wire order — new endian-safe helpers, distinct from
`serialization.h`'s host-native ones, to be added in Task 1.5. WAL entry *payloads* still reuse
`writeValue`'s existing host-native per-field encoding, which is why mixed-endianness clusters are
out of scope for the first milestone (see Non-Goals).

### Leader→follower replication RPC

A new tonic/prost streaming service, alongside the existing `QueryService` in
`services/proto/kadedb.proto` (or a new `replication.proto` imported into the same build), using
the tonic 0.12 / prost 0.13 versions already pinned in `services/grpc/Cargo.toml`:

```protobuf
service ReplicationService {
  // Follower opens one long-lived stream per shard, starting after `since_sequence`.
  // Never terminates on success; NO_OP entries serve as keepalive.
  rpc StreamWal(WalStreamRequest) returns (stream WalEntry);
}

message WalStreamRequest {
  uint64 shard_id = 1;
  uint64 since_sequence = 2; // 0 = from the start of the retained WAL
  uint64 leader_term = 3;    // follower's last-known term, for staleness detection
  uint32 format_version = 4; // follower's supported WAL format version; leader closes the
                              // stream immediately on a major-version mismatch instead of
                              // negotiating per-entry
}

message WalEntry {
  uint64 shard_id = 1;
  uint64 sequence = 2;
  uint64 term = 3;
  WalEntryType entry_type = 4;
  bytes payload = 5;         // encoded per the WAL wire format above
  uint32 payload_crc32c = 6; // same checksum as the on-disk WalEntryHeader.crc32c, carried
                              // over so followers can verify payload integrity end-to-end
                              // independent of transport-level checks; not a second,
                              // independently-computed value
}

enum WalEntryType {
  WAL_ENTRY_UNSPECIFIED = 0;
  RELATIONAL_ROW_UPSERT = 1;
  RELATIONAL_ROW_DELETE = 2;
  TIME_SERIES_POINT_APPEND = 3;
  NO_OP = 4;
}
```

**Layering**: WAL entries are generated inside the C++ core (`kadedb_core`) at the point of
mutation, because that is the only place with mutex-protected access to the authoritative
in-memory state needed to resolve physical post-mutation rows/documents. A new C ABI surface
(`bindings/c`) exposes a pull-based cursor — e.g. `KadeDB_WAL_OpenCursor`/`_CursorNext`/`_CursorClose`
— rather than a push callback, so backpressure flows naturally: the Rust `ffi` crate wraps the
cursor as a safe Rust iterator/`Stream`, and the `grpc` crate's `ReplicationService` adapts that
stream directly into the tonic streaming response, pulling only as fast as the gRPC stream has
capacity. Followers consume the tonic stream and apply each `WalEntry` via a new C ABI apply
function (`KadeDB_WAL_ApplyEntry`) against their own local mutex-protected storage; a follower
never originates writes to a shard it doesn't lead.

### etcd client crate

`etcd-client` (0.14.x line) — the de facto standard async Rust etcd v3 client, used only from the
Rust services layer (etcd-awareness stays out of C++/the C ABI, per the existing Core Rules
decision). APIs used: `Client::connect` (endpoint list), `lease_grant`/`lease_keep_alive`
(liveness leases), the election client's `campaign`/`proclaim` (per-shard leader election, so
leader election is delegated to etcd's built-in recipe rather than hand-rolled on top of raw etcd,
consistent with Task 1.1-spike's consensus-protocol decision), and `watch` (so the routing layer
can observe leader changes).

**Risk flag**: `etcd-client` 0.14.x pins tonic 0.12, which matches this workspace's currently
pinned tonic 0.12 — no conflict today. This is a coupled-upgrade constraint, not a rejection
reason: bumping tonic in `services/` in the future requires a matching `etcd-client` release (or a
vendored patch) to land first.

### Task 1.3 Design (2026-07-30)

Cluster membership implementation, in the new `services/cluster` crate. Fills in the concrete
details Task 1.1/1.2 left to later tasks; does not revisit the etcd-vs-self-implemented decision.

- **Key schema**: each node PUTs its `NodeInfo { node_id, address }` (JSON-encoded — this is etcd
  membership metadata, not WAL replication data, so it does not reuse `serialization.h`'s
  `writeValue` framing from the Task 1.2 WAL design) under `/kadedb/cluster/nodes/<node_id>`,
  bound to a lease (`PutOptions::with_lease`). `members()` lists the prefix; `watch()` watches it
  and maps etcd `Put`/`Delete` events to `MembershipEvent::Joined`/`Left`.
- **Liveness**: a background task calls `lease_keep_alive` roughly every TTL/3 (etcd's own
  recommended cadence, so a couple of missed ticks don't immediately expire the node). Default TTL
  is 5s, configurable via `MembershipConfig`.
- **Join/leave semantics**: `leave()` is graceful — it stops the heartbeat and immediately
  `lease_revoke`s, deleting the key right away. Crash/partition is simulated in tests (and occurs
  for real on an actual crash) by the heartbeat simply stopping without a revoke: the key is only
  removed once etcd expires the lease, which is what makes heartbeat failure "observable" per this
  task's DoD — proven against a real etcd instance (`gcr.io/etcd-development/etcd`) in
  `services/cluster/tests/membership.rs`'s `#[ignore]`-gated integration tests.
- **Build prerequisite**: `etcd-client`'s build script shells out to `protoc` directly (unlike
  `services/grpc`'s `build.rs`, which vendors it via `protoc-bin-vendored`). CI now installs
  `protobuf-compiler`; local dev needs it too (or `PROTOC` pointed at a protoc binary).

### Task 1.4 Design (2026-07-31)

Distributed query execution, in a new `services/router` crate. Scopes "query routing to shards,
distributed aggregation (map-reduce style), result merging" (Plans.md Task 1.4 DoD) against what
Task 1.2/1.3 actually built so far — not a restatement of later tasks' scope.

- **Shard topology is static and single-node-per-shard for this milestone.** Task 1.5 ("failover
  and leader election") is what introduces followers and dynamic leader promotion; Task 1.4 has no
  dependency on that landing first, because with no replicas yet, "the leader of a shard" and "the
  shard's one node" are the same thing. `ShardTopology`/`ShardInfo` in `services/router` are a
  caller-supplied, static `{shard_id, range_start, range_end, address}` list — not read from etcd —
  consistent with `services/cluster/src/lib.rs`'s own comment that shard routing is out of its
  scope. A later task can source this topology from etcd once per-shard leader keys exist; that
  swap is confined to how `ShardTopology` is constructed, not to `QueryRouter`/`merge_rows`.
- **Routing narrows by range, but is a heuristic, not a KadeQL parse.** The KadeQL parser lives in
  C++ (`cpp/include/kadedb/kadeql_parser.h`) with no Rust binding; re-implementing it in Rust just
  to extract a `WHERE` bound is disproportionate to this task. `services/router` instead scans the
  query text for a recognized bound on the topology's `shard_key_column` (`col >= N`, `col > N`,
  `col <= N`, `col < N`, `col = N`, `col BETWEEN a AND b`, conjoined with `AND`) and prunes to
  shards whose `[range_start, range_end)` overlaps the recognized bound. When no bound is
  recognized (or the query predicate is more complex than this heuristic covers), routing falls
  back to every shard — always correct, just not minimal. This mirrors the DoD's "query routing to
  shards," while leaving precise cross-language predicate extraction as a follow-up if it turns out
  to matter (e.g. a future C ABI "extract predicate bounds" helper) rather than solving it now.
- **"Map-reduce style" aggregation is scoped to what KadeQL actually supports.** KadeQL's only
  aggregate functions are `TIME_BUCKET`/`FIRST`/`LAST` (`cpp/src/core/query_executor.cpp:680-681`)
  — there is no `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` to combine. `services/router` exposes a
  `MergeStrategy` the caller selects for its own query shape: `Concat` (plain `SELECT`s — shard
  rows appended in shard-id order) or `TimeBucketFirst`/`TimeBucketLast { bucket_key }` for
  `TIME_BUCKET(...) ... FIRST(...)`/`LAST(...)` queries. `bucket_key` (the JSON field name to group
  rows by) is caller-supplied rather than inferred from the row JSON, because JSON object field
  order is not guaranteed by `serde_json` and the router has no independent way to know which field
  is the bucket column.
  - **Merge rule for `FIRST`/`LAST` exploits range-sharding, not per-row timestamps.** Because
    shards own disjoint, ordered key ranges, a bucket that appears in more than one shard's result
    (only possible at a shard boundary) has its true global `FIRST` in the *lowest*-`shard_id`
    shard that produced it, and its true global `LAST` in the *highest*-`shard_id` shard that
    produced it — no cross-shard timestamp comparison needed, since shard order already reflects
    key/time order. Buckets present in only one shard pass through unchanged.
- **`QueryRouter` talks to the existing `QueryService` gRPC contract, whatever is behind it.**
  `services/grpc`'s `QueryServiceImpl::query` is currently a stub (canned/echoed rows, not wired to
  `kadedb_ffi`) — a pre-existing gap from earlier scaffolding, not something Task 1.4 introduces or
  is responsible for closing. The router is written against the tonic `QueryServiceClient`
  interface; when a later task wires `QueryServiceImpl` to real `kadedb_ffi::Storage` execution,
  routing and merging work unchanged. `QueryServiceImpl` gained a `with_rows` constructor (default
  behavior unchanged) purely so its own crate's tests, and this task's multi-node integration test,
  can give each simulated shard distinguishable canned data.

## Links

- `TODO.md` (#5 Distributed Scalability)
- `Plans.md` (Phase 1)
