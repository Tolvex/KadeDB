# KadeDB TODO

This file tracks discrepancies between the README claims and the current codebase implementation.

---

## Unimplemented Features

### Storage Models

| Feature | README Claim | Status |
|---------|--------------|--------|
| **Time-series storage** | "Time-series storage for temporal data (e.g., sensor readings, market ticks)" | ❌ Not implemented |
| **Graph storage** | "Graph storage for network data (e.g., networks, relationships)" | ❌ Not implemented |

### Components

| Component | README Claim | Status |
|-----------|--------------|--------|
| **KadeDB Services (Rust)** | "Secure, async service layer (REST/gRPC), connectors, auth/RBAC, and orchestration built with Rust" | ❌ No Rust code found in codebase |
| **GPU acceleration** | "High-performance engine with GPU acceleration" | ❌ No CUDA/GPU code found |
| **Distributed scalability** | "distributed scalability for compute-intensive analytics" | ❌ Not implemented |

---

## Action Items

### High Priority

#### 1. Time-Series Storage Model

- [x] **Design time-series schema**
  - [x] Define `TimeSeriesSchema` class with timestamp column, value columns, and optional tags
  - [x] Support configurable time granularity (nanoseconds, milliseconds, seconds)
  - [x] Design retention policies (TTL, max rows)
- [x] **Implement time-series storage engine**
  - [x] Create `TimeSeriesStorage` class in `cpp/include/kadedb/timeseries/`
  - [x] Implement time-partitioned storage (hourly/daily buckets)
  - [x] Add efficient range queries by time window
  - [x] Implement downsampling/aggregation (avg, min, max, sum, count)
- [x] **Add time-series query support to KadeQL**
  - [x] Extend parser for `SELECT ... WHERE timestamp BETWEEN ... AND ...`
  - [x] Add time-based aggregation functions (`TIME_BUCKET()`, `FIRST()`, `LAST()`)
- [x] **Create tests and examples**
  - [x] Unit tests in `cpp/test/timeseries_test.cpp`
  - [x] Example in `cpp/examples/timeseries_example.cpp`
- [x] **Update documentation**
  - [x] Add `docs/sphinx/timeseries_api.rst`
  - [x] Update README to reflect implementation status

#### 2. Graph Storage Model

- [x] **Design graph schema**
  - [x] Define `Node` and `Edge` types with property support
  - [x] Design index structures for adjacency lookups
  - [x] Support labeled edges and typed relationships
- [x] **Implement graph storage engine**
  - [x] Create `GraphStorage` class in `cpp/include/kadedb/graph/`
  - [x] Implement node/edge CRUD operations
  - [x] Add adjacency list storage with efficient neighbor lookups
  - [x] Implement basic traversal algorithms (BFS, DFS)
- [x] **Add graph query support**
  - [x] Design query syntax for path traversal (e.g., Cypher-like or custom)
  - [x] Implement `MATCH` patterns for subgraph queries
  - [x] Add shortest path and connectivity queries
- [x] **Create tests and examples**
  - [x] Unit tests in `cpp/test/graph_test.cpp`
  - [x] Example in `cpp/examples/graph_example.cpp`
- [x] **Update documentation**
  - [x] Add `docs/sphinx/graph_api.rst`
  - [x] Update README to reflect implementation status

#### 3. Rust Services Layer

- [x] **Set up Rust project structure**
  - [x] Create `services/` directory with `Cargo.toml`
  - [x] Configure workspace in root or standalone crate
  - [x] Set up CI for Rust builds (add to `.github/workflows/ci.yml`)
- [x] **Implement REST API service**
  - [x] Use `axum` or `actix-web` framework
  - [x] Create endpoints: `POST /query`, `GET /health`, `POST /tables`
  - [x] Add JSON serialization for query results
- [x] **Implement gRPC service**
  - [x] Define `.proto` files in `services/proto/`
  - [x] Generate Rust bindings with `tonic`
  - [x] Implement streaming query results
- [x] **Add authentication and RBAC**
  - [x] Implement JWT-based authentication
  - [x] Define role-based permissions (read, write, admin)
  - [x] Add middleware for auth enforcement
- [x] **FFI bridge to C++ core**
  - [x] Use `cxx` or `bindgen` to call C ABI from Rust
  - [x] Wrap storage operations in async Rust tasks
- [x] **Create tests and examples**
  - [x] Integration tests for REST/gRPC endpoints
  - [x] Example client in `services/examples/`
- [x] **Update documentation**
  - [x] Add `docs/sphinx/services_api.rst`
  - [x] Update README to reflect implementation status

---

### Medium Priority

#### 4. GPU Acceleration

- [ ] **Evaluate GPU use cases**
  - [x] Identify compute-intensive operations (aggregations, joins, scans)
    - [x] Relational full scan + predicate (`InMemoryRelationalStorage::select` + `evalPredicate`)
    - [x] Time-series range scan + predicate (`InMemoryTimeSeriesStorage::rangeQuery`)
    - [x] Time-series bucket aggregation (`InMemoryTimeSeriesStorage::aggregate`: per-row bucketing + SUM/MIN/MAX/AVG/COUNT)
    - [x] Expression-mode aggregation (`QueryExecutor::executeSelectWithExpressions`: `TIME_BUCKET`, `FIRST`, `LAST`)
    - [x] Join note: no relational join operator is currently implemented; GPU join evaluation is blocked on implementing CPU join first
  - [x] Decide GPU-suitable candidates (near-term)
    - [x] Numeric predicate evaluation (filters on `int64`/`double`) with simple comparisons (Eq/Ne/Lt/Le/Gt/Ge)
    - [x] Projection + compaction (produce row-id mask on GPU, compact selected columns)
    - [x] Histogram-style aggregation / fixed-width time buckets for time-series (COUNT/SUM/MIN/MAX)
  - [x] Record constraints / risks (likely to dominate vs GPU compute)
    - [x] Current row layout uses `std::unique_ptr<Value>` per cell (AoS + pointers) -> not GPU-friendly; likely need a columnar/packed representation for GPU paths
    - [x] Host↔device transfer cost: GPU wins only for large batches and/or when reuse amortizes copies
    - [x] High-cardinality GROUP BY requires hash tables; start with fixed-width bucketing where bucket index can be computed cheaply
    - [x] String comparisons and general `Value::compare()` are not good first targets
  - [x] Benchmark CPU vs potential GPU speedup
    - [x] Metrics: end-to-end query latency, rows/sec, bytes/sec, and time split (predicate eval vs materialization vs transfers)
    - [x] Datasets: synthetic numeric tables (1e6, 1e7, 1e8 rows) with 1-3 numeric columns + optional timestamp
    - [x] Queries:
      - [x] Scan-only: `SELECT * FROM t` (baseline materialization cost)
      - [x] Filter-selective: `WHERE x < k` at ~1%, 10%, 50%, 90% selectivity
      - [x] Time range: `rangeQuery` equivalent over varying window sizes
      - [x] Time bucket agg: COUNT/SUM over fixed bucket widths with low vs moderate bucket counts
    - [x] Go/no-go thresholds (initial): GPU path should beat optimized CPU by >= 2x on >= 10M rows without pathological latency for small inputs
- [x] **Set up CUDA/GPU build infrastructure**
  - [x] Add CMake option `KADEDB_ENABLE_GPU`
  - [x] Detect CUDA toolkit and configure compilation
  - [x] Add CI job for GPU builds (optional, or document manual build)
- [x] **Implement GPU-accelerated kernels**
  - [x] Create `cpp/src/gpu/` directory for CUDA code
  - [x] Implement parallel scan/filter kernel
  - [x] Implement parallel aggregation kernel
- [x] **Integrate with storage engine**
  - [x] Add GPU execution path in query executor
  - [x] Implement memory transfer optimizations (pinned memory, async copy)
- [x] **Create tests and benchmarks**
  - [x] GPU unit tests (requires CUDA-capable CI or manual testing)
  - [x] CPU baseline benchmark for scan/filter + time-series aggregation (`kadedb_query_bench`)
  - [x] Benchmark comparing CPU vs GPU performance (`kadedb_gpu_vs_cpu_bench`)
- [x] **Update documentation**
  - [x] Add GPU build instructions to `docs/sphinx/guides/getting_started.md`
  - [x] Update README to reflect implementation status

#### 5. Distributed Scalability

- [ ] **Design distributed architecture**
  - [ ] Define sharding strategy (hash-based, range-based)
  - [ ] Design replication model (leader-follower, multi-leader)
  - [ ] Plan consensus protocol (Raft, or external coordinator like etcd)
- [ ] **Implement cluster membership**
  - [ ] Node discovery and heartbeat mechanism
  - [ ] Cluster configuration management
- [ ] **Implement distributed query execution**
  - [ ] Query routing to appropriate shards
  - [ ] Distributed aggregation (map-reduce style)
  - [ ] Result merging from multiple nodes
- [ ] **Implement data replication**
  - [ ] Write-ahead log replication
  - [ ] Failover and leader election
- [ ] **Create tests and examples**
  - [ ] Multi-node integration tests (Docker Compose or similar)
  - [ ] Example cluster setup scripts
- [ ] **Update documentation**
  - [ ] Add `docs/sphinx/guides/distributed_setup.md`
  - [ ] Update README to reflect implementation status

---

### Low Priority

#### 6. Docker Support

- [ ] **Create Dockerfile**
  - [ ] Multi-stage build: build stage (compile) + runtime stage (minimal image)
  - [ ] Support for both Debug and Release builds
  - [ ] Include KadeDB-Lite CLI as entrypoint option
- [ ] **Create docker-compose.yml**
  - [ ] Single-node development setup
  - [ ] (Future) Multi-node cluster setup
- [ ] **Add to CI**
  - [ ] Build and push Docker image on release tags
  - [ ] Publish to Docker Hub (`medilang/kadedb`)
- [ ] **Update documentation**
  - [ ] Add Docker usage instructions to README
  - [ ] Verify Docker badge links correctly

#### 7. ReadTheDocs Integration

- [ ] **Verify `.readthedocs.yaml` exists and is correct**
  - [ ] Check Sphinx build configuration
  - [ ] Ensure `docs/sphinx/conf.py` is compatible
- [ ] **Test ReadTheDocs build**
  - [ ] Trigger manual build on ReadTheDocs dashboard
  - [ ] Fix any build errors
- [ ] **Update badge URL if needed**
  - [ ] Ensure badge in README points to correct project

---

## Notes

- **Relational storage**: ✅ Implemented (`cpp/src/`, `cpp/include/`)
- **Document storage**: ✅ Implemented (`cpp/src/`, `cpp/include/`)
- **KadeDB Core (C++)**: ✅ Implemented
- **KadeDB-Lite (C)**: ✅ Implemented (`lite/`)
- **C ABI Bindings**: ✅ Implemented (`bindings/c/`)
- **Examples**: ✅ All paths exist (`cpp/examples/`, `examples/`)
- **Docs**: ✅ All paths exist (`docs/sphinx/guides/`)
