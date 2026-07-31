# KadeDB Plans.md

作成日: 2026-07-27

Seeded from `TODO.md`'s remaining unchecked action items (Distributed Scalability, Docker Support,
ReadTheDocs Integration). GPU Acceleration's sub-items are all complete in `TODO.md`.

---

## Phase 1: Distributed Scalability [needs-spike]

Purpose: Deliver the "distributed scalability" capability the README claims but TODO.md (#5) marks unimplemented.

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 1.1-spike | [spike] Distributed architecture technical validation — evaluate sharding (hash vs range), replication (leader-follower vs multi-leader), consensus (Raft vs external coordinator e.g. etcd) | Validation report recorded in `docs/spec/00-project-spec.md` Open Decisions table with a chosen approach | - | cc:done [0911538] |
| 1.2 | Design distributed architecture: finalize sharding strategy, replication model, consensus protocol [needs-spike] | `docs/spec/00-project-spec.md` updated with a concrete WAL wire format, leader→follower replication RPC schema, and chosen etcd client crate/API (not just the Task 1.1-spike direction) | 1.1-spike | cc:done [732d9ee] |
| 1.3 | Implement cluster membership: node discovery/heartbeat mechanism, cluster configuration management | Nodes can join/leave a cluster; heartbeat failure is observable in tests | 1.2 | cc:done [a4d946e] |
| 1.4 | Implement distributed query execution: query routing to shards, distributed aggregation (map-reduce style), result merging from multiple nodes | Multi-node integration test executes a query across shards and returns correctly merged results | 1.3 | cc:done [42d80e4] |
| 1.5 | Implement data replication: write-ahead log replication, failover and leader election | Killing the leader node in a test triggers election and replica promotion | 1.3 | cc:TODO |
| 1.6 | Create tests and examples: multi-node integration tests (Docker Compose or similar), example cluster setup scripts [tdd:skip:test-authoring-task] | Multi-node test suite passes in CI or documented manual run; example cluster script runs end-to-end | 1.4, 1.5 | cc:TODO |
| 1.7 | Update documentation: `docs/sphinx/guides/distributed_setup.md`, README [skip:tdd] | Docs page exists with no broken links; README reflects implementation status | 1.6 | cc:TODO |

## Phase 2: Docker Support

Purpose: Ship the container image and compose workflow the README's Docker badge implies but TODO.md (#6) marks unimplemented.

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 2.1 | Create Dockerfile: multi-stage build (compile + minimal runtime), support Debug and Release builds, KadeDB-Lite CLI as an entrypoint option [skip:tdd] | `docker build` succeeds and produces a runnable image | - | cc:TODO |
| 2.2 | Create docker-compose.yml: single-node development setup [skip:tdd] | `docker compose up` starts a working single-node instance | 2.1 | cc:TODO |
| 2.3 | Add Docker build/push to CI: build and push image on release tags, publish to `medilang/kadedb` on Docker Hub [skip:tdd] | CI workflow run produces a pushed tag on a release trigger | 2.1 | cc:TODO |
| 2.4 | Update documentation: Docker usage instructions in README, verify Docker badge link [skip:tdd] | README has working Docker instructions; badge link resolves | 2.2, 2.3 | cc:TODO |

## Phase 3: ReadTheDocs Integration

Purpose: Make the ReadTheDocs badge in README accurate — no `.readthedocs.yaml` currently exists in the repo (TODO.md #7).

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 3.1 | Create `.readthedocs.yaml` and confirm `docs/sphinx/conf.py` is compatible [skip:tdd] | `.readthedocs.yaml` exists and `sphinx-build` runs locally without errors | - | cc:TODO |
| 3.2 | Trigger and fix the ReadTheDocs build | ReadTheDocs dashboard build succeeds | 3.1 | cc:TODO |
| 3.3 | Update badge URL in README if needed [skip:tdd] | Badge in README resolves to the correct, passing ReadTheDocs project | 3.2 | cc:TODO |
