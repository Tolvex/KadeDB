# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

KadeDB is a multi-model database (relational, document, time-series, graph) with three components in one
repo:

- **`cpp/`** — KadeDB Core: the C++17 storage/query engine (`kadedb_core` library), with optional CUDA GPU
  acceleration.
- **`bindings/c/`** — Stable C ABI (`kadedb_c` library) wrapping the C++ core for FFI consumption.
- **`lite/`** — KadeDB-Lite: a minimal-footprint C client for IoT/wearables, with optional RocksDB backend.
- **`services/`** — KadeDB Services: a Rust workspace providing REST (Axum) and gRPC (Tonic) service layers,
  auth/RBAC, and an FFI bridge that calls into the C ABI.

## Build & Test (C++ core / C ABI / Lite)

Uses CMake presets (`CMakePresets.json`). Common presets: `debug`, `release`, `relwithdebinfo`,
`linux-gcc-debug`, `linux-clang-debug`, `macos-clang-debug`, `windows-vs2022`, plus sanitizer/coverage
variants `debug-asan`, `debug-ubsan`, `relwithdebinfo-coverage`.

```bash
# Configure & build (Debug)
cmake -S . --preset debug
cmake --build --preset debug -j

# Run all tests
ctest --test-dir build/debug --output-on-failure

# Run a single test by name (regex match against CTest test names)
ctest --test-dir build/debug --output-on-failure -R kadedb_kadeql_test

# Optional: GPU acceleration (requires CUDA toolkit)
cmake -S . --preset debug -DKADEDB_ENABLE_GPU=ON
```

Other useful CMake options (see root `CMakeLists.txt`):
`KADEDB_BUILD_SHARED`, `KADEDB_LITE_WITH_ROCKSDB`, `KADEDB_ENABLE_ASAN`, `KADEDB_ENABLE_UBSAN`,
`KADEDB_ENABLE_COVERAGE`, `KADEDB_MEM_DEBUG`, `KADEDB_ENABLE_SMALL_OBJECT_POOL`, `KADEDB_RC_STRINGS`.

Coverage reports (gcc/clang only):

```bash
cmake -S . -B build/debug -DKADEDB_ENABLE_COVERAGE=ON
cmake --build build/debug -j
cmake --build build/debug --target coverage   # writes build/debug/coverage/{index.html,coverage.xml}
```

GPU vs CPU benchmarks (after building with `-DKADEDB_ENABLE_GPU=ON`):

```bash
./build/debug/bin/kadedb_gpu_vs_cpu_bench 1000000
./build/debug/bin/kadedb_query_bench 200000 200000
```

Code formatting is enforced via pre-commit (`clang-format` for C/C++/headers, `cmake-format` for CMake files,
`markdownlint` for docs). Run `./scripts/format.sh` (all files) or `./scripts/format-changed.sh` (changed
files only) before committing.

## Build & Test (Rust services)

The Rust workspace lives in `services/` with its own `Cargo.toml`/`Cargo.lock` (members: `api`, `auth`,
`ffi`, `grpc`, `examples`). It depends on the C ABI (`kadedb_c`) via the `ffi` crate, so the native C++/C
build must exist first (built via the `debug` CMake preset in CI).

```bash
# Run test suite
cargo test --workspace --manifest-path services/Cargo.toml

# Run REST service (Axum)
cargo run -p kadedb-services-api --manifest-path services/Cargo.toml

# Run gRPC service (Tonic)
cargo run -p kadedb-services-grpc --manifest-path services/Cargo.toml

# Format check
cargo fmt --all --check --manifest-path services/Cargo.toml
```

Auth is shared config across both services via env vars:

```bash
export KADEDB_AUTH_ENABLED=true
export KADEDB_JWT_SECRET=your_shared_secret
```
REST expects `Authorization: Bearer <token>`; gRPC expects the `authorization` metadata key with the same
value.

## Architecture

### C++ core (`cpp/`)

`kadedb_core` is one library assembled from several layers (see `cpp/CMakeLists.txt` for the exact source
list):

- **Value/Schema layer** (`value.h`, `schema.h`, `serialization.h`, `status.h`, `result.h`): `Value` is a
  polymorphic base (`NullValue`, `IntegerValue`, `FloatValue`, `StringValue`, `BooleanValue`) with
  `clone()`/`equals()`/`compare()`. `Row` (deep copy, `unique_ptr<Value>` per cell) vs `RowShallow` (shallow
  copy, `shared_ptr<Value>` per cell, converted via `fromClones()`/`toRowDeep()`) are two intentionally
  different ownership models — see `docs/design-notes.md` before touching row/value copy semantics.
  `Status`/`Result<T>` is the error-handling convention used across the core instead of exceptions at API
  boundaries.
- **Storage layer** (`storage.h`, `storage.cpp`, `graph_storage.cpp`, `timeseries_storage.cpp`): in-memory
  relational, document, graph, and time-series stores. `Predicate` is a recursive tree
  (`Comparison`/`And`/`Or`/`Not`) shared by all storage backends for `SELECT`/`UPDATE`/`DELETE` filtering.
- **KadeQL** (`kadeql*.h/.cpp`): a SQL-like query language — tokenizer → parser (produces an AST) →
  `QueryExecutor`, which executes against the storage layer including `TIME_BUCKET`/`FIRST`/`LAST`
  aggregation functions and an optimizer canonicalization pass.
- **GPU layer** (`gpu.h`, `gpu_transfer.h`, `src/gpu/*.cpp`): optional CUDA-accelerated execution path for
  numeric predicate evaluation, projection/compaction, and fixed-width bucket aggregation, gated behind
  `KADEDB_ENABLE_GPU`/`KADEDB_HAVE_CUDA` with a CPU fallback when CUDA isn't found. Row storage is
  AoS/pointer-based (`unique_ptr<Value>` per cell), which is not naturally GPU-friendly — see
  `docs/optimizer.md` and `TODO.md` for the constraints this places on which operations get GPU paths.

### C ABI (`bindings/c/`)

`kadedb_c` wraps `kadedb_core` behind a stable, versioned C interface (`kadedb.h`, `kadedb_ffi_helpers.h`)
intended for cross-language FFI. This is the only supported entry point for non-C++ consumers (notably the
Rust `services/ffi` crate) — do not link Rust or other language bindings directly against `kadedb_core`.

### KadeDB-Lite (`lite/`)

A separate, minimal C client (`kadedb_lite.h`/`.c`, plus `_query`/`_sync` modules) aimed at
resource-constrained IoT/wearable targets, with an optional RocksDB-backed storage engine
(`KADEDB_LITE_WITH_ROCKSDB`, can `FetchContent` RocksDB if not found locally). It has its own test suite and
is excluded from core coverage reports.

### Rust services (`services/`)

- `ffi`: unsafe `extern "C"` bindings to the C ABI (`KadeDB_CreateStorage`, `KadeDB_ExecuteQuery`, result-set
  iteration, etc.), wrapped in a safe Rust API. Raw handles are `Send` because the underlying C++ storage is
  internally mutex-synchronized — see the safety comment in `services/ffi/src/lib.rs` before changing that
  assumption.
- `auth`: JWT-based auth and `Permission`-based RBAC (`Read`/`Write`/...), consumed as Axum middleware and
  gRPC interceptors.
- `api`: Axum REST router; routes are split into public (`/health`) and permission-gated groups
  (`/query` requires `Permission::Read`, `/tables` requires `Permission::Write`) via
  `auth_middleware`/`middleware::from_fn_with_state`.
- `grpc`: Tonic gRPC service defined by `services/proto/kadedb.proto`.
- `examples`: a CLI client exercising the REST/gRPC services.

Data flow for a query: client → `api` (REST) or `grpc` service → `auth` middleware checks JWT + permission →
`ffi` crate calls into `kadedb_c` (C ABI) → `kadedb_core` (C++ engine, optionally GPU-accelerated) →
result set marshaled back up through FFI → JSON/protobuf response.

## Coding standards

- C++ follows the Google C++ Style Guide with: 100-char lines, `#pragma once` include guards, `constexpr`
  over `#define`, `nullptr` over `NULL`, `auto` when the type is obvious. C++17, C99.
- Document public APIs with Doxygen-style comments (`StorageAPI`, `DocumentAPI`, `PredicateBuilder` are the
  main Doxygen groups; see the docs site for the generated reference).
- Conventional Commits for commit messages (`type(scope): description`); branch names as
  `type/scope/short-description` (e.g. `feat/storage/add-rocksdb-backend`).

## Docs

Full developer docs (getting started, architecture, industry guides, FFI guidance, API reference) live at
https://medilang.github.io/KadeDB/ and under `docs/sphinx/`, built locally via:

```bash
doxygen docs/Doxyfile
sphinx-build -b html docs/sphinx docs/sphinx/_build/html
```

`TODO.md` tracks known gaps between README claims and actual implementation status — check it before
assuming a described feature (e.g. distributed scalability, Docker support) is actually implemented.

## Working Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. **Tradeoff:** these bias toward caution over
speed — for trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant
clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to
overcomplication, and clarifying questions come before implementation rather than after mistakes.
