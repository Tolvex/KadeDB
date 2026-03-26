# KadeDB: A Multi-Model Database for Healthcare and Beyond

[![CI/CD Pipeline](https://github.com/MediLang/KadeDB/actions/workflows/ci.yml/badge.svg)](https://github.com/MediLang/KadeDB/actions/workflows/ci.yml)
[![Code Coverage](https://codecov.io/gh/MediLang/KadeDB/branch/main/graph/badge.svg)](https://codecov.io/gh/MediLang/KadeDB)
[![Documentation Status](https://readthedocs.org/projects/kadedb/badge/?version=latest)](https://kadedb.readthedocs.io/en/latest/?badge=latest)
[![Docker Pulls](https://img.shields.io/docker/pulls/medilang/kadedb)](https://hub.docker.com/r/medilang/kadedb)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://medilang.github.io/KadeDB/)
[![Docs Last Updated](https://img.shields.io/github/last-commit/MediLang/KadeDB?label=docs%20last%20updated)](https://medilang.github.io/KadeDB/)

### Quick Links

- Docs site: https://medilang.github.io/KadeDB/
- Getting Started: `docs/sphinx/guides/getting_started.md`
- API Reference (Doxygen groups): `StorageAPI`, `DocumentAPI`, `PredicateBuilder`
- Examples: `cpp/examples/`
- Services Examples: `services/examples/`
- Tests: `cpp/test/`
- Services Tests: `services/*/tests/`
- Troubleshooting: `docs/sphinx/guides/troubleshooting.md`
- Contributing: `docs/sphinx/guides/contributing.md`

## Overview

KadeDB is a versatile multi-model database designed to support a wide range of applications, from healthcare to finance, logistics, manufacturing, smart cities, and scientific research. It unifies:

- **Relational** storage for structured data (e.g., records, inventories)
- **Document** storage for semi-structured data (e.g., logs, configurations)
- **Time-series** storage for temporal data (e.g., sensor readings, market ticks)
- **Graph** storage for network data (e.g., networks, relationships)

Optimized for high-performance computing (HPC) on servers and edge nodes, KadeDB includes a lightweight client, KadeDB-Lite, for resource-constrained IoT and wearable devices.

### Components

- **KadeDB Core (C++)**: High-performance engine with GPU acceleration (CUDA support + CPU fallback) and distributed scalability for compute-intensive analytics and storage internals.
- **KadeDB Services (Rust)**: Secure, async service layer (REST/gRPC), connectors, auth/RBAC, and orchestration built with Rust for memory safety and reliability.
- **KadeDB-Lite (C)**: Minimal footprint client for IoT/wearables with RocksDB-based embedded storage and low-bandwidth syncing.
- **C ABI Bindings**: Stable C interface (`bindings/c/`) for FFI integration with other languages (Python, Rust, etc.).

> Note: This README provides a high-level overview. Full developer documentation, API reference, and detailed guides now live under `docs/` for GitHub Pages and Sphinx.

## Quick Start (Core)

```bash
# Clone the repository
git clone --recurse-submodules https://github.com/MediLang/KadeDB.git
cd KadeDB

# Configure & build (Debug preset)
cmake -S . --preset debug
# Optional: Enable GPU acceleration with CUDA
cmake -S . --preset debug -DKADEDB_ENABLE_GPU=ON
cmake --build --preset debug -j

# Run tests
ctest --test-dir build --output-on-failure
```

## Quick Start (Services)

The Rust services workspace lives in `services/`.

Run REST (Axum):

```bash
cargo run -p kadedb-services-api --manifest-path services/Cargo.toml
```

Run gRPC (Tonic):

```bash
cargo run -p kadedb-services-grpc --manifest-path services/Cargo.toml
```

Run the services test suite:

```bash
cargo test --workspace --manifest-path services/Cargo.toml
```

### Auth configuration

Both services share the same environment variables:

```bash
export KADEDB_AUTH_ENABLED=true
export KADEDB_JWT_SECRET=your_shared_secret
```

When enabled:

- REST expects `Authorization: Bearer <token>`
- gRPC expects metadata `authorization: Bearer <token>`

### Examples CLI

```bash
cargo run -p kadedb-services-examples --manifest-path services/Cargo.toml -- --help
```

## Testing and Coverage

Run the test suite (default Debug preset):

```bash
cmake -S . -B build/debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/debug -j
ctest --test-dir build/debug --output-on-failure
```

Enable coverage (non-MSVC GCC/Clang) and generate reports with gcovr:

```bash
# Install gcovr (one-time)
python3 -m pip install --user gcovr

# Configure with coverage flags
cmake -S . -B build/debug -DKADEDB_ENABLE_COVERAGE=ON
cmake --build build/debug -j

# Generate coverage (runs tests and writes reports)
cmake --build build/debug --target coverage

# Reports
# - HTML: build/debug/coverage/index.html
# - XML:  build/debug/coverage/coverage.xml
```

Explore examples:

- `cpp/examples/inmemory_rel_example.cpp` – in-memory relational storage
- `cpp/examples/inmemory_rel_errors_example.cpp` – error handling demo
- `cpp/examples/timeseries_example.cpp` – in-memory time-series storage (rangeQuery + aggregate)
- `cpp/examples/graph_example.cpp` – in-memory graph storage (CRUD + traversal + graph queries)
- `examples/cpp_example/` – standalone C++ core API example
- `examples/c_example/` – C ABI example
- `examples/lite_cli/` – interactive KadeDB-Lite CLI

GPU benchmarking:

```bash
# Run comprehensive GPU vs CPU benchmarks
./build/debug/bin/kadedb_gpu_vs_cpu_bench 1000000

# Run basic CPU baseline benchmarks
./build/debug/bin/kadedb_query_bench 200000 200000
```

Key tests:

- `cpp/test/storage_api_test.cpp`
- `cpp/test/storage_predicates_test.cpp`
- `cpp/test/document_predicates_test.cpp`
- `cpp/test/timeseries_test.cpp`
- `cpp/test/graph_test.cpp`

## Developer Reference

Full docs, build options, platform presets, FFI guidance, and CI details live in the documentation site.

- Docs site: https://medilang.github.io/KadeDB/
- Getting Started: `docs/sphinx/guides/getting_started.md`
- API Reference (Doxygen): groups `StorageAPI`, `DocumentAPI`, `PredicateBuilder`

Local docs build (optional):

```bash
doxygen docs/Doxyfile
sphinx-build -b html docs/sphinx docs/sphinx/_build/html
```

## Architecture and Design

Detailed architecture, language rationale, industry guides, roadmaps, and API surfaces are documented on the docs site.

- Docs site: https://medilang.github.io/KadeDB/
- Overview: `docs/sphinx/overview.md`
- API: see the Doxygen groups and Storage/Document API pages



## Contributing

Please see:
- `docs/sphinx/guides/contributing.md`
- [CONTRIBUTING.md](CONTRIBUTING.md)

## License

This project is licensed under the [MIT License](./LICENSE), ensuring open access for developers across industries.
