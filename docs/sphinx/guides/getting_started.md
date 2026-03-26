# Getting Started

Welcome to KadeDB development. This guide helps you build the core, run tests, and explore examples quickly.

## Prerequisites

- CMake 3.21+
- A C++17 compiler (GCC/Clang/MSVC)
- Python (optional, for Sphinx docs)
- CUDA Toolkit 11.0+ (optional, for GPU acceleration)

## GPU Acceleration (Optional)

KadeDB supports GPU acceleration for compute-intensive operations. The GPU implementation currently provides CPU fallback using multi-threading, with CUDA support available when enabled.

### Building with GPU Support

To enable GPU acceleration:

```bash
cmake -S . --preset debug -DKADEDB_ENABLE_GPU=ON
cmake --build --preset debug -j
```

### GPU Requirements

- **CUDA Toolkit**: 11.0 or higher for actual GPU acceleration
- **GPU Hardware**: CUDA-capable GPU with compute capability 6.0+
- **Fallback Mode**: If CUDA is not available, the system automatically uses optimized multi-threaded CPU implementations

### GPU Features

Currently supported GPU-accelerated operations:
- Numeric predicate evaluation (filters on int64/double)
- Parallel scan and filter operations
- Time-series bucket aggregation (COUNT/SUM/MIN/MAX)

### Benchmarking

Test GPU vs CPU performance:

```bash
# Run comprehensive GPU vs CPU benchmarks
./build/debug/bin/kadedb_gpu_vs_cpu_bench 1000000

# Run basic CPU baseline benchmarks
./build/debug/bin/kadedb_query_bench 200000 200000
```

## Build the Core (via CMake Presets)

```bash
cmake -S . --preset debug
cmake --build --preset debug -j
```

Run tests:

```bash
ctest --output-on-failure
```

## Explore the Relational Storage Examples

- `cpp/examples/inmemory_rel_example.cpp` (create/insert/select, AND/OR/NOT)
- `cpp/examples/inmemory_rel_errors_example.cpp` (invalid schema, duplicate unique, composite predicates)

Build targets are created automatically; binaries are in `build/<preset>/bin/`.

## Key Tests to Read

- `cpp/test/storage_api_test.cpp` — end-to-end relational API semantics
- `cpp/test/storage_predicates_test.cpp` — AND/OR/NOT and nested filters, plus corner cases
- `cpp/test/document_predicates_test.cpp` — document queries with composite predicates

Run a subset of tests:

```bash
# From the build directory for your preset
ctest --output-on-failure -R "kadedb_(storage|document)_predicates_test"
```

## Developer Docs (API Reference)

We generate Doxygen XML and render Sphinx HTML. The CI workflow publishes to GitHub Pages.

- Online: https://medilang.github.io/KadeDB/
- Locally:
  ```bash
  doxygen docs/Doxyfile
  sphinx-build -b html docs/sphinx docs/sphinx/_build/html
  ```

## Where to Start Contributing

- Improve docs or add examples under `cpp/examples/`
- Add tests in `cpp/test/`
- Extend the in-memory storages or add new API groups

## Style and Conventions

- Public headers are under `cpp/include/kadedb/`
- Core implementations are under `cpp/src/core/`
- Use `@defgroup` / `@ingroup` in headers to structure API docs
- Prefer `Result<T>` and `Status` for error handling in new APIs
