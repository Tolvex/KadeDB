# Core C++ library

# Optional memory debug flags
option(KADEDB_MEM_DEBUG "Enable KadeDB memory debug counters" OFF)
option(KADEDB_ENABLE_SMALL_OBJECT_POOL "Enable small object pool for small Value types" OFF)
option(KADEDB_RC_STRINGS "Enable reference-counted storage for StringValue payloads" OFF)
set(LIB_TYPE SHARED)
if(NOT KADEDB_BUILD_SHARED)
  set(LIB_TYPE STATIC)
endif()

add_library(kadedb_core ${LIB_TYPE}
  src/core/version.cpp
  src/core/value.cpp
  src/core/schema.cpp
  src/core/serialization.cpp
  src/core/storage.cpp
  src/core/graph_storage.cpp
  src/core/graph_query.cpp
  src/core/timeseries_storage.cpp
  src/core/kadeql_tokenizer.cpp
  src/core/kadeql_ast.cpp
  src/core/kadeql_parser.cpp
  src/core/query_executor.cpp
  src/gpu/gpu.cpp
  src/gpu/gpu_transfer.cpp
)

add_library(KadeDB::kadedb_core ALIAS kadedb_core)

# Include directories
# - Public headers in source tree
# - Generated headers in build tree
# - Installed headers under include/

target_include_directories(kadedb_core
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${KADEDB_GENERATED_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>
)

# Compile features
target_compile_features(kadedb_core PUBLIC cxx_std_17)

# Properties
set_target_properties(kadedb_core PROPERTIES
  OUTPUT_NAME kadedb_core
  SOVERSION ${PROJECT_VERSION_MAJOR}
  VERSION ${PROJECT_VERSION}
)

# Compile features/definitions
if(MSVC)
  target_compile_definitions(kadedb_core PRIVATE _CRT_SECURE_NO_WARNINGS)
  set_target_properties(kadedb_core PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

# Propagate optional flags
if(KADEDB_MEM_DEBUG)
  target_compile_definitions(kadedb_core PUBLIC KADEDB_MEM_DEBUG)
endif()
if(KADEDB_ENABLE_SMALL_OBJECT_POOL)
  target_compile_definitions(kadedb_core PUBLIC KADEDB_ENABLE_SMALL_OBJECT_POOL)
endif()
if(KADEDB_RC_STRINGS)
  target_compile_definitions(kadedb_core PUBLIC KADEDB_RC_STRINGS)
endif()

# Optional GPU support (CUDA)
if(KADEDB_ENABLE_GPU)
  find_package(CUDAToolkit)
  if(CUDAToolkit_FOUND)
    target_compile_definitions(kadedb_core PUBLIC KADEDB_HAVE_CUDA)
    target_link_libraries(kadedb_core PUBLIC CUDA::cudart)
  else()
    message(WARNING "KADEDB_ENABLE_GPU is ON but CUDAToolkit was not found; building without CUDA support")
  endif()
endif()

# Install
install(TARGETS kadedb_core
  EXPORT KadeDBTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Benchmark executable (no external deps)
add_executable(kadedb_serialization_bench
  bench/serialization_bench.cpp
)

target_link_libraries(kadedb_serialization_bench PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_serialization_bench PRIVATE cxx_std_17)

add_executable(kadedb_query_bench
  bench/query_bench.cpp
)

target_link_libraries(kadedb_query_bench PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_query_bench PRIVATE cxx_std_17)

add_executable(kadedb_gpu_vs_cpu_bench
  bench/gpu_vs_cpu_bench.cpp
)

target_link_libraries(kadedb_gpu_vs_cpu_bench PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_gpu_vs_cpu_bench PRIVATE cxx_std_17)

# Examples
add_executable(kadedb_inmemory_rel_example
  examples/inmemory_rel_example.cpp
)

target_link_libraries(kadedb_inmemory_rel_example PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_inmemory_rel_example PRIVATE cxx_std_17)

add_executable(kadedb_inmemory_rel_errors_example
  examples/inmemory_rel_errors_example.cpp
)

target_link_libraries(kadedb_inmemory_rel_errors_example PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_inmemory_rel_errors_example PRIVATE cxx_std_17)

# TimeSeries example
add_executable(kadedb_timeseries_example
  examples/timeseries_example.cpp
)

target_link_libraries(kadedb_timeseries_example PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_timeseries_example PRIVATE cxx_std_17)

# KadeQL example
add_executable(kadedb_kadeql_example
  examples/kadeql_example.cpp
)

target_link_libraries(kadedb_kadeql_example PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_kadeql_example PRIVATE cxx_std_17)

# Graph example
add_executable(kadedb_graph_example
  examples/graph_example.cpp
)

target_link_libraries(kadedb_graph_example PRIVATE KadeDB::kadedb_core)

target_compile_features(kadedb_graph_example PRIVATE cxx_std_17)
