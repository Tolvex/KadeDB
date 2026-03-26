#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>

#include "kadedb/schema.h"
#include "kadedb/storage.h"
#include "kadedb/timeseries/storage.h"
#include "kadedb/value.h"
#include "kadedb/gpu.h"

using namespace kadedb;

struct BenchmarkResult {
  double cpu_time_ms = 0.0;
  double gpu_time_ms = 0.0;
  size_t rows_processed = 0;
  size_t output_rows = 0;
  double speedup = 0.0;
  
  // Time breakdown for GPU path
  double transfer_to_gpu_ms = 0.0;
  double gpu_compute_ms = 0.0;
  double transfer_from_gpu_ms = 0.0;
};

static int64_t parseInt64(const char *s, int64_t def) {
  if (!s) return def;
  try {
    return std::stoll(std::string(s));
  } catch (...) {
    return def;
  }
}

template <class Fun> static double time_ms(Fun &&f) {
  auto start = std::chrono::high_resolution_clock::now();
  f();
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

// Data generators
static TableSchema makeRelSchema(int num_columns = 3) {
  std::vector<Column> cols;
  cols.push_back(Column{"id", ColumnType::Integer, false, true, {}});
  for (int i = 1; i < num_columns; ++i) {
    cols.push_back(Column{"x" + std::to_string(i), ColumnType::Integer, false, false, {}});
  }
  return TableSchema(cols, std::optional<std::string>("id"));
}

static Row makeRelRow(int64_t id, const std::vector<int64_t>& values) {
  Row r(values.size() + 1);
  r.set(0, ValueFactory::createInteger(id));
  for (size_t i = 0; i < values.size(); ++i) {
    r.set(i + 1, ValueFactory::createInteger(values[i]));
  }
  return r;
}

static TimeSeriesSchema makeTsSchema(int num_value_columns = 1) {
  TimeSeriesSchema s("timestamp", TimeGranularity::Seconds);
  for (int i = 0; i < num_value_columns; ++i) {
    s.addValueColumn(Column{"value" + std::to_string(i), ColumnType::Float, false, false, {}});
  }
  return s;
}

static Row makeTsRow(int64_t ts, const std::vector<double>& values) {
  Row r(values.size() + 1);
  r.set(0, ValueFactory::createInteger(ts));
  for (size_t i = 0; i < values.size(); ++i) {
    r.set(i + 1, ValueFactory::createFloat(values[i]));
  }
  return r;
}

// CPU baseline implementations
static BenchmarkResult benchmarkCpuScanFilter(InMemoryRelationalStorage& rel, 
                                              const std::string& table,
                                              const std::string& column,
                                              int64_t rhs,
                                              Predicate::Op op,
                                              int64_t num_rows) {
  BenchmarkResult result;
  
  Predicate pred;
  pred.kind = Predicate::Kind::Comparison;
  pred.column = column;
  pred.op = op;
  pred.rhs = ValueFactory::createInteger(rhs);
  
  result.cpu_time_ms = time_ms([&]() {
    auto res = rel.select(table, /*columns=*/{}, std::optional<Predicate>(std::move(pred)));
    if (!res.hasValue()) {
      std::cerr << "CPU select failed: " << res.status().message() << "\n";
      std::exit(1);
    }
    result.output_rows = res.value().rowCount();
  });
  
  // Get total rows for throughput calculation
  // Note: InMemoryRelationalStorage doesn't expose getTableInfo, so we estimate
  result.rows_processed = num_rows; // Use estimated row count
  
  return result;
}

static BenchmarkResult benchmarkCpuTimeRange(InMemoryTimeSeriesStorage& ts,
                                             const std::string& series,
                                             int64_t start, int64_t end,
                                             int64_t num_rows) {
  BenchmarkResult result;
  
  result.cpu_time_ms = time_ms([&]() {
    auto res = ts.rangeQuery(series, /*columns=*/{}, start, end, std::nullopt);
    if (!res.hasValue()) {
      std::cerr << "CPU rangeQuery failed: " << res.status().message() << "\n";
      std::exit(1);
    }
    result.output_rows = res.value().rowCount();
  });
  
  // Note: InMemoryTimeSeriesStorage doesn't expose getSeriesInfo, so we estimate
  result.rows_processed = num_rows; // Use estimated row count
  
  return result;
}

static BenchmarkResult benchmarkCpuTimeAggregation(InMemoryTimeSeriesStorage& ts,
                                                  const std::string& series,
                                                  const std::string& value_column,
                                                  TimeAggregation agg,
                                                  int64_t start, int64_t end,
                                                  int64_t bucket_width,
                                                  int64_t num_rows) {
  BenchmarkResult result;
  
  result.cpu_time_ms = time_ms([&]() {
    auto res = ts.aggregate(series, value_column, agg, start, end,
                           bucket_width, TimeGranularity::Seconds, std::nullopt);
    if (!res.hasValue()) {
      std::cerr << "CPU aggregate failed: " << res.status().message() << "\n";
      std::exit(1);
    }
    result.output_rows = res.value().rowCount();
  });
  
  // Note: InMemoryTimeSeriesStorage doesn't expose getSeriesInfo, so we estimate
  result.rows_processed = num_rows; // Use estimated row count
  
  return result;
}

// GPU benchmark implementations
static BenchmarkResult benchmarkGpuScanFilter(const std::vector<int64_t>& column_data,
                                             int64_t rhs, GpuScanSpec::Op op) {
  BenchmarkResult result;
  
  GpuScanSpec spec;
  spec.column = column_data.data();
  spec.count = column_data.size();
  spec.rhs = rhs;
  spec.op = op;
  
  result.rows_processed = column_data.size();
  
  // Time the transfer and computation
  result.gpu_time_ms = time_ms([&]() {
    result.transfer_to_gpu_ms = 0.0; // Placeholder - actual transfer timing needed
    auto gpu_result = gpuScanFilterInt64(spec);
    result.output_rows = gpu_result.size();
    result.transfer_from_gpu_ms = 0.0; // Placeholder
  });
  
  result.gpu_compute_ms = result.gpu_time_ms - result.transfer_to_gpu_ms - result.transfer_from_gpu_ms;
  
  return result;
}

static BenchmarkResult benchmarkGpuTimeAggregation(const std::vector<int64_t>& timestamps,
                                                  const std::vector<double>& values,
                                                  int64_t start, int64_t end,
                                                  int64_t bucket_width) {
  BenchmarkResult result;
  
  GpuTimeBucketAggSpec spec;
  spec.timestamps = timestamps.data();
  spec.values = values.data();
  spec.count = timestamps.size();
  spec.startInclusive = start;
  spec.endExclusive = end;
  spec.bucketWidth = bucket_width;
  
  result.rows_processed = timestamps.size();
  
  result.gpu_time_ms = time_ms([&]() {
    result.transfer_to_gpu_ms = 0.0; // Placeholder
    auto gpu_result = gpuTimeBucketSumCount(spec);
    result.output_rows = gpu_result.bucketStart.size();
    result.transfer_from_gpu_ms = 0.0; // Placeholder
  });
  
  result.gpu_compute_ms = result.gpu_time_ms - result.transfer_to_gpu_ms - result.transfer_from_gpu_ms;
  
  return result;
}

// Utility functions
static void printBenchmarkResult(const std::string& test_name, BenchmarkResult& result) {
  std::cout << std::fixed << std::setprecision(2);
  std::cout << test_name << ":\n";
  std::cout << "  CPU time: " << result.cpu_time_ms << " ms\n";
  std::cout << "  GPU time: " << result.gpu_time_ms << " ms\n";
  std::cout << "  Rows processed: " << result.rows_processed << "\n";
  std::cout << "  Output rows: " << result.output_rows << "\n";
  
  if (result.cpu_time_ms > 0) {
    result.speedup = result.cpu_time_ms / result.gpu_time_ms;
    std::cout << "  Speedup: " << result.speedup << "x\n";
  }
  
  if (result.gpu_time_ms > 0) {
    std::cout << "  GPU breakdown - Transfer to: " << result.transfer_to_gpu_ms 
              << " ms, Compute: " << result.gpu_compute_ms 
              << " ms, Transfer from: " << result.transfer_from_gpu_ms << " ms\n";
  }
  
  // Throughput metrics
  if (result.cpu_time_ms > 0) {
    double cpu_throughput = result.rows_processed / (result.cpu_time_ms / 1000.0);
    std::cout << "  CPU throughput: " << static_cast<int64_t>(cpu_throughput) << " rows/sec\n";
  }
  
  if (result.gpu_time_ms > 0) {
    double gpu_throughput = result.rows_processed / (result.gpu_time_ms / 1000.0);
    std::cout << "  GPU throughput: " << static_cast<int64_t>(gpu_throughput) << " rows/sec\n";
  }
  
  std::cout << "\n";
}

static bool evaluateGoNoGo(const BenchmarkResult& result) {
  // Go/no-go: GPU should beat CPU by >= 2x on >= 10M rows
  if (result.rows_processed >= 10000000 && result.speedup >= 2.0) {
    std::cout << "✅ GO: Meets threshold (>=2x speedup on >=10M rows)\n";
    return true;
  } else if (result.rows_processed >= 10000000) {
    std::cout << "❌ NO-GO: Insufficient speedup on >=10M rows (need >=2x, got " 
              << result.speedup << "x)\n";
    return false;
  } else {
    std::cout << "⚠️  SMALL DATASET: " << result.rows_processed 
              << " rows (<10M threshold)\n";
    return true; // Don't fail on small datasets
  }
}

int main(int argc, char **argv) {
  const int64_t num_rows = (argc > 1) ? parseInt64(argv[1], 1000000) : 1000000;
  const int num_columns = (argc > 2) ? parseInt64(argv[2], 3) : 3;
  
  std::cout << "KadeDB GPU vs CPU Benchmark Suite\n";
  std::cout << "Dataset: " << num_rows << " rows, " << num_columns << " columns\n\n";
  
  // Check GPU availability
  auto gpu_status = gpuStatus();
  std::cout << "GPU Status: " << (gpu_status.available ? "Available" : "Not Available") 
            << " - " << gpu_status.message << "\n\n";
  
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<int64_t> id_dist(0, 1000000);
  std::uniform_real_distribution<double> val_dist(0.0, 1000.0);
  
  bool all_go_no_go_passed = true;
  
  // === Test 1: Scan-only benchmark ===
  {
    std::cout << "=== Test 1: Scan-only (SELECT * FROM t) ===\n";
    
    InMemoryRelationalStorage rel;
    auto schema = makeRelSchema(num_columns);
    (void)rel.createTable("t", schema);
    
    // Generate data
    std::cout << "Generating " << num_rows << " rows...\n";
    double insert_time = time_ms([&]() {
      for (int64_t i = 0; i < num_rows; ++i) {
        std::vector<int64_t> values;
        for (int j = 0; j < num_columns - 1; ++j) {
          values.push_back(id_dist(rng));
        }
        (void)rel.insertRow("t", makeRelRow(i, values));
      }
    });
    std::cout << "Insert time: " << insert_time << " ms\n";
    
    // CPU baseline - full scan (no predicate)
    BenchmarkResult cpu_result = benchmarkCpuScanFilter(rel, "t", "x1", 0, Predicate::Op::Ge, num_rows);
    printBenchmarkResult("CPU Full Scan", cpu_result);
    
    // GPU equivalent (currently CPU fallback)
    std::vector<int64_t> column_data(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      column_data[i] = id_dist(rng);
    }
    BenchmarkResult gpu_result = benchmarkGpuScanFilter(column_data, 0, GpuScanSpec::Op::Ge);
    printBenchmarkResult("GPU Full Scan", gpu_result);
    
    // Evaluate go/no-go
    BenchmarkResult combined;
    combined.cpu_time_ms = cpu_result.cpu_time_ms;
    combined.gpu_time_ms = gpu_result.gpu_time_ms;
    combined.rows_processed = cpu_result.rows_processed;
    combined.speedup = cpu_result.cpu_time_ms / gpu_result.gpu_time_ms;
    
    std::cout << "Go/No-Go Evaluation:\n";
    if (!evaluateGoNoGo(combined)) {
      all_go_no_go_passed = false;
    }
    std::cout << "\n";
  }
  
  // === Test 2: Filter-selective benchmark ===
  {
    std::cout << "=== Test 2: Filter-selective (WHERE x < k) ===\n";
    
    const std::vector<std::pair<int64_t, double>> selectivities = {
      {10000, 1.0},    // ~1% selectivity
      {100000, 10.0},  // ~10% selectivity
      {500000, 50.0},  // ~50% selectivity
      {900000, 90.0}   // ~90% selectivity
    };
    
    for (auto [threshold, expected_selectivity] : selectivities) {
      std::cout << "--- Selectivity ~" << expected_selectivity << "% (x < " << threshold << ") ---\n";
      
      // CPU
      InMemoryRelationalStorage rel;
      auto schema = makeRelSchema(num_columns);
      (void)rel.createTable("t", schema);
      
      // Generate data (reuse same pattern for consistent selectivity)
      for (int64_t i = 0; i < num_rows; ++i) {
        std::vector<int64_t> values;
        for (int j = 0; j < num_columns - 1; ++j) {
          values.push_back(i % 1000000); // Predictable pattern for consistent selectivity
        }
        (void)rel.insertRow("t", makeRelRow(i, values));
      }
      
      BenchmarkResult cpu_result = benchmarkCpuScanFilter(rel, "t", "x1", threshold, Predicate::Op::Lt, num_rows);
      printBenchmarkResult("CPU Filter " + std::to_string(static_cast<int>(expected_selectivity)) + "%", cpu_result);
      
      // GPU
      std::vector<int64_t> column_data(num_rows);
      for (int64_t i = 0; i < num_rows; ++i) {
        column_data[i] = i % 1000000;
      }
      BenchmarkResult gpu_result = benchmarkGpuScanFilter(column_data, threshold, GpuScanSpec::Op::Lt);
      printBenchmarkResult("GPU Filter " + std::to_string(static_cast<int>(expected_selectivity)) + "%", gpu_result);
      
      // Evaluate
      BenchmarkResult combined;
      combined.cpu_time_ms = cpu_result.cpu_time_ms;
      combined.gpu_time_ms = gpu_result.gpu_time_ms;
      combined.rows_processed = cpu_result.rows_processed;
      combined.speedup = cpu_result.cpu_time_ms / gpu_result.gpu_time_ms;
      
      if (!evaluateGoNoGo(combined)) {
        all_go_no_go_passed = false;
      }
      std::cout << "\n";
    }
  }
  
  // === Test 3: Time range query benchmark ===
  {
    std::cout << "=== Test 3: Time range query ===\n";
    
    InMemoryTimeSeriesStorage ts;
    auto ts_schema = makeTsSchema(1);
    (void)ts.createSeries("s", ts_schema, TimePartition::Hourly);
    
    const int64_t base_ts = 1700000000;
    
    // Generate time-series data
    std::cout << "Generating " << num_rows << " time-series points...\n";
    double insert_time = time_ms([&]() {
      for (int64_t i = 0; i < num_rows; ++i) {
        std::vector<double> values = {val_dist(rng)};
        (void)ts.append("s", makeTsRow(base_ts + i, values));
      }
    });
    std::cout << "Insert time: " << insert_time << " ms\n";
    
    // Test different window sizes
    const std::vector<std::pair<int64_t, std::string>> windows = {
      {num_rows / 10, "10% window"},
      {num_rows / 2, "50% window"},
      {num_rows, "100% window"}
    };
    
    for (auto [window_size, window_name] : windows) {
      std::cout << "--- " << window_name << " ---\n";
      
      int64_t start = base_ts;
      int64_t end = base_ts + window_size;
      
      BenchmarkResult cpu_result = benchmarkCpuTimeRange(ts, "s", start, end, num_rows);
      printBenchmarkResult("CPU Time Range " + window_name, cpu_result);
      
      // GPU equivalent (prepare data)
      std::vector<int64_t> timestamps(window_size);
      std::vector<double> values(window_size);
      for (int64_t i = 0; i < window_size; ++i) {
        timestamps[i] = base_ts + i;
        values[i] = val_dist(rng);
      }
      
      BenchmarkResult gpu_result = benchmarkGpuTimeAggregation(timestamps, values, start, end, window_size / 10);
      printBenchmarkResult("GPU Time Range " + window_name, gpu_result);
      
      BenchmarkResult combined;
      combined.cpu_time_ms = cpu_result.cpu_time_ms;
      combined.gpu_time_ms = gpu_result.gpu_time_ms;
      combined.rows_processed = cpu_result.rows_processed;
      combined.speedup = cpu_result.cpu_time_ms / gpu_result.gpu_time_ms;
      
      if (!evaluateGoNoGo(combined)) {
        all_go_no_go_passed = false;
      }
      std::cout << "\n";
    }
  }
  
  // === Test 4: Time bucket aggregation benchmark ===
  {
    std::cout << "=== Test 4: Time bucket aggregation ===\n";
    
    InMemoryTimeSeriesStorage ts;
    auto ts_schema = makeTsSchema(1);
    (void)ts.createSeries("s", ts_schema, TimePartition::Hourly);
    
    const int64_t base_ts = 1700000000;
    
    // Generate data
    for (int64_t i = 0; i < num_rows; ++i) {
      std::vector<double> values = {val_dist(rng)};
      (void)ts.append("s", makeTsRow(base_ts + i, values));
    }
    
    // Test different bucket widths
    const std::vector<std::pair<int64_t, std::string>> bucket_configs = {
      {60, "1-minute buckets"},
      {300, "5-minute buckets"},
      {3600, "1-hour buckets"}
    };
    
    for (auto [bucket_width, bucket_name] : bucket_configs) {
      std::cout << "--- " << bucket_name << " ---\n";
      
      int64_t start = base_ts;
      int64_t end = base_ts + num_rows;
      
      BenchmarkResult cpu_result = benchmarkCpuTimeAggregation(ts, "s", "value0", TimeAggregation::Sum, start, end, bucket_width, num_rows);
      printBenchmarkResult("CPU Time Bucket " + bucket_name, cpu_result);
      
      // GPU equivalent
      std::vector<int64_t> timestamps(num_rows);
      std::vector<double> values(num_rows);
      for (int64_t i = 0; i < num_rows; ++i) {
        timestamps[i] = base_ts + i;
        values[i] = val_dist(rng);
      }
      
      BenchmarkResult gpu_result = benchmarkGpuTimeAggregation(timestamps, values, start, end, bucket_width);
      printBenchmarkResult("GPU Time Bucket " + bucket_name, gpu_result);
      
      BenchmarkResult combined;
      combined.cpu_time_ms = cpu_result.cpu_time_ms;
      combined.gpu_time_ms = gpu_result.gpu_time_ms;
      combined.rows_processed = cpu_result.rows_processed;
      combined.speedup = cpu_result.cpu_time_ms / gpu_result.gpu_time_ms;
      
      if (!evaluateGoNoGo(combined)) {
        all_go_no_go_passed = false;
      }
      std::cout << "\n";
    }
  }
  
  // === Summary ===
  std::cout << "=== BENCHMARK SUMMARY ===\n";
  std::cout << "Dataset size: " << num_rows << " rows\n";
  std::cout << "GPU available: " << (gpu_status.available ? "Yes" : "No") << "\n";
  std::cout << "Go/No-Go status: " << (all_go_no_go_passed ? "✅ PASSED" : "❌ FAILED") << "\n";
  
  if (!all_go_no_go_passed) {
    std::cout << "\n⚠️  Some benchmarks did not meet the 2x speedup threshold on >=10M rows.\n";
    std::cout << "This may indicate that GPU optimization needs more work or that the\n";
    std::cout << "current implementation is already CPU-optimized for these workloads.\n";
  }
  
  return all_go_no_go_passed ? 0 : 1;
}
