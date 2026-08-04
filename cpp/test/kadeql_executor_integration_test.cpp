#include "kadedb/kadeql.h"
#include "kadedb/query_executor.h"
#include "kadedb/schema.h"
#include "kadedb/storage.h"
#include "kadedb/value.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

using namespace kadedb;
using namespace kadedb::kadeql;

int main() {
  // Enable GPU exec path (currently CPU-threaded fallback) for this test.
  // This should not change semantics.
#ifdef _WIN32
  (void)_putenv_s("KADEDB_ENABLE_GPU_EXEC", "1");
#else
  (void)setenv("KADEDB_ENABLE_GPU_EXEC", "1", 1);
#endif

  // Set up in-memory storage with a simple table
  InMemoryRelationalStorage storage;
  TableSchema users({
      Column{"name", ColumnType::String, /*nullable=*/false},
      Column{"age", ColumnType::Integer, /*nullable=*/false},
      Column{"email", ColumnType::String, /*nullable=*/true},
  });
  auto st = storage.createTable("users", users);
  assert(st.ok());

  QueryExecutor exec(storage);

  // INSERT with explicit columns
  {
    auto stmt = parseQuery(
        "INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'a@x.com')");
    auto res = exec.execute(*stmt);
    assert(res.hasValue());
    assert(res.value().rowCount() == 1);
    assert(res.value().at(0, 0).asInt() == 1);
  }

  // INSERT implicit columns order (table order)
  {
    auto stmt =
        parseQuery("INSERT INTO users VALUES ('Bob', 22, 'bob@example.com')");
    auto res = exec.execute(*stmt);
    assert(res.hasValue());
  }

  // SELECT *
  {
    auto stmt = parseQuery("SELECT * FROM users");
    auto res = exec.execute(*stmt);
    assert(res.hasValue());
    const auto &rs = res.value();
    assert(rs.columnCount() == 3);
    assert(rs.rowCount() == 2);
  }

  // SELECT with WHERE age > 25
  {
    auto stmt = parseQuery("SELECT name FROM users WHERE age > 25");
    auto res = exec.execute(*stmt);
    assert(res.hasValue());
    const auto &rs = res.value();
    assert(rs.columnCount() == 1);
    assert(rs.rowCount() == 1);
    assert(rs.at(0, 0).asString() == std::string("Alice"));
  }

  // SELECT with AND predicate
  {
    auto stmt = parseQuery(
        "SELECT email FROM users WHERE age >= 20 AND name != 'Alice'");
    auto res = exec.execute(*stmt);
    assert(res.hasValue());
    const auto &rs = res.value();
    assert(rs.rowCount() == 1);
    assert(rs.at(0, 0).asString() == std::string("bob@example.com"));
  }

  std::cout << "KadeQL executor integration test passed" << std::endl;
  return 0;
}
