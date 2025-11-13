"""
Performance overhead measurement for Python bindings

This script measures the performance overhead of the Python bindings
compared to native Rust execution.
"""
import time
import vibesql


def benchmark_simple_queries(query_count=1000):
    """Benchmark simple SELECT queries"""
    print(f"\n=== Benchmarking {query_count} Simple Queries ===")

    db = vibesql.connect()
    cursor = db.cursor()

    start = time.perf_counter()
    for i in range(query_count):
        cursor.execute(f"SELECT {i}")
        cursor.fetchall()
    elapsed = time.perf_counter() - start

    print(f"Total time: {elapsed:.3f}s")
    print(f"Average: {elapsed/query_count*1000:.3f}ms per query")
    print(f"Throughput: {query_count/elapsed:.1f} queries/second")

    cursor.close()
    db.close()


def benchmark_table_operations(iterations=100):
    """Benchmark CREATE, INSERT, SELECT operations"""
    print(f"\n=== Benchmarking {iterations} Table Operations ===")

    db = vibesql.connect()
    cursor = db.cursor()

    # Create table
    create_start = time.perf_counter()
    cursor.execute("CREATE TABLE benchmark (id INTEGER, value VARCHAR(50))")
    create_elapsed = time.perf_counter() - create_start
    print(f"CREATE TABLE: {create_elapsed*1000:.3f}ms")

    # Insert operations
    insert_start = time.perf_counter()
    for i in range(iterations):
        cursor.execute(f"INSERT INTO benchmark VALUES ({i}, 'value_{i}')")
    insert_elapsed = time.perf_counter() - insert_start
    print(f"INSERT {iterations} rows: {insert_elapsed*1000:.3f}ms")
    print(f"Average INSERT: {insert_elapsed/iterations*1000:.3f}ms per row")

    # Select all
    select_start = time.perf_counter()
    cursor.execute("SELECT * FROM benchmark")
    rows = cursor.fetchall()
    select_elapsed = time.perf_counter() - select_start
    print(f"SELECT {len(rows)} rows: {select_elapsed*1000:.3f}ms")

    # Drop table
    drop_start = time.perf_counter()
    cursor.execute("DROP TABLE benchmark")
    drop_elapsed = time.perf_counter() - drop_start
    print(f"DROP TABLE: {drop_elapsed*1000:.3f}ms")

    cursor.close()
    db.close()


def benchmark_data_types():
    """Benchmark different data type conversions"""
    print("\n=== Benchmarking Data Type Conversions ===")

    db = vibesql.connect()
    cursor = db.cursor()

    test_cases = [
        ("INTEGER", "SELECT 42"),
        ("FLOAT", "SELECT 3.14159"),
        ("VARCHAR", "SELECT 'Hello, World!'"),
        ("BOOLEAN", "SELECT TRUE"),
        ("NULL", "SELECT NULL"),
        ("MIXED", "SELECT 1, 2.5, 'test', FALSE, NULL"),
    ]

    iterations = 1000

    for name, query in test_cases:
        start = time.perf_counter()
        for _ in range(iterations):
            cursor.execute(query)
            cursor.fetchone()
        elapsed = time.perf_counter() - start
        print(f"{name:12s}: {elapsed/iterations*1000:.4f}ms per query")

    cursor.close()
    db.close()


def benchmark_bulk_operations(row_count=1000):
    """Benchmark bulk insert and select operations"""
    print(f"\n=== Benchmarking Bulk Operations ({row_count} rows) ===")

    db = vibesql.connect()
    cursor = db.cursor()

    cursor.execute(
        "CREATE TABLE bulk_test (id INTEGER, name VARCHAR(50), value FLOAT, active BOOLEAN)"
    )

    # Bulk insert
    insert_start = time.perf_counter()
    for i in range(row_count):
        cursor.execute(
            f"INSERT INTO bulk_test VALUES ({i}, 'name_{i}', {i * 1.5}, TRUE)"
        )
    insert_elapsed = time.perf_counter() - insert_start

    print(f"Bulk INSERT: {insert_elapsed*1000:.3f}ms ({insert_elapsed/row_count*1000:.4f}ms per row)")
    print(f"INSERT throughput: {row_count/insert_elapsed:.1f} rows/second")

    # Bulk select
    select_start = time.perf_counter()
    cursor.execute("SELECT * FROM bulk_test")
    rows = cursor.fetchall()
    select_elapsed = time.perf_counter() - select_start

    print(f"Bulk SELECT: {select_elapsed*1000:.3f}ms ({select_elapsed/len(rows)*1000:.4f}ms per row)")
    print(f"SELECT throughput: {len(rows)/select_elapsed:.1f} rows/second")

    cursor.execute("DROP TABLE bulk_test")

    cursor.close()
    db.close()


def benchmark_fetchmany(row_count=10000):
    """Benchmark fetchmany with different batch sizes"""
    print(f"\n=== Benchmarking fetchmany() with {row_count} rows ===")

    db = vibesql.connect()
    cursor = db.cursor()

    # Setup
    cursor.execute("CREATE TABLE fetch_test (n INTEGER)")
    for i in range(row_count):
        cursor.execute(f"INSERT INTO fetch_test VALUES ({i})")

    batch_sizes = [1, 10, 100, 1000]

    for batch_size in batch_sizes:
        cursor.execute("SELECT * FROM fetch_test")

        start = time.perf_counter()
        total_rows = 0
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            total_rows += len(rows)
        elapsed = time.perf_counter() - start

        print(f"Batch size {batch_size:5d}: {elapsed*1000:.3f}ms ({total_rows/elapsed:.1f} rows/second)")

    cursor.execute("DROP TABLE fetch_test")
    cursor.close()
    db.close()


def main():
    """Run all benchmarks"""
    print("=" * 60)
    print("vibesql Python Bindings Performance Benchmark")
    print("=" * 60)

    benchmark_simple_queries(1000)
    benchmark_table_operations(100)
    benchmark_data_types()
    benchmark_bulk_operations(1000)
    benchmark_fetchmany(1000)

    print("\n" + "=" * 60)
    print("Benchmark Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
