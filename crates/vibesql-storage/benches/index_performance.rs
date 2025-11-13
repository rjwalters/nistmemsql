/// Performance benchmarks for index architecture validation (Phase 4)
///
/// Compares performance across different scenarios:
/// 1. InMemory (BTreeMap) vs DiskBacked (B+ tree)
/// 2. Small vs large datasets
/// 3. Point lookups vs range scans
/// 4. Memory-constrained vs unlimited memory

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use vibesql_storage::{Database, DatabaseConfig, SpillPolicy};
use vibesql_catalog::SqlValue;
use std::time::Duration;

// Helper to create database with varying sizes
fn setup_database_with_rows(row_count: usize) -> Database {
    let mut db = Database::new();
    db.execute("CREATE TABLE bench_table (id INTEGER, value INTEGER, data TEXT)").unwrap();

    for i in 0..row_count {
        db.execute(&format!(
            "INSERT INTO bench_table VALUES ({}, {}, 'data_{}')",
            i,
            i * 2,
            i
        )).unwrap();
    }

    db.execute("CREATE INDEX idx_id ON bench_table(id)").unwrap();
    db
}

fn setup_disk_backed_database(row_count: usize) -> (Database, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    let mut db = Database::with_path(&db_path);

    db.execute("CREATE TABLE bench_table (id INTEGER, value INTEGER, data TEXT)").unwrap();

    for i in 0..row_count {
        db.execute(&format!(
            "INSERT INTO bench_table VALUES ({}, {}, 'data_{}')",
            i,
            i * 2,
            i
        )).unwrap();
    }

    db.execute("CREATE INDEX idx_id ON bench_table(id)").unwrap();
    (db, temp_dir)
}

fn bench_point_lookup_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup_small");
    group.measurement_time(Duration::from_secs(10));

    let db = setup_database_with_rows(1_000);

    group.bench_function("in_memory", |b| {
        b.iter(|| {
            let rows = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(500))),
                Some(SqlValue::Integer(black_box(500))),
            ).unwrap();
            assert_eq!(rows.len(), 1);
        });
    });

    group.finish();
}

fn bench_point_lookup_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup_large");
    group.measurement_time(Duration::from_secs(10));

    let db = setup_database_with_rows(100_000);

    group.bench_function("in_memory", |b| {
        b.iter(|| {
            let rows = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(50_000))),
                Some(SqlValue::Integer(black_box(50_000))),
            ).unwrap();
            assert_eq!(rows.len(), 1);
        });
    });

    group.finish();
}

fn bench_range_scan_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_small");
    group.measurement_time(Duration::from_secs(10));

    // 1K rows, scan returning 100 rows
    let db = setup_database_with_rows(1_000);

    group.bench_function("range_100_of_1k", |b| {
        b.iter(|| {
            let rows = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(400))),
                Some(SqlValue::Integer(black_box(499))),
            ).unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.finish();
}

fn bench_range_scan_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_large");
    group.measurement_time(Duration::from_secs(15));

    // 100K rows, scan returning 1K rows
    let db = setup_database_with_rows(100_000);

    group.bench_function("range_1k_of_100k", |b| {
        b.iter(|| {
            let rows = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(50_000))),
                Some(SqlValue::Integer(black_box(50_999))),
            ).unwrap();
            assert_eq!(rows.len(), 1_000);
        });
    });

    group.finish();
}

fn bench_full_scan_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");
    group.measurement_time(Duration::from_secs(20));

    let sizes = vec![1_000, 10_000, 100_000];

    for size in sizes {
        let db = setup_database_with_rows(size);

        group.bench_with_input(BenchmarkId::new("indexed", size), &size, |b, _| {
            b.iter(|| {
                let rows = db.scan_table_with_index_range(
                    "bench_table",
                    "idx_id",
                    Some(SqlValue::Integer(black_box(0))),
                    Some(SqlValue::Integer(black_box(size as i64))),
                ).unwrap();
                assert!(rows.len() > 0);
            });
        });
    }

    group.finish();
}

fn bench_index_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_creation");
    group.measurement_time(Duration::from_secs(30));

    let sizes = vec![1_000, 10_000, 50_000];

    for size in sizes {
        group.bench_with_input(BenchmarkId::new("bulk_load", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = Database::new();
                    db.execute("CREATE TABLE bench_table (id INTEGER, value INTEGER)").unwrap();
                    for i in 0..size {
                        db.execute(&format!("INSERT INTO bench_table VALUES ({}, {})", i, i * 2)).unwrap();
                    }
                    db
                },
                |mut db| {
                    db.execute("CREATE INDEX idx_id ON bench_table(id)").unwrap();
                },
            );
        });
    }

    group.finish();
}

fn bench_memory_constrained_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_constrained");
    group.measurement_time(Duration::from_secs(20));

    // Database with tight memory budget
    let config = DatabaseConfig {
        memory_budget: 5 * 1024 * 1024, // 5MB
        disk_budget: 100 * 1024 * 1024,
        spill_policy: SpillPolicy::SpillToDisk,
    };

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    let mut db = Database::with_config_and_path(config, &db_path);

    // Create multiple indexes to trigger eviction
    for table_num in 0..5 {
        let table_name = format!("table{}", table_num);
        db.execute(&format!("CREATE TABLE {} (id INTEGER, data TEXT)", table_name)).unwrap();

        for i in 0..1000 {
            db.execute(&format!(
                "INSERT INTO {} VALUES ({}, '{}')",
                table_name,
                i,
                "x".repeat(50)
            )).unwrap();
        }

        let index_name = format!("idx_{}", table_name);
        db.execute(&format!("CREATE INDEX {} ON {}(id)", index_name, table_name)).unwrap();
    }

    group.bench_function("scan_under_pressure", |b| {
        b.iter(|| {
            // Access different indexes - tests LRU eviction
            for table_num in 0..5 {
                let table_name = format!("table{}", table_num);
                let index_name = format!("idx_{}", table_name);

                let rows = db.scan_table_with_index_range(
                    &table_name,
                    &index_name,
                    Some(SqlValue::Integer(black_box(10))),
                    Some(SqlValue::Integer(black_box(20))),
                ).unwrap();
                assert_eq!(rows.len(), 11);
            }
        });
    });

    group.finish();
}

fn bench_disk_backed_vs_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("backend_comparison");
    group.measurement_time(Duration::from_secs(15));

    let row_count = 10_000;

    // In-memory database
    let db_mem = setup_database_with_rows(row_count);

    // Disk-backed database
    let (db_disk, _temp_dir) = setup_disk_backed_database(row_count);

    group.bench_function("in_memory_range_scan", |b| {
        b.iter(|| {
            let rows = db_mem.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(1000))),
                Some(SqlValue::Integer(black_box(1099))),
            ).unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.bench_function("disk_backed_range_scan", |b| {
        b.iter(|| {
            let rows = db_disk.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(1000))),
                Some(SqlValue::Integer(black_box(1099))),
            ).unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.finish();
}

fn bench_buffer_pool_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool");
    group.measurement_time(Duration::from_secs(15));

    let (db, _temp_dir) = setup_disk_backed_database(50_000);

    // First access (cold cache)
    group.bench_function("cold_cache", |b| {
        b.iter_with_setup(
            || {
                // Reset would happen here if we had the API
                ()
            },
            |_| {
                let rows = db.scan_table_with_index_range(
                    "bench_table",
                    "idx_id",
                    Some(SqlValue::Integer(black_box(1000))),
                    Some(SqlValue::Integer(black_box(1099))),
                ).unwrap();
                assert_eq!(rows.len(), 100);
            },
        );
    });

    // Repeated access (warm cache)
    group.bench_function("warm_cache", |b| {
        // Prime the cache
        for _ in 0..10 {
            let _ = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(1000)),
                Some(SqlValue::Integer(1099)),
            );
        }

        b.iter(|| {
            let rows = db.scan_table_with_index_range(
                "bench_table",
                "idx_id",
                Some(SqlValue::Integer(black_box(1000))),
                Some(SqlValue::Integer(black_box(1099))),
            ).unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_point_lookup_small,
    bench_point_lookup_large,
    bench_range_scan_small,
    bench_range_scan_large,
    bench_full_scan_comparison,
    bench_index_creation,
    bench_memory_constrained_workload,
    bench_disk_backed_vs_in_memory,
    bench_buffer_pool_hit_rate,
);

criterion_main!(benches);
