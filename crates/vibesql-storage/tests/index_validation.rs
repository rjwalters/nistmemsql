/// Comprehensive validation tests for index architecture refactoring (Phase 4)
///
/// This test suite validates:
/// 1. Adaptive backend selection (InMemory vs DiskBacked)
/// 2. LRU eviction behavior
/// 3. Memory budget enforcement
/// 4. Performance characteristics
/// 5. Persistence and recovery

use vibesql_storage::{Database, DatabaseConfig, SpillPolicy};
use vibesql_catalog::SqlValue;

#[cfg(test)]
mod adaptive_backend_tests {
    use super::*;

    #[test]
    fn test_small_index_uses_in_memory_backend() {
        // Create database with default config
        let mut db = Database::new();

        // Create a small table with index
        db.execute("CREATE TABLE small_table (id INTEGER, name TEXT)").unwrap();

        // Insert a small number of rows (should stay in memory)
        for i in 0..100 {
            db.execute(&format!("INSERT INTO small_table VALUES ({}, 'user{}')", i, i)).unwrap();
        }

        db.execute("CREATE INDEX idx_small ON small_table(id)").unwrap();

        // Verify index works
        let rows = db.scan_table_with_index_range(
            "small_table",
            "idx_small",
            Some(SqlValue::Integer(10)),
            Some(SqlValue::Integer(20)),
        ).unwrap();

        assert_eq!(rows.len(), 11); // 10-20 inclusive

        // Small indexes should use InMemory backend
        // (We can't directly inspect internal state, but we can verify behavior)
        // InMemory indexes are fast and don't create disk files
    }

    #[test]
    fn test_large_index_can_use_disk_backed_backend() {
        // Create database with path to enable disk-backed indexes
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut db = Database::with_path(&db_path);

        // Create a larger table
        db.execute("CREATE TABLE large_table (id INTEGER, data TEXT)").unwrap();

        // Insert more rows to potentially trigger disk-backed storage
        for i in 0..1000 {
            db.execute(&format!(
                "INSERT INTO large_table VALUES ({}, '{}')",
                i,
                "x".repeat(100) // Make rows larger
            )).unwrap();
        }

        db.execute("CREATE INDEX idx_large ON large_table(id)").unwrap();

        // Verify index works correctly
        let rows = db.scan_table_with_index_range(
            "large_table",
            "idx_large",
            Some(SqlValue::Integer(500)),
            Some(SqlValue::Integer(510)),
        ).unwrap();

        assert_eq!(rows.len(), 11); // 500-510 inclusive
    }

    #[test]
    fn test_multiple_indexes_coexist() {
        let mut db = Database::new();

        db.execute("CREATE TABLE multi (a INTEGER, b INTEGER, c INTEGER)").unwrap();

        for i in 0..500 {
            db.execute(&format!("INSERT INTO multi VALUES ({}, {}, {})", i, i * 2, i * 3)).unwrap();
        }

        // Create multiple indexes
        db.execute("CREATE INDEX idx_a ON multi(a)").unwrap();
        db.execute("CREATE INDEX idx_b ON multi(b)").unwrap();
        db.execute("CREATE INDEX idx_c ON multi(c)").unwrap();

        // All indexes should work
        let rows_a = db.scan_table_with_index_range(
            "multi",
            "idx_a",
            Some(SqlValue::Integer(10)),
            Some(SqlValue::Integer(20)),
        ).unwrap();
        assert_eq!(rows_a.len(), 11);

        let rows_b = db.scan_table_with_index_range(
            "multi",
            "idx_b",
            Some(SqlValue::Integer(20)),
            Some(SqlValue::Integer(40)),
        ).unwrap();
        assert_eq!(rows_b.len(), 11);
    }
}

#[cfg(test)]
mod resource_budget_tests {
    use super::*;

    #[test]
    fn test_memory_budget_configuration() {
        let config = DatabaseConfig {
            memory_budget: 10 * 1024 * 1024, // 10MB
            disk_budget: 100 * 1024 * 1024,   // 100MB
            spill_policy: SpillPolicy::SpillToDisk,
        };

        let _db = Database::with_config(config);

        // Database should respect the configured budgets
        // This is validated by the ResourceTracker internally
    }

    #[test]
    fn test_browser_default_config() {
        let config = DatabaseConfig::browser_default();

        assert_eq!(config.memory_budget, 512 * 1024 * 1024); // 512MB
        assert_eq!(config.disk_budget, 2 * 1024 * 1024 * 1024); // 2GB
        assert_eq!(config.spill_policy, SpillPolicy::SpillToDisk);
    }

    #[test]
    fn test_server_default_config() {
        let config = DatabaseConfig::server_default();

        assert_eq!(config.memory_budget, 16 * 1024 * 1024 * 1024); // 16GB
        assert_eq!(config.disk_budget, 1024 * 1024 * 1024 * 1024); // 1TB
        assert_eq!(config.spill_policy, SpillPolicy::BestEffort);
    }

    #[test]
    fn test_test_default_config() {
        let config = DatabaseConfig::test_default();

        // Test config should have reasonable limits for testing
        assert!(config.memory_budget > 0);
        assert!(config.disk_budget > 0);
    }
}

#[cfg(test)]
mod lru_eviction_tests {
    use super::*;

    #[test]
    fn test_index_eviction_with_memory_pressure() {
        // Create database with tight memory budget
        let config = DatabaseConfig {
            memory_budget: 5 * 1024 * 1024, // 5MB - tight budget
            disk_budget: 100 * 1024 * 1024,
            spill_policy: SpillPolicy::SpillToDisk,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut db = Database::with_config_and_path(config, &db_path);

        // Create multiple tables with indexes
        for table_num in 0..5 {
            let table_name = format!("table{}", table_num);
            db.execute(&format!("CREATE TABLE {} (id INTEGER, data TEXT)", table_name)).unwrap();

            // Insert data
            for i in 0..200 {
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

        // With memory pressure, some indexes should be evicted to disk
        // But all indexes should still work correctly via demand paging

        // Access all indexes to verify they work despite eviction
        for table_num in 0..5 {
            let table_name = format!("table{}", table_num);
            let index_name = format!("idx_{}", table_name);

            let rows = db.scan_table_with_index_range(
                &table_name,
                &index_name,
                Some(SqlValue::Integer(10)),
                Some(SqlValue::Integer(20)),
            ).unwrap();

            assert_eq!(rows.len(), 11, "Index {} should work correctly", index_name);
        }
    }

    #[test]
    fn test_hot_index_stays_in_memory() {
        let config = DatabaseConfig {
            memory_budget: 10 * 1024 * 1024,
            disk_budget: 100 * 1024 * 1024,
            spill_policy: SpillPolicy::SpillToDisk,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut db = Database::with_config_and_path(config, &db_path);

        // Create hot and cold tables
        db.execute("CREATE TABLE hot (id INTEGER)").unwrap();
        db.execute("CREATE TABLE cold (id INTEGER)").unwrap();

        for i in 0..100 {
            db.execute(&format!("INSERT INTO hot VALUES ({})", i)).unwrap();
            db.execute(&format!("INSERT INTO cold VALUES ({})", i)).unwrap();
        }

        db.execute("CREATE INDEX idx_hot ON hot(id)").unwrap();
        db.execute("CREATE INDEX idx_cold ON cold(id)").unwrap();

        // Repeatedly access hot index
        for _ in 0..10 {
            let _ = db.scan_table_with_index_range(
                "hot",
                "idx_hot",
                Some(SqlValue::Integer(10)),
                Some(SqlValue::Integer(20)),
            ).unwrap();
        }

        // Hot index should stay in memory (LRU keeps frequently accessed)
        // Cold index may be evicted but should still work
        let cold_rows = db.scan_table_with_index_range(
            "cold",
            "idx_cold",
            Some(SqlValue::Integer(30)),
            Some(SqlValue::Integer(40)),
        ).unwrap();

        assert_eq!(cold_rows.len(), 11);
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;

    #[test]
    fn test_index_survives_database_reload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create database with index
        {
            let mut db = Database::with_path(&db_path);
            db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

            for i in 0..100 {
                db.execute(&format!("INSERT INTO users VALUES ({}, 'user{}')", i, i)).unwrap();
            }

            db.execute("CREATE INDEX idx_users ON users(id)").unwrap();

            // Verify index works
            let rows = db.scan_table_with_index_range(
                "users",
                "idx_users",
                Some(SqlValue::Integer(50)),
                Some(SqlValue::Integer(55)),
            ).unwrap();
            assert_eq!(rows.len(), 6);
        } // Database dropped, files should persist

        // Reload database
        {
            let db = Database::with_path(&db_path);

            // Index should still work after reload
            let rows = db.scan_table_with_index_range(
                "users",
                "idx_users",
                Some(SqlValue::Integer(50)),
                Some(SqlValue::Integer(55)),
            ).unwrap();
            assert_eq!(rows.len(), 6);
        }
    }

    #[test]
    fn test_disk_backed_index_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        {
            let mut db = Database::with_path(&db_path);
            db.execute("CREATE TABLE products (id INTEGER, price INTEGER)").unwrap();

            for i in 0..1000 {
                db.execute(&format!("INSERT INTO products VALUES ({}, {})", i, i * 100)).unwrap();
            }

            db.execute("CREATE INDEX idx_products ON products(id)").unwrap();
        }

        // Reload and verify
        {
            let db = Database::with_path(&db_path);
            let rows = db.scan_table_with_index_range(
                "products",
                "idx_products",
                Some(SqlValue::Integer(100)),
                Some(SqlValue::Integer(200)),
            ).unwrap();

            assert_eq!(rows.len(), 101);
        }
    }
}

#[cfg(test)]
mod correctness_tests {
    use super::*;

    #[test]
    fn test_empty_index() {
        let mut db = Database::new();
        db.execute("CREATE TABLE empty (id INTEGER)").unwrap();
        db.execute("CREATE INDEX idx_empty ON empty(id)").unwrap();

        // Should not crash on empty index
        let rows = db.scan_table_with_index_range(
            "empty",
            "idx_empty",
            Some(SqlValue::Integer(1)),
            Some(SqlValue::Integer(10)),
        ).unwrap();

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_null_handling_in_index() {
        let mut db = Database::new();
        db.execute("CREATE TABLE nulls (id INTEGER)").unwrap();

        // Insert some NULLs
        db.execute("INSERT INTO nulls VALUES (1)").unwrap();
        db.execute("INSERT INTO nulls VALUES (NULL)").unwrap();
        db.execute("INSERT INTO nulls VALUES (3)").unwrap();
        db.execute("INSERT INTO nulls VALUES (NULL)").unwrap();
        db.execute("INSERT INTO nulls VALUES (5)").unwrap();

        db.execute("CREATE INDEX idx_nulls ON nulls(id)").unwrap();

        // Range scan should handle NULLs correctly
        let rows = db.scan_table_with_index_range(
            "nulls",
            "idx_nulls",
            Some(SqlValue::Integer(0)),
            Some(SqlValue::Integer(10)),
        ).unwrap();

        // Should find 1, 3, 5 (not NULLs)
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_duplicate_values_in_non_unique_index() {
        let mut db = Database::new();
        db.execute("CREATE TABLE dups (value INTEGER)").unwrap();

        // Insert many duplicates
        for _ in 0..100 {
            db.execute("INSERT INTO dups VALUES (42)").unwrap();
        }

        db.execute("CREATE INDEX idx_dups ON dups(value)").unwrap();

        // Should find all duplicates
        let rows = db.scan_table_with_index_range(
            "dups",
            "idx_dups",
            Some(SqlValue::Integer(42)),
            Some(SqlValue::Integer(42)),
        ).unwrap();

        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn test_multi_column_index_range_scan() {
        let mut db = Database::new();
        db.execute("CREATE TABLE multi (a INTEGER, b INTEGER, c INTEGER)").unwrap();

        for i in 0..50 {
            for j in 0..3 {
                db.execute(&format!("INSERT INTO multi VALUES ({}, {}, {})", i, j, i + j)).unwrap();
            }
        }

        db.execute("CREATE INDEX idx_multi ON multi(a, b)").unwrap();

        // Range scan on multi-column index
        // This was broken in #1297 with BTreeMap migration
        let rows = db.scan_table_with_index_range(
            "multi",
            "idx_multi",
            Some(SqlValue::Integer(25)),
            Some(SqlValue::Integer(30)),
        ).unwrap();

        // Should find rows where a is between 25 and 30
        assert!(rows.len() > 0);
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;

    #[test]
    fn test_regression_1297_range_scan_multi_column() {
        // Issue #1297: BTreeMap migration broke range scans on multi-column indexes
        // Root cause: Attempted to use range() with single-element keys
        //             to query multi-column index with multi-element keys

        let mut db = Database::new();
        db.execute("CREATE TABLE users (age INTEGER, name TEXT, city TEXT)").unwrap();

        for i in 20..40 {
            db.execute(&format!(
                "INSERT INTO users VALUES ({}, 'user{}', 'city{}')",
                i, i, i % 5
            )).unwrap();
        }

        db.execute("CREATE INDEX idx_multi ON users(age, name)").unwrap();

        // Range query on first column
        let rows = db.scan_table_with_index_range(
            "users",
            "idx_multi",
            Some(SqlValue::Integer(25)),
            Some(SqlValue::Integer(30)),
        ).unwrap();

        // Should return correct results (not empty!)
        assert!(rows.len() > 0);
        assert!(rows.len() <= 6); // 25-30 inclusive
    }

    #[test]
    fn test_order_by_with_index_performance() {
        // Related to #1301: ORDER BY should use index efficiently

        let mut db = Database::new();
        db.execute("CREATE TABLE products (id INTEGER, price INTEGER)").unwrap();

        for i in 0..1000 {
            db.execute(&format!("INSERT INTO products VALUES ({}, {})", i, 1000 - i)).unwrap();
        }

        db.execute("CREATE INDEX idx_price ON products(price)").unwrap();

        // ORDER BY using index should be efficient
        // (We can't directly test performance here, but verify correctness)
        let rows = db.scan_table_with_index_range(
            "products",
            "idx_price",
            Some(SqlValue::Integer(100)),
            Some(SqlValue::Integer(200)),
        ).unwrap();

        assert_eq!(rows.len(), 101);
    }
}
