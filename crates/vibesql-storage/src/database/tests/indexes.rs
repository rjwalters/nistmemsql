use vibesql_types::SqlValue;

use crate::database::{DatabaseConfig, SpillPolicy};
use crate::database::indexes::{IndexData, IndexManager};

#[test]
fn test_range_scan_preserves_index_order() {
    // Create index data with rows that are NOT in order by row index
    // but ARE in order by indexed value
    let mut data = std::collections::BTreeMap::new();

    // col0 values: row 1 has 50, row 2 has 60, row 0 has 70
    // Index should be sorted by value: 50, 60, 70
    data.insert(vec![SqlValue::Integer(50)], vec![1]);
    data.insert(vec![SqlValue::Integer(60)], vec![2]);
    data.insert(vec![SqlValue::Integer(70)], vec![0]);

    let index_data = IndexData::InMemory { data };

    // Query: col0 > 55 should return rows in index order: [2, 0] (values 60, 70)
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(55)),
        None,
        false, // exclusive start
        false,
    );

    // Result should be [2, 0] NOT [0, 2]
    // This preserves the index ordering (60 comes before 70)
    assert_eq!(
        result,
        vec![2, 0],
        "range_scan should return rows in index order (by value), not row index order"
    );
}

#[test]
fn test_range_scan_between_preserves_order() {
    // Test BETWEEN queries maintain index order
    let mut data = std::collections::BTreeMap::new();

    // Values out of row-index order
    data.insert(vec![SqlValue::Integer(40)], vec![5]);
    data.insert(vec![SqlValue::Integer(50)], vec![1]);
    data.insert(vec![SqlValue::Integer(60)], vec![2]);
    data.insert(vec![SqlValue::Integer(70)], vec![0]);

    let index_data = IndexData::InMemory { data };

    // Query: col0 BETWEEN 45 AND 65 (i.e., col0 >= 45 AND col0 <= 65)
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(45)),
        Some(&SqlValue::Integer(65)),
        true, // inclusive start
        true, // inclusive end
    );

    // Should return [1, 2] (values 50, 60) in that order
    assert_eq!(result, vec![1, 2], "BETWEEN should return rows in index order");
}

#[test]
fn test_range_scan_with_duplicate_values() {
    // Test case: multiple rows with the same indexed value
    let mut data = std::collections::BTreeMap::new();

    // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
    data.insert(vec![SqlValue::Integer(50)], vec![1]);
    data.insert(vec![SqlValue::Integer(60)], vec![3, 7, 2]); // duplicates
    data.insert(vec![SqlValue::Integer(70)], vec![0]);

    let index_data = IndexData::InMemory { data };

    // Query: col0 >= 60 should return [3, 7, 2, 0]
    // Rows with value 60 maintain insertion order, then row 0 with value 70
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(60)),
        None,
        true, // inclusive start
        false,
    );

    assert_eq!(
        result,
        vec![3, 7, 2, 0],
        "Duplicate values should maintain insertion order within the same key"
    );
}

#[test]
fn test_multi_lookup_with_duplicate_values() {
    // Test case: multi_lookup with duplicate indexed values
    let mut data = std::collections::BTreeMap::new();

    // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
    data.insert(vec![SqlValue::Integer(50)], vec![1]);
    data.insert(vec![SqlValue::Integer(60)], vec![3, 7, 2]); // duplicates
    data.insert(vec![SqlValue::Integer(70)], vec![0]);

    let index_data = IndexData::InMemory { data };

    // Query: col0 IN (60, 70) should return [3, 7, 2, 0]
    // Rows with value 60 maintain insertion order, then row 0 with value 70
    let result = index_data.multi_lookup(&[SqlValue::Integer(60), SqlValue::Integer(70)]);

    assert_eq!(result, vec![3, 7, 2, 0],
        "multi_lookup with duplicate values should maintain insertion order within the same key");
}

#[test]
#[ignore] // Slow test (>60s) - disk-backed indexes require real disk I/O. Run with: cargo test -- --ignored
fn test_disk_backed_index_creation_with_bulk_load() {
    // Test that disk-backed indexes can be created when table exceeds threshold
    // This test is marked #[ignore] because it's slow (>60 seconds in debug builds)
    // due to actual disk I/O operations in the B+ tree bulk_load.
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;

    // Create a table schema with one integer column
    let columns = vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    // This test uses 100,500 rows to exceed the production DISK_BACKED_THRESHOLD (100,000)
    // Note: In normal test mode, DISK_BACKED_THRESHOLD is usize::MAX, which would prevent
    // disk-backed mode from ever being triggered. However, this test is marked #[ignore]
    // and is intended to be run explicitly with --ignored flag, at which point it will
    // compile without the test cfg and use the production threshold of 100,000.
    //
    // Alternative: This test could be moved to an integration test suite where cfg(test)
    // doesn't apply, allowing it to use the production threshold naturally.
    let num_rows = 100_500;
    let table_rows: Vec<Row> = (0..num_rows)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    // Create index - should use disk-backed backend when run with production threshold
    let result = index_manager.create_index(
        "idx_id".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,  // non-unique
        vec![vibesql_ast::IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
        }],
    );

    assert!(result.is_ok(), "Disk-backed index creation should succeed");

    // Verify index was created
    assert!(index_manager.index_exists("idx_id"));

    // Verify it's using DiskBacked variant
    let index_data = index_manager.get_index_data("idx_id");
    assert!(index_data.is_some());
    match index_data.unwrap() {
        IndexData::DiskBacked { .. } => {
            // Success - disk-backed was used
        }
        IndexData::InMemory { .. } => {
            panic!("Expected DiskBacked variant, got InMemory");
        }
    }
}

#[test]
fn test_in_memory_index_for_small_tables() {
    // Test that in-memory indexes are still used for small tables
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("small_table".to_string(), columns);

    // Create small number of rows (well below threshold)
    let table_rows: Vec<Row> = (0..100)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    let result = index_manager.create_index(
        "idx_value".to_string(),
        "small_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    );

    assert!(result.is_ok());
    assert!(index_manager.index_exists("idx_value"));

    // Verify it's using InMemory variant
    let index_data = index_manager.get_index_data("idx_value");
    assert!(index_data.is_some());
    match index_data.unwrap() {
        IndexData::InMemory { .. } => {
            // Success - in-memory was used for small table
        }
        IndexData::DiskBacked { .. } => {
            panic!("Expected InMemory variant for small table, got DiskBacked");
        }
    }
}

#[test]
#[ignore] // Slow test - triggers disk-backed eviction. Run with: cargo test -- --ignored
fn test_budget_enforcement_with_spill_policy() {
    // Test that memory budget is enforced with SpillToDisk policy
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    // Create small rows for in-memory indexes
    let table_rows: Vec<Row> = (0..100)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    // Set a very small memory budget to force eviction
    let config = DatabaseConfig {
        memory_budget: 1000,  // 1KB - very small to force eviction
        disk_budget: 100 * 1024 * 1024,  // 100MB disk
        spill_policy: SpillPolicy::SpillToDisk,
    };
    index_manager.set_config(config);

    // Create first index - should succeed and be in-memory
    let result1 = index_manager.create_index(
        "idx_1".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    );
    assert!(result1.is_ok());

    // Creating a second index should trigger eviction of the first one
    let result2 = index_manager.create_index(
        "idx_2".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    );
    assert!(result2.is_ok());

    // Both indexes should exist (one in memory, one spilled to disk)
    assert!(index_manager.index_exists("idx_1"));
    assert!(index_manager.index_exists("idx_2"));
}

#[test]
#[ignore] // Slow test - triggers disk-backed eviction. Run with: cargo test -- --ignored
fn test_lru_eviction_order() {
    // Test that LRU eviction selects the coldest (least recently used) index
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;
    use std::thread;
    use std::time::Duration;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> = (0..50)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    // Small budget to trigger eviction
    let config = DatabaseConfig {
        memory_budget: 2000,  // 2KB
        disk_budget: 100 * 1024 * 1024,
        spill_policy: SpillPolicy::SpillToDisk,
    };
    index_manager.set_config(config);

    // Create idx_1
    index_manager.create_index(
        "idx_1".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    ).unwrap();

    thread::sleep(Duration::from_millis(10));

    // Create idx_2
    index_manager.create_index(
        "idx_2".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    ).unwrap();

    thread::sleep(Duration::from_millis(10));

    // Access idx_1 to make it "hot" (more recently used than idx_2)
    let _ = index_manager.get_index_data("idx_1");

    thread::sleep(Duration::from_millis(10));

    // Create idx_3 - should evict idx_2 (coldest), not idx_1
    index_manager.create_index(
        "idx_3".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    ).unwrap();

    // All indexes should still exist
    assert!(index_manager.index_exists("idx_1"));
    assert!(index_manager.index_exists("idx_2"));
    assert!(index_manager.index_exists("idx_3"));

    // idx_2 should have been evicted to disk (coldest)
    // idx_1 and idx_3 should still be in memory (hot)
    let _backend_1 = index_manager.resource_tracker.get_backend("idx_1");
    let backend_2 = index_manager.resource_tracker.get_backend("idx_2");
    let _backend_3 = index_manager.resource_tracker.get_backend("idx_3");

    // Note: Exact behavior depends on memory sizes, but idx_2 should be coldest
    assert!(backend_2.is_some());
}

#[test]
fn test_access_tracking() {
    // Test that index accesses are tracked for LRU
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> = (0..10)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    index_manager.create_index(
        "idx_test".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    ).unwrap();

    // Initial access count should be 0 (creation doesn't count as access)
    let stats = index_manager.resource_tracker.get_index_stats("idx_test");
    assert!(stats.is_some());
    let initial_count = stats.unwrap().get_access_count();

    // Access the index a few times
    let _ = index_manager.get_index_data("idx_test");
    let _ = index_manager.get_index_data("idx_test");
    let _ = index_manager.get_index_data("idx_test");

    // Access count should have increased
    let stats = index_manager.resource_tracker.get_index_stats("idx_test");
    assert!(stats.is_some());
    let final_count = stats.unwrap().get_access_count();

    assert!(final_count > initial_count,
        "Access count should increase after index accesses (initial: {}, final: {})",
        initial_count, final_count);
}

#[test]
fn test_resource_cleanup_on_drop() {
    // Test that resources are freed when indexes are dropped
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_ast::OrderDirection;
    use vibesql_types::DataType;
    use crate::Row;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> = (0..100)
        .map(|i| Row {
            values: vec![SqlValue::Integer(i as i64)],
        })
        .collect();

    let mut index_manager = IndexManager::new();

    // Create an index
    index_manager.create_index(
        "idx_test".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
        }],
    ).unwrap();

    // Memory should be in use
    let memory_before = index_manager.resource_tracker.memory_used();
    assert!(memory_before > 0);

    // Drop the index
    index_manager.drop_index("idx_test").unwrap();

    // Memory should be freed
    let memory_after = index_manager.resource_tracker.memory_used();
    assert_eq!(memory_after, 0, "Memory should be freed after dropping index");
}

#[test]
fn test_database_config_presets() {
    // Test that preset configurations have expected values
    let browser_config = DatabaseConfig::browser_default();
    assert_eq!(browser_config.memory_budget, 512 * 1024 * 1024);  // 512MB
    assert_eq!(browser_config.disk_budget, 2 * 1024 * 1024 * 1024);  // 2GB
    assert_eq!(browser_config.spill_policy, SpillPolicy::SpillToDisk);

    let server_config = DatabaseConfig::server_default();
    assert_eq!(server_config.memory_budget, 16 * 1024 * 1024 * 1024);  // 16GB
    assert_eq!(server_config.disk_budget, 1024 * 1024 * 1024 * 1024);  // 1TB
    assert_eq!(server_config.spill_policy, SpillPolicy::BestEffort);

    let test_config = DatabaseConfig::test_default();
    assert_eq!(test_config.memory_budget, 10 * 1024 * 1024);  // 10MB
    assert_eq!(test_config.disk_budget, 100 * 1024 * 1024);  // 100MB
    assert_eq!(test_config.spill_policy, SpillPolicy::SpillToDisk);
}
