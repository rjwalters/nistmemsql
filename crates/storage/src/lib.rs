//! Storage - In-Memory Data Storage
//!
//! This crate provides in-memory storage for database tables and rows.

pub mod database;
pub mod error;
pub mod row;
pub mod table;

pub use database::{Database, TransactionState};
pub use error::StorageError;
pub use row::Row;
pub use table::Table;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use types::SqlValue;
    use catalog::{ColumnSchema, DataType, TableSchema};

    #[test]
    fn test_hash_indexes_primary_key() {
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar(100), false),
            ],
            vec!["id".to_string()],
        );

        let mut table = Table::new(schema);

        // Insert some rows
        for i in 0..10 {
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(format!("User {}", i)),
            ]);
            table.insert(row).unwrap();
        }

        // Check that primary key index exists and has entries
        assert!(table.primary_key_index.is_some());
        assert_eq!(table.primary_key_index.as_ref().unwrap().len(), 10);

        // Try to insert duplicate - should work at table level (constraint check is in executor)
        let duplicate_row = Row::new(vec![
            SqlValue::Integer(0),
            SqlValue::Varchar("Duplicate User".to_string()),
        ]);
        table.insert(duplicate_row).unwrap(); // This succeeds because constraint checking is in executor
    }

    #[test]
    fn test_hash_indexes_unique_constraints() {
        let schema = TableSchema::with_unique_constraints(
            "products".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("sku".to_string(), DataType::Varchar(50), false),
            ],
            vec![vec!["sku".to_string()]], // Unique constraint on sku
        );

        let mut table = Table::new(schema);

        // Insert some rows
        for i in 0..5 {
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(format!("SKU{}", i)),
            ]);
            table.insert(row).unwrap();
        }

        // Check that unique index exists and has entries
        assert_eq!(table.unique_indexes.len(), 1);
        assert_eq!(table.unique_indexes[0].len(), 5);
    }
}
