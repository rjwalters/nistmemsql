//! Table-level statistics

use std::collections::HashMap;
use std::time::SystemTime;
use super::ColumnStatistics;

/// Statistics for an entire table
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Total number of rows
    pub row_count: usize,

    /// Per-column statistics
    pub columns: HashMap<String, ColumnStatistics>,

    /// Timestamp when stats were last updated
    pub last_updated: SystemTime,

    /// Whether stats are stale (need recomputation)
    pub is_stale: bool,
}

impl TableStatistics {
    /// Compute statistics by scanning the table
    pub fn compute(
        rows: &[crate::Row],
        schema: &vibesql_catalog::TableSchema,
    ) -> Self {
        let row_count = rows.len();
        let mut columns = HashMap::new();

        for (idx, column) in schema.columns.iter().enumerate() {
            let col_stats = ColumnStatistics::compute(rows, idx);
            columns.insert(column.name.clone(), col_stats);
        }

        TableStatistics {
            row_count,
            columns,
            last_updated: SystemTime::now(),
            is_stale: false,
        }
    }

    /// Mark statistics as stale after significant data changes
    pub fn mark_stale(&mut self) {
        self.is_stale = true;
    }

    /// Check if statistics should be recomputed
    ///
    /// Returns true if stats are marked stale or too old
    pub fn needs_refresh(&self) -> bool {
        self.is_stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    #[test]
    fn test_table_statistics() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true,
                ),
            ],
        );

        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Alice".to_string())]),
        ];

        let stats = TableStatistics::compute(&rows, &schema);

        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns.len(), 2);
        assert!(!stats.is_stale);

        // Check column stats
        let id_stats = stats.columns.get("id").unwrap();
        assert_eq!(id_stats.n_distinct, 3);

        let name_stats = stats.columns.get("name").unwrap();
        assert_eq!(name_stats.n_distinct, 2); // Alice, Bob
    }

    #[test]
    fn test_mark_stale() {
        let schema = TableSchema::new("test".to_string(), vec![]);

        let mut stats = TableStatistics::compute(&[], &schema);
        assert!(!stats.is_stale);
        assert!(!stats.needs_refresh());

        stats.mark_stale();
        assert!(stats.is_stale);
        assert!(stats.needs_refresh());
    }
}
