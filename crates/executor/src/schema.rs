use std::collections::HashMap;

/// Represents the combined schema from multiple tables (for JOINs)
#[derive(Debug, Clone)]
pub(crate) struct CombinedSchema {
    /// Map from table name to (start_index, TableSchema)
    /// start_index is where this table's columns begin in the combined row
    pub(crate) table_schemas: HashMap<String, (usize, catalog::TableSchema)>,
    /// Total number of columns across all tables
    pub(crate) total_columns: usize,
}

impl CombinedSchema {
    /// Create a new combined schema from a single table
    pub(crate) fn from_table(table_name: String, schema: catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        table_schemas.insert(table_name, (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Create a new combined schema from a derived table (subquery result)
    pub(crate) fn from_derived_table(
        alias: String,
        column_names: Vec<String>,
        column_types: Vec<types::DataType>,
    ) -> Self {
        let total_columns = column_names.len();

        // Build column definitions
        let columns: Vec<catalog::ColumnSchema> = column_names
            .into_iter()
            .zip(column_types)
            .map(|(name, data_type)| catalog::ColumnSchema {
                name,
                data_type,
                nullable: true, // Derived table columns are always nullable
            })
            .collect();

        let schema = catalog::TableSchema {
            name: alias.clone(),
            columns,
            primary_key: None,
        };
        let mut table_schemas = HashMap::new();
        table_schemas.insert(alias, (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Combine two schemas (for JOIN operations)
    pub(crate) fn combine(
        left: CombinedSchema,
        right_table: String,
        right_schema: catalog::TableSchema,
    ) -> Self {
        let mut table_schemas = left.table_schemas;
        let left_total = left.total_columns;
        let right_columns = right_schema.columns.len();
        table_schemas.insert(right_table, (left_total, right_schema));
        CombinedSchema { table_schemas, total_columns: left_total + right_columns }
    }

    /// Look up a column by name (optionally qualified with table name)
    pub(crate) fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            if let Some((start_index, schema)) = self.table_schemas.get(table_name) {
                schema.get_column_index(column).map(|idx| start_index + idx)
            } else {
                None
            }
        } else {
            // Unqualified column reference - search all tables
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    return Some(start_index + idx);
                }
            }
            None
        }
    }
}
