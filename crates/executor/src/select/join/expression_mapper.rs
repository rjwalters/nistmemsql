//! Expression mapper for tracking schema evolution during join execution
//!
//! As joins are executed, the combined schema grows. This module provides utilities
//! to track column positions and validate that expressions can be evaluated in the
//! current schema state.
//!
//! Example:
//! ```
//! let mut mapper = ExpressionMapper::new();
//! mapper.add_table("users", &users_schema);  // cols 0-1
//! mapper.add_table("orders", &orders_schema); // cols 2-3
//!
//! // Now users.id resolves to index 0, orders.user_id to index 3
//! let idx = mapper.resolve_column(Some("users"), "id");  // Some(0)
//! ```

use std::collections::{HashMap, HashSet};
use ast::Expression;
use catalog::TableSchema;

/// Tracks column positions as schemas evolve during join execution
///
/// Maintains a mapping from (table_name, column_name) to its index in the
/// combined schema. Updates this mapping after each join operation.
#[derive(Debug, Clone)]
pub struct ExpressionMapper {
    /// Map from (table_name, column_name) to index in combined schema
    column_positions: HashMap<(String, String), usize>,
    /// Offset where each table starts in combined schema
    table_offsets: HashMap<String, usize>,
    /// Current total columns
    total_columns: usize,
}

impl ExpressionMapper {
    /// Create a new empty mapper
    pub fn new() -> Self {
        Self {
            column_positions: HashMap::new(),
            table_offsets: HashMap::new(),
            total_columns: 0,
        }
    }

    /// Add a table to the schema (happens after each join)
    ///
    /// Registers all columns from the table and updates the total column count.
    pub fn add_table(&mut self, table_name: &str, schema: &TableSchema) {
        let offset = self.total_columns;
        let table_lower = table_name.to_lowercase();
        self.table_offsets.insert(table_lower.clone(), offset);

        for (idx, column) in schema.columns.iter().enumerate() {
            let key = (table_lower.clone(), column.name.to_lowercase());
            self.column_positions.insert(key, offset + idx);
        }

        self.total_columns += schema.columns.len();
    }

    /// Resolve a table-qualified column to its index in current schema
    ///
    /// Returns Some(index) if the column exists, None otherwise.
    /// If table is None, tries to find an unqualified column by name.
    pub fn resolve_column(&self, table: Option<&str>, column: &str) -> Option<usize> {
        let table_lower = table.map(|t| t.to_lowercase());
        let column_lower = column.to_lowercase();

        // Try exact match first (table.column)
        if let Some(tbl) = table_lower {
            if let Some(&idx) = self.column_positions.get(&(tbl, column_lower.clone())) {
                return Some(idx);
            }
        }

        // Try unqualified (just column name) - find first match
        for ((_, col), &idx) in &self.column_positions {
            if col == &column_lower {
                return Some(idx);
            }
        }

        None
    }

    /// Get all tables currently in the mapper
    pub fn tables(&self) -> HashSet<String> {
        self.table_offsets.keys().cloned().collect()
    }

    /// Get the offset where a specific table starts in the combined schema
    pub fn table_offset(&self, table: &str) -> Option<usize> {
        self.table_offsets.get(&table.to_lowercase()).copied()
    }

    /// Get the total number of columns in the combined schema
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }

    /// Analyze an expression to understand what it references
    ///
    /// Returns information about which tables the expression references
    /// and whether all column references are resolvable in the current schema.
    pub fn analyze_expression(&self, expr: &Expression) -> ExpressionAnalysis {
        let mut tables_referenced = HashSet::new();
        let mut columns_used = Vec::new();
        let mut resolvable = true;

        self.walk_expression(expr, &mut tables_referenced, &mut columns_used, &mut resolvable);

        ExpressionAnalysis {
            tables_referenced,
            columns_used,
            all_resolvable: resolvable,
        }
    }

    /// Check if an expression only references tables in a specific set
    ///
    /// Useful for determining if an expression can be applied after certain joins.
    pub fn expression_refs_only_tables(
        &self,
        expr: &Expression,
        allowed_tables: &HashSet<String>,
    ) -> bool {
        let analysis = self.analyze_expression(expr);
        analysis.all_resolvable
            && analysis
                .tables_referenced
                .iter()
                .all(|t| allowed_tables.contains(t))
    }

    /// Walk an expression tree, collecting table and column references
    fn walk_expression(
        &self,
        expr: &Expression,
        tables: &mut HashSet<String>,
        columns: &mut Vec<(String, String)>,
        resolvable: &mut bool,
    ) {
        match expr {
            Expression::ColumnRef { table, column } => {
                let column_lower = column.to_lowercase();
                if let Some(t) = table {
                    let table_lower = t.to_lowercase();
                    tables.insert(table_lower.clone());
                    columns.push((table_lower.clone(), column_lower.clone()));
                    // Check if this column is in current schema
                    if self.resolve_column(Some(t), column).is_none() {
                        *resolvable = false;
                    }
                } else {
                    // Unqualified column - try to resolve
                    if let Some(idx) = self.resolve_column(None, column) {
                        columns.push(("*".to_string(), column_lower.clone()));
                        // Find which table this column belongs to
                        for ((tbl, col), &col_idx) in &self.column_positions {
                            if col_idx == idx {
                                tables.insert(tbl.clone());
                                break;
                            }
                        }
                    } else {
                        *resolvable = false;
                    }
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.walk_expression(left, tables, columns, resolvable);
                self.walk_expression(right, tables, columns, resolvable);
            }
            Expression::UnaryOp { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    self.walk_expression(arg, tables, columns, resolvable);
                }
            }
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    self.walk_expression(arg, tables, columns, resolvable);
                }
            }
            Expression::IsNull { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    self.walk_expression(op, tables, columns, resolvable);
                }
                for when_clause in when_clauses {
                    // Handle multiple conditions in this when clause
                    for condition in &when_clause.conditions {
                        self.walk_expression(condition, tables, columns, resolvable);
                    }
                    self.walk_expression(&when_clause.result, tables, columns, resolvable);
                }
                if let Some(else_expr) = else_result {
                    self.walk_expression(else_expr, tables, columns, resolvable);
                }
            }
            Expression::InList { expr: e, values, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                for value in values {
                    self.walk_expression(value, tables, columns, resolvable);
                }
            }
            Expression::In { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                // Subquery joins can reference outer columns - mark as not reliably resolvable
                *resolvable = false;
            }
            Expression::Between { expr: e, low, high, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                self.walk_expression(low, tables, columns, resolvable);
                self.walk_expression(high, tables, columns, resolvable);
            }
            Expression::Cast { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Position { substring, string, .. } => {
                self.walk_expression(substring, tables, columns, resolvable);
                self.walk_expression(string, tables, columns, resolvable);
            }
            Expression::Trim { removal_char, string, .. } => {
                if let Some(rc) = removal_char {
                    self.walk_expression(rc, tables, columns, resolvable);
                }
                self.walk_expression(string, tables, columns, resolvable);
            }
            Expression::Like { expr: e, pattern, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                self.walk_expression(pattern, tables, columns, resolvable);
            }
            Expression::ScalarSubquery(_) | Expression::Exists { .. }
            | Expression::QuantifiedComparison { .. } => {
                // Subqueries can reference outer columns - mark as not resolvable
                *resolvable = false;
            }
            Expression::WindowFunction { .. } => {
                // Window functions are complex - conservative approach
                *resolvable = false;
            }
            // Literals and other terminal expressions don't reference columns
            Expression::Literal(_) | Expression::Wildcard | Expression::CurrentDate
            | Expression::CurrentTime { .. } | Expression::CurrentTimestamp { .. }
            | Expression::Default | Expression::NextValue { .. } => {
                // No column references
            }
        }
    }
}

/// Analysis of an expression's column references
#[derive(Debug, Clone)]
pub struct ExpressionAnalysis {
    /// Tables referenced in the expression
    pub tables_referenced: HashSet<String>,
    /// Columns used: (table, column) pairs
    pub columns_used: Vec<(String, String)>,
    /// Whether all column references are resolvable in current schema
    pub all_resolvable: bool,
}

impl ExpressionAnalysis {
    /// Check if expression only references columns from a single table
    pub fn is_single_table(&self) -> bool {
        self.tables_referenced.len() == 1
    }

    /// Get the single table if this expression references only one table
    pub fn single_table(&self) -> Option<String> {
        if self.is_single_table() {
            self.tables_referenced.iter().next().cloned()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::ColumnSchema;
    use types::DataType;

    fn create_test_schema(name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
        let cols = columns
            .into_iter()
            .map(|(n, t)| ColumnSchema::new(n.to_string(), t, false))
            .collect();
        TableSchema::new(name.to_string(), cols)
    }

    #[test]
    fn test_single_table_resolution() {
        let mut mapper = ExpressionMapper::new();
        let varchar_100 = DataType::Varchar { max_length: Some(100) };
        let schema = create_test_schema(
            "users",
            vec![("id", DataType::Integer), ("name", varchar_100.clone())],
        );
        mapper.add_table("users", &schema);

        assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
        assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));
        assert_eq!(mapper.resolve_column(Some("users"), "missing"), None);
    }

    #[test]
    fn test_single_table_unqualified_column() {
        let mut mapper = ExpressionMapper::new();
        let varchar_100 = DataType::Varchar { max_length: Some(100) };
        let schema = create_test_schema(
            "users",
            vec![("id", DataType::Integer), ("name", varchar_100.clone())],
        );
        mapper.add_table("users", &schema);

        // Unqualified columns should resolve to first match
        assert_eq!(mapper.resolve_column(None, "id"), Some(0));
        assert_eq!(mapper.resolve_column(None, "name"), Some(1));
    }

    #[test]
    fn test_two_table_resolution() {
        let mut mapper = ExpressionMapper::new();
        let varchar_100 = DataType::Varchar { max_length: Some(100) };

        let users_schema =
            create_test_schema("users", vec![("id", DataType::Integer), ("name", varchar_100.clone())]);
        mapper.add_table("users", &users_schema);

        let orders_schema = create_test_schema(
            "orders",
            vec![("id", DataType::Integer), ("user_id", DataType::Integer)],
        );
        mapper.add_table("orders", &orders_schema);

        // Users columns at 0-1
        assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
        assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));

        // Orders columns at 2-3
        assert_eq!(mapper.resolve_column(Some("orders"), "id"), Some(2));
        assert_eq!(mapper.resolve_column(Some("orders"), "user_id"), Some(3));
    }

    #[test]
    fn test_case_insensitive_resolution() {
        let mut mapper = ExpressionMapper::new();
        let varchar_100 = DataType::Varchar { max_length: Some(100) };
        let schema =
            create_test_schema("users", vec![("ID", DataType::Integer), ("Name", varchar_100.clone())]);
        mapper.add_table("USERS", &schema);

        // Case variations should resolve
        assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
        assert_eq!(mapper.resolve_column(Some("USERS"), "ID"), Some(0));
        assert_eq!(mapper.resolve_column(Some("Users"), "name"), Some(1));
    }

    #[test]
    fn test_total_columns() {
        let mut mapper = ExpressionMapper::new();

        assert_eq!(mapper.total_columns(), 0);

        let schema1 = create_test_schema("t1", vec![("a", DataType::Integer), ("b", DataType::Integer)]);
        mapper.add_table("t1", &schema1);
        assert_eq!(mapper.total_columns(), 2);

        let schema2 = create_test_schema("t2", vec![("c", DataType::Integer)]);
        mapper.add_table("t2", &schema2);
        assert_eq!(mapper.total_columns(), 3);
    }

    #[test]
    fn test_table_offset() {
        let mut mapper = ExpressionMapper::new();

        let schema1 = create_test_schema("t1", vec![("a", DataType::Integer), ("b", DataType::Integer)]);
        mapper.add_table("t1", &schema1);

        let schema2 = create_test_schema("t2", vec![("c", DataType::Integer)]);
        mapper.add_table("t2", &schema2);

        assert_eq!(mapper.table_offset("t1"), Some(0));
        assert_eq!(mapper.table_offset("t2"), Some(2));
    }

    #[test]
    fn test_expression_analysis_single_table() {
        let mut mapper = ExpressionMapper::new();
        let schema = create_test_schema("users", vec![("id", DataType::Integer)]);
        mapper.add_table("users", &schema);

        // Single column reference
        let expr = Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        };

        let analysis = mapper.analyze_expression(&expr);
        assert!(analysis.all_resolvable);
        assert_eq!(analysis.tables_referenced.len(), 1);
        assert_eq!(analysis.single_table(), Some("users".to_string()));
    }

    #[test]
    fn test_expression_analysis_two_tables() {
        let mut mapper = ExpressionMapper::new();
        mapper.add_table(
            "users",
            &create_test_schema("users", vec![("id", DataType::Integer)]),
        );
        mapper.add_table(
            "orders",
            &create_test_schema("orders", vec![("user_id", DataType::Integer)]),
        );

        // Binary operation: users.id = orders.user_id
        let expr = Expression::BinaryOp {
            op: ast::BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "user_id".to_string(),
            }),
        };

        let analysis = mapper.analyze_expression(&expr);
        assert!(analysis.all_resolvable);
        assert_eq!(analysis.tables_referenced.len(), 2);
        assert!(!analysis.is_single_table());
    }

    #[test]
    fn test_expression_analysis_unresolvable() {
        let mapper = ExpressionMapper::new();

        // Try to reference non-existent column
        let expr = Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        };

        let analysis = mapper.analyze_expression(&expr);
        assert!(!analysis.all_resolvable);
    }

    #[test]
    fn test_expression_refs_only_tables() {
        let mut mapper = ExpressionMapper::new();
        mapper.add_table(
            "users",
            &create_test_schema("users", vec![("id", DataType::Integer)]),
        );
        mapper.add_table(
            "orders",
            &create_test_schema("orders", vec![("user_id", DataType::Integer)]),
        );
        mapper.add_table(
            "items",
            &create_test_schema("items", vec![("order_id", DataType::Integer)]),
        );

        let expr = Expression::BinaryOp {
            op: ast::BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "user_id".to_string(),
            }),
        };

        let mut allowed = HashSet::new();
        allowed.insert("users".to_string());
        allowed.insert("orders".to_string());

        assert!(mapper.expression_refs_only_tables(&expr, &allowed));

        // Remove one table from allowed set
        allowed.remove("orders");
        assert!(!mapper.expression_refs_only_tables(&expr, &allowed));
    }

    #[test]
    fn test_mapper_clone() {
        let mut mapper1 = ExpressionMapper::new();
        let schema = create_test_schema("users", vec![("id", DataType::Integer)]);
        mapper1.add_table("users", &schema);

        let mapper2 = mapper1.clone();
        assert_eq!(
            mapper1.resolve_column(Some("users"), "id"),
            mapper2.resolve_column(Some("users"), "id")
        );
    }
}
