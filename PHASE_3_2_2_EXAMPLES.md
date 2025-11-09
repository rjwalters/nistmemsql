# Phase 3.2.2 - Concrete Code Examples

## Example 1: Expression Mapping Foundation

### Problem
```sql
SELECT * FROM t1 
JOIN t2 ON t1.id = t2.t1_id
JOIN t3 ON t2.id = t3.t2_id
JOIN t4 ON t3.id = t4.t3_id;
```

During execution, join conditions reference tables that are in different schema positions:
- After `t1 ⋈ t2`: schema has `[t1.cols, t2.cols]`
- After adding `t3`: schema has `[t1.cols, t2.cols, t3.cols]`
- Condition `t3.id = t4.t3_id` references columns at indices `[4, 8]` (or similar)

### Solution: Expression Mapper

```rust
// File: crates/executor/src/select/join/expression_mapper.rs

use std::collections::HashMap;
use ast::Expression;
use catalog::TableSchema;

/// Tracks column positions as schemas evolve during join execution
pub struct ExpressionMapper {
    /// Map from (table_name, column_name) to index in combined schema
    column_positions: HashMap<(String, String), usize>,
    /// Offset where each table starts in combined schema
    table_offsets: HashMap<String, usize>,
    /// Current total columns
    total_columns: usize,
}

impl ExpressionMapper {
    pub fn new() -> Self {
        Self {
            column_positions: HashMap::new(),
            table_offsets: HashMap::new(),
            total_columns: 0,
        }
    }

    /// Add a table to the schema (happens after each join)
    pub fn add_table(&mut self, table_name: &str, schema: &TableSchema) {
        let offset = self.total_columns;
        self.table_offsets.insert(table_name.to_lowercase(), offset);

        for (idx, column) in schema.columns.iter().enumerate() {
            let key = (
                table_name.to_lowercase(),
                column.name.to_lowercase(),
            );
            self.column_positions.insert(key, offset + idx);
        }

        self.total_columns += schema.columns.len();
    }

    /// Resolve a table-qualified column to its index in current schema
    pub fn resolve_column(&self, table: Option<&str>, column: &str) -> Option<usize> {
        let table_lower = table.map(|t| t.to_lowercase());
        
        // Try exact match first (table.column)
        if let Some(tbl) = table_lower {
            if let Some(&idx) = self.column_positions.get(&(tbl, column.to_lowercase())) {
                return Some(idx);
            }
        }

        // Try unqualified (just column name) - find first match
        for ((tbl, col), &idx) in &self.column_positions {
            if col == &column.to_lowercase() {
                return Some(*idx);
            }
        }

        None
    }

    /// Analyze an expression to understand what tables it references
    pub fn analyze_expression(&self, expr: &Expression) -> ExpressionAnalysis {
        let mut tables_referenced = Vec::new();
        let mut columns_used = Vec::new();
        let mut resolvable = true;

        self.walk_expression(expr, &mut tables_referenced, &mut columns_used, &mut resolvable);

        let unique_tables: std::collections::HashSet<_> = tables_referenced.into_iter().collect();

        ExpressionAnalysis {
            tables_referenced: unique_tables,
            columns_used,
            all_resolvable: resolvable,
        }
    }

    fn walk_expression(
        &self,
        expr: &Expression,
        tables: &mut Vec<String>,
        columns: &mut Vec<(String, String)>,
        resolvable: &mut bool,
    ) {
        match expr {
            Expression::ColumnRef { table, column } => {
                if let Some(t) = table {
                    tables.push(t.to_lowercase());
                    columns.push((t.to_lowercase(), column.to_lowercase()));
                    // Check if this column is in current schema
                    if !self.resolve_column(Some(t), column).is_some() {
                        *resolvable = false;
                    }
                } else {
                    // Unqualified column - try to resolve
                    if self.resolve_column(None, column).is_some() {
                        columns.push(("*".to_string(), column.to_lowercase()));
                    } else {
                        *resolvable = false;
                    }
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.walk_expression(left, tables, columns, resolvable);
                self.walk_expression(right, tables, columns, resolvable);
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    self.walk_expression(arg, tables, columns, resolvable);
                }
            }
            _ => {} // Other expression types (literals, etc.)
        }
    }
}

pub struct ExpressionAnalysis {
    pub tables_referenced: std::collections::HashSet<String>,
    pub columns_used: Vec<(String, String)>,
    pub all_resolvable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_table_resolution() {
        let mut mapper = ExpressionMapper::new();
        let schema = TableSchema::new("users", vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
        ]);
        mapper.add_table("users", &schema);

        assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
        assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));
        assert_eq!(mapper.resolve_column(Some("users"), "missing"), None);
    }

    #[test]
    fn test_two_table_resolution() {
        let mut mapper = ExpressionMapper::new();
        
        let users_schema = TableSchema::new("users", vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
        ]);
        mapper.add_table("users", &users_schema);

        let orders_schema = TableSchema::new("orders", vec![
            Column::new("id", DataType::Integer),
            Column::new("user_id", DataType::Integer),
        ]);
        mapper.add_table("orders", &orders_schema);

        // Users columns at 0-1
        assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
        assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));
        
        // Orders columns at 2-3
        assert_eq!(mapper.resolve_column(Some("orders"), "id"), Some(2));
        assert_eq!(mapper.resolve_column(Some("orders"), "user_id"), Some(3));
    }

    #[test]
    fn test_expression_analysis() {
        let mut mapper = ExpressionMapper::new();
        // ... setup ...
        
        // Expression: users.id = orders.user_id
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
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
    }
}
```

## Example 2: Reordering Decision Logic

```rust
// File: crates/executor/src/select/join/reorder_executor.rs

use crate::select::join::{FromResult, reorder::JoinStep};
use ast::FromClause;

/// Check if we can safely apply the computed optimal order
pub fn can_reorder_safely(
    from: &FromClause,
    optimal_order: &[JoinStep],
    mapper: &ExpressionMapper,
) -> bool {
    // Only reorder linear chains for now
    if !is_linear_chain(from) {
        return false;
    }

    // Only reorder if all join conditions are resolvable in current schema
    for step in optimal_order {
        let analysis = mapper.analyze_expression(&step.condition);
        if !analysis.all_resolvable {
            return false;
        }
    }

    // Only reorder if tables involved form a connected component
    if !all_tables_in_component(from, optimal_order) {
        return false;
    }

    true
}

/// Check if FROM clause is a linear chain: A ⋈ B ⋈ C (no branching)
fn is_linear_chain(from: &FromClause) -> bool {
    match from {
        FromClause::Join { left, right, .. } => {
            // Right side must be a table
            if !matches!(right.as_ref(), FromClause::Table { .. }) {
                return false;
            }
            // Left side can be table or join
            match left.as_ref() {
                FromClause::Table { .. } => true,
                FromClause::Join { .. } => is_linear_chain(left),
                _ => false,
            }
        }
        FromClause::Table { .. } => true,
        _ => false,
    }
}

/// Verify that optimal order includes all tables from FROM clause
fn all_tables_in_component(from: &FromClause, optimal_order: &[JoinStep]) -> bool {
    let from_tables = extract_tables_from_from(from);
    let order_tables: std::collections::HashSet<_> = optimal_order
        .iter()
        .flat_map(|step| {
            let mut tables = step.left_tables.clone();
            tables.insert(step.right_table.clone());
            tables
        })
        .collect();
    
    from_tables.iter().all(|t| order_tables.contains(t))
}

fn extract_tables_from_from(from: &FromClause) -> std::collections::HashSet<String> {
    let mut tables = std::collections::HashSet::new();
    match from {
        FromClause::Table { name, .. } => {
            tables.insert(name.to_lowercase());
        }
        FromClause::Join { left, right, .. } => {
            tables.extend(extract_tables_from_from(left));
            tables.extend(extract_tables_from_from(right));
        }
        _ => {}
    }
    tables
}
```

## Example 3: Integration into execute_join

```rust
// Modifications to crates/executor/src/select/join/mod.rs

pub(super) fn nested_loop_join_with_reordering(
    left: FromResult,
    right: FromResult,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
    optimal_order: Option<Vec<JoinStep>>,  // From Phase 3.2.1 analysis
    mapper: &ExpressionMapper,             // Schema tracking
) -> Result<FromResult, ExecutorError> {
    // If we have an optimal order and it's safe to apply, use it
    if let Some(order) = optimal_order {
        if can_reorder_safely(...) {
            return execute_reordered_joins(left, right, order, mapper, database);
        }
    }

    // Fall back to standard execution
    nested_loop_join(left, right, join_type, condition, database)
}

fn execute_reordered_joins(
    first_result: FromResult,
    remaining_right: FromResult,  // Other tables to join
    optimal_order: Vec<JoinStep>,
    mapper: &ExpressionMapper,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    let mut current = first_result;
    let mut current_mapper = ExpressionMapper::new();
    
    // Initialize mapper with first table
    for (table_name, (_, schema)) in &current.schema.table_schemas {
        current_mapper.add_table(table_name, schema);
    }

    // Execute joins in optimal order
    for step in optimal_order {
        // Get the right table
        let right_table_name = &step.right_table;
        let right_table = extract_table_from_remaining(&remaining_right, right_table_name)?;

        // Join with current result
        current = nested_loop_join(
            current,
            right_table,
            &ast::JoinType::Inner,  // Assumes all are inner joins
            &Some(step.condition.clone()),
            database,
        )?;

        // Update mapper
        for (table_name, (_, schema)) in &current.schema.table_schemas {
            current_mapper.add_table(table_name, schema);
        }
    }

    Ok(current)
}
```

## Example 4: Test Cases

```rust
// File: crates/executor/src/tests/phase3_2_2_reordering.rs

#[cfg(test)]
mod phase_3_2_2_reordering_tests {
    use super::*;

    #[test]
    fn test_three_table_linear_chain_reordering() {
        // CREATE TABLE t1 (id INT, val INT);
        // CREATE TABLE t2 (t1_id INT, val INT);
        // CREATE TABLE t3 (t2_id INT, val INT);
        // 
        // SELECT * FROM t1
        // JOIN t2 ON t1.id = t2.t1_id
        // JOIN t3 ON t2.id = t3.t2_id
        // WHERE t3.val > 100;
        
        // Optimal order would be:
        // 1. t3 scan with WHERE t3.val > 100 (filtered)
        // 2. JOIN with t2 on t2.id = t3.t2_id
        // 3. JOIN with t1 on t1.id = t2.t1_id
        
        // Result should be identical regardless of join order
    }

    #[test]
    fn test_four_table_linear_chain_with_hash_join() {
        // Verify that hash join optimization still applies with reordering
    }

    #[test]
    fn test_expression_mapper_tracks_schema_changes() {
        // Add t1 (2 cols) -> mapper offset [0-1]
        // Add t2 (2 cols) -> mapper offset [2-3]
        // Add t3 (2 cols) -> mapper offset [4-5]
        // Verify all columns resolve correctly
    }

    #[test]
    fn test_fallback_to_standard_when_unsafe() {
        // If join condition references a table not yet joined, 
        // should fall back to original order
    }

    #[test]
    fn test_branching_join_not_reordered() {
        // (t1 JOIN t2) JOIN (t3 JOIN t4) should not be reordered
        // (not a linear chain)
    }

    #[test]
    fn test_reorder_with_null_handling() {
        // LEFT/RIGHT/FULL OUTER joins should not be reordered
        // (changes semantics)
    }
}
```

## Decision Tree

```
Have computed optimal order from Phase 3.2.1?
├─ NO → Skip reordering, use standard order
└─ YES → Is it a linear chain?
    ├─ NO → Skip reordering (too complex)
    └─ YES → Are all join conditions equijoins?
        ├─ NO → Skip reordering (complex predicates)
        └─ YES → Are all columns resolvable in mapper?
            ├─ NO → Skip reordering (safety check fails)
            └─ YES → Apply optimal order ✓
```

## Performance Expectations

For this query:
```sql
SELECT COUNT(*) FROM
  t1 (10,000 rows)
  JOIN t2 ON t1.id = t2.t1_id (100,000 rows)
  JOIN t3 ON t2.id = t3.t2_id (10,000 rows, filtered)
WHERE t3.status = 'active'  (only 100 rows match);
```

**Without reordering** (t1 → t2 → t3):
- t1 × t2: 10,000 × 100,000 = 1B comparisons
- Filter to t3: 1B × 10,000 = 10T ops
- Final filter: 100 rows

**With reordering** (t3 filtered → t2 → t1):
- t3 scan with filter: 10,000 → 100 rows
- 100 × 100,000 = 10M comparisons (join with t2)
- Join with t1: 10M more comparisons
- Total: ~10M ops (1000x better!)

---

**Next Steps**:
1. Implement ExpressionMapper as foundation (no behavior change)
2. Add safety checks for linear chains and resolvability
3. Implement selective reordering execution
4. Add comprehensive tests
5. Benchmark against standard order
