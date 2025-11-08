//! Column name derivation for SELECT results

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::select::join::FromResult;

impl SelectExecutor<'_> {
    /// Derive column names from SELECT list
    pub(super) fn derive_column_names(
        &self,
        select_list: &[ast::SelectItem],
        from_result: Option<&FromResult>,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut column_names = Vec::new();

        for item in select_list {
            match item {
                ast::SelectItem::Wildcard { alias } => {
                    // SELECT * [AS (col1, col2, ...)] - expand to all column names from schema
                    if let Some(from_res) = from_result {
                        // Get all column names in order from the combined schema
                        let mut table_columns: Vec<(usize, String)> = Vec::new();

                        for (start_index, schema) in from_res.schema.table_schemas.values() {
                            for (col_idx, col_schema) in schema.columns.iter().enumerate() {
                                table_columns
                                    .push((start_index + col_idx, col_schema.name.clone()));
                            }
                        }

                        // Sort by index to maintain column order
                        table_columns.sort_by_key(|(idx, _)| *idx);

                        // Apply derived column list if present
                        if let Some(derived_cols) = alias {
                            if derived_cols.len() != table_columns.len() {
                                return Err(ExecutorError::ColumnCountMismatch {
                                    expected: table_columns.len(),
                                    provided: derived_cols.len(),
                                });
                            }
                            column_names.extend(derived_cols.clone());
                        } else {
                            for (_, name) in table_columns {
                                column_names.push(name);
                            }
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * requires FROM clause".to_string(),
                        ));
                    }
                }
                ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    // SELECT table.* [AS (col1, col2, ...)] or SELECT alias.* [AS (col1, col2, ...)]
                    if let Some(from_res) = from_result {
                        // Find the table/alias in the schema
                        // Try exact match first for performance
                        let result = from_res.schema.table_schemas.get(qualifier).cloned()
                            .or_else(|| {
                                // Fall back to case-insensitive lookup
                                let qualifier_lower = qualifier.to_lowercase();
                                from_res.schema.table_schemas.iter()
                                    .find(|(key, _)| key.to_lowercase() == qualifier_lower)
                                    .map(|(_, value)| value.clone())
                            });
                        
                        if let Some((_start_index, schema)) = result
                        {
                            // Apply derived column list if present
                            if let Some(derived_cols) = alias {
                                if derived_cols.len() != schema.columns.len() {
                                    return Err(ExecutorError::ColumnCountMismatch {
                                        expected: schema.columns.len(),
                                        provided: derived_cols.len(),
                                    });
                                }
                                column_names.extend(derived_cols.clone());
                            } else {
                                // Add all column names from this table in order
                                for col_schema in &schema.columns {
                                    column_names.push(col_schema.name.clone());
                                }
                            }
                        } else {
                            return Err(ExecutorError::TableNotFound(qualifier.clone()));
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT table.* without FROM not supported".to_string(),
                        ));
                    }
                }
                ast::SelectItem::Expression { expr, alias } => {
                    // If there's an alias, use it
                    if let Some(alias_name) = alias {
                        column_names.push(alias_name.clone());
                    } else {
                        // Derive name from the expression
                        column_names.push(self.derive_expression_name(expr));
                    }
                }
            }
        }

        Ok(column_names)
    }

    /// Derive a column name from an expression
    pub(super) fn derive_expression_name(&self, expr: &ast::Expression) -> String {
        derive_expression_name_impl(expr)
    }
}

/// Helper function to derive a column name from an expression
fn derive_expression_name_impl(expr: &ast::Expression) -> String {
    match expr {
        ast::Expression::ColumnRef { table: _, column } => column.clone(),
        ast::Expression::Function { name, args, character_unit: _ } => {
            // For functions, use name(args) format
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter().map(derive_expression_name_impl).collect::<Vec<_>>().join(", ")
            };
            format!("{}({})", name, args_str)
        }
        ast::Expression::AggregateFunction { name, distinct, args } => {
            // For aggregate functions, use name(DISTINCT args) format
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter().map(derive_expression_name_impl).collect::<Vec<_>>().join(", ")
            };
            format!("{}({}{})", name, distinct_str, args_str)
        }
        ast::Expression::BinaryOp { left, op, right } => {
            // For binary operations, create descriptive name
            format!(
                "({} {} {})",
                derive_expression_name_impl(left),
                match op {
                    ast::BinaryOperator::Plus => "+",
                    ast::BinaryOperator::Minus => "-",
                    ast::BinaryOperator::Multiply => "*",
                    ast::BinaryOperator::Divide => "/",
                    ast::BinaryOperator::Equal => "=",
                    ast::BinaryOperator::NotEqual => "!=",
                    ast::BinaryOperator::LessThan => "<",
                    ast::BinaryOperator::LessThanOrEqual => "<=",
                    ast::BinaryOperator::GreaterThan => ">",
                    ast::BinaryOperator::GreaterThanOrEqual => ">=",
                    ast::BinaryOperator::And => "AND",
                    ast::BinaryOperator::Or => "OR",
                    ast::BinaryOperator::Concat => "||",
                    _ => "?",
                },
                derive_expression_name_impl(right)
            )
        }
        ast::Expression::Literal(val) => {
            // For literals, use a clean string representation
            match val {
                types::SqlValue::Integer(n) => n.to_string(),
                types::SqlValue::Smallint(n) => n.to_string(),
                types::SqlValue::Bigint(n) => n.to_string(),
                types::SqlValue::Unsigned(n) => n.to_string(),
                types::SqlValue::Double(f) => f.to_string(),
                types::SqlValue::Float(f) => f.to_string(),
                types::SqlValue::Real(f) => f.to_string(),
                types::SqlValue::Numeric(f) => f.to_string(),
                types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
                    format!("'{}'", s)
                }
                types::SqlValue::Boolean(b) => b.to_string(),
                types::SqlValue::Date(d) => format!("'{}'", d),
                types::SqlValue::Time(t) => format!("'{}'", t),
                types::SqlValue::Timestamp(ts) => format!("'{}'", ts),
                types::SqlValue::Interval(i) => format!("INTERVAL '{}'", i),
                types::SqlValue::Null => "NULL".to_string(),
            }
        }
        _ => "?column?".to_string(), // Default for other expression types
    }
}
