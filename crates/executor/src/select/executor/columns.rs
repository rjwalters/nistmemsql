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
                ast::SelectItem::Wildcard => {
                    // SELECT * - expand to all column names from schema
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

                        for (_, name) in table_columns {
                            column_names.push(name);
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * without FROM not supported".to_string(),
                        ));
                    }
                }
                ast::SelectItem::QualifiedWildcard { qualifier } => {
                    // SELECT table.* or SELECT alias.* - expand to columns from specific table/alias
                    if let Some(from_res) = from_result {
                        // Find the table/alias in the schema
                        if let Some((_start_index, schema)) = from_res.schema.table_schemas.get(qualifier) {
                            // Add all column names from this table in order
                            for col_schema in &schema.columns {
                                column_names.push(col_schema.name.clone());
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
            // For literals, use the value representation
            format!("{:?}", val)
        }
        _ => "?column?".to_string(), // Default for other expression types
    }
}
