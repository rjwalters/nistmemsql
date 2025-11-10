//! Common Table Expression (CTE) handling for SELECT queries

use std::collections::HashMap;

use crate::errors::ExecutorError;

/// CTE result: (schema, rows)
pub(super) type CteResult = (catalog::TableSchema, Vec<storage::Row>);

/// Execute all CTEs and return their results
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
pub(super) fn execute_ctes<F>(
    ctes: &[ast::CommonTableExpr],
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<storage::Row>, ExecutorError>,
{
    let mut cte_results = HashMap::new();

    // Execute each CTE in order
    // CTEs can reference previously defined CTEs
    for cte in ctes {
        // Execute the CTE query with accumulated CTE results so far
        // This allows later CTEs to reference earlier ones
        let rows = executor(&cte.query, &cte_results)?;

        //  Determine the schema for this CTE
        let schema = derive_cte_schema(cte, &rows)?;

        // Store the CTE result
        cte_results.insert(cte.name.clone(), (schema, rows));
    }

    Ok(cte_results)
}

/// Derive the schema for a CTE from its query and results
pub(super) fn derive_cte_schema(
    cte: &ast::CommonTableExpr,
    rows: &[storage::Row],
) -> Result<catalog::TableSchema, ExecutorError> {
    // If column names are explicitly specified, use those
    if let Some(column_names) = &cte.columns {
        // Get data types from first row (if available)
        if let Some(first_row) = rows.first() {
            if first_row.values.len() != column_names.len() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CTE column count mismatch: specified {} columns but query returned {}",
                    column_names.len(),
                    first_row.values.len()
                )));
            }

            let columns = column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    let data_type = infer_type_from_value(value);
                    catalog::ColumnSchema::new(name.clone(), data_type, true) // nullable for
                                                                              // simplicity
                })
                .collect();

            Ok(catalog::TableSchema::new(cte.name.clone(), columns))
        } else {
            // Empty result set - create schema with VARCHAR columns
            let columns = column_names
                .iter()
                .map(|name| {
                    catalog::ColumnSchema::new(
                        name.clone(),
                        types::DataType::Varchar { max_length: Some(255) },
                        true,
                    )
                })
                .collect();

            Ok(catalog::TableSchema::new(cte.name.clone(), columns))
        }
    } else {
        // No explicit column names - infer from query SELECT list
        if let Some(first_row) = rows.first() {
            let columns = cte
                .query
                .select_list
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let data_type = infer_type_from_value(&first_row.values[i]);

                    // Extract column name from SELECT item
                    let col_name = match item {
                        ast::SelectItem::Wildcard { .. }
                        | ast::SelectItem::QualifiedWildcard { .. } => format!("col{}", i),
                        ast::SelectItem::Expression { expr, alias } => {
                            if let Some(a) = alias {
                                a.clone()
                            } else {
                                // Try to extract name from expression
                                match expr {
                                    ast::Expression::ColumnRef { table: _, column } => {
                                        column.clone()
                                    }
                                    _ => format!("col{}", i),
                                }
                            }
                        }
                    };

                    catalog::ColumnSchema::new(col_name, data_type, true) // nullable
                })
                .collect();

            Ok(catalog::TableSchema::new(cte.name.clone(), columns))
        } else {
            // Empty CTE with no column specification
            Err(ExecutorError::UnsupportedFeature(
                "Cannot infer schema for empty CTE without column list".to_string(),
            ))
        }
    }
}

/// Infer data type from a SQL value
pub(super) fn infer_type_from_value(value: &types::SqlValue) -> types::DataType {
    match value {
        types::SqlValue::Null => types::DataType::Varchar { max_length: Some(255) }, // default
        types::SqlValue::Integer(_) => types::DataType::Integer,
        types::SqlValue::Varchar(_) => types::DataType::Varchar { max_length: Some(255) },
        types::SqlValue::Character(_) => types::DataType::Character { length: 1 },
        types::SqlValue::Boolean(_) => types::DataType::Boolean,
        types::SqlValue::Float(_) => types::DataType::Float { precision: 53 },
        types::SqlValue::Double(_) => types::DataType::DoublePrecision,
        types::SqlValue::Numeric(_) => types::DataType::Numeric { precision: 10, scale: 2 },
        types::SqlValue::Real(_) => types::DataType::Real,
        types::SqlValue::Smallint(_) => types::DataType::Smallint,
        types::SqlValue::Bigint(_) => types::DataType::Bigint,
        types::SqlValue::Unsigned(_) => types::DataType::Unsigned,
        types::SqlValue::Date(_) => types::DataType::Date,
        types::SqlValue::Time(_) => types::DataType::Time { with_timezone: false },
        types::SqlValue::Timestamp(_) => types::DataType::Timestamp { with_timezone: false },
        types::SqlValue::Interval(_) => {
            // For now, return a simple INTERVAL type (can be enhanced to detect field types)
            types::DataType::Interval {
                start_field: types::IntervalField::Day,
                end_field: None,
            }
        }
    }
}
