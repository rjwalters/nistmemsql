//! Apache Arrow RecordBatch adapter for SIMD vectorized execution
//!
//! This module provides conversion between vibesql's row-based format and Arrow's
//! columnar RecordBatch format, enabling SIMD acceleration through Arrow compute kernels.

use crate::errors::ExecutorError;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    PrimitiveBuilder, StringBuilder, BooleanBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Default batch size for vectorized operations
/// Tuned for balance between memory usage and SIMD efficiency
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// Convert a batch of rows to an Arrow RecordBatch
///
/// This performs the key transformation from row-oriented to column-oriented layout.
pub fn rows_to_record_batch(
    rows: &[Row],
    column_names: &[String],
) -> Result<RecordBatch, ExecutorError> {
    if rows.is_empty() {
        return Err(ExecutorError::Other(
            "Cannot create RecordBatch from empty rows".to_string(),
        ));
    }

    // Infer schema from first row
    let schema = infer_schema_from_row(&rows[0], column_names)?;
    let num_columns = rows[0].values.len();

    // Build columns
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_columns);

    for col_idx in 0..num_columns {
        let field = schema.field(col_idx);
        let array = build_column_array(rows, col_idx, field.data_type())?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| ExecutorError::Other(format!("Failed to create RecordBatch: {}", e)))
}

/// Convert RecordBatch back to row format
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<Row>, ExecutorError> {
    let num_rows = batch.num_rows();
    let num_columns = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let array = batch.column(col_idx);
            let value = arrow_value_to_sql(array, row_idx)?;
            values.push(value);
        }

        rows.push(Row { values });
    }

    Ok(rows)
}

/// Infer Arrow schema from vibesql Row
fn infer_schema_from_row(row: &Row, column_names: &[String]) -> Result<Schema, ExecutorError> {
    let fields: Result<Vec<_>, _> = row.values.iter().enumerate().map(|(idx, value)| {
        let name = column_names.get(idx)
            .cloned()
            .unwrap_or_else(|| format!("col_{}", idx));

        let data_type = match value {
            SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => DataType::Int64,
            SqlValue::Float(_) | SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) => DataType::Float64,
            SqlValue::Character(_) | SqlValue::Varchar(_) => DataType::Utf8,
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Null => DataType::Int64, // Default to Int64 for nulls
            _ => return Err(ExecutorError::Other(format!(
                "Unsupported type for SIMD: {:?}", value
            ))),
        };

        Ok(Field::new(name, data_type, true))
    }).collect();

    Ok(Schema::new(fields?))
}

/// Build a columnar array from row data
fn build_column_array(
    rows: &[Row],
    col_idx: usize,
    data_type: &DataType,
) -> Result<ArrayRef, ExecutorError> {
    match data_type {
        DataType::Int64 => {
            let mut builder = PrimitiveBuilder::<Int64Type>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Integer(v) => builder.append_value(*v),
                    SqlValue::Bigint(v) => builder.append_value(*v),
                    SqlValue::Smallint(v) => builder.append_value(*v as i64),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = PrimitiveBuilder::<Float64Type>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Float(v) => builder.append_value(*v as f64),
                    SqlValue::Real(v) => builder.append_value(*v as f64),
                    SqlValue::Double(v) => builder.append_value(*v),
                    SqlValue::Numeric(v) => builder.append_value(*v),
                    SqlValue::Integer(v) => builder.append_value(*v as f64),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Character(s) | SqlValue::Varchar(s) => builder.append_value(s),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Boolean(b) => builder.append_value(*b),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ExecutorError::Other(format!(
            "Unsupported Arrow data type: {:?}",
            data_type
        ))),
    }
}

/// Convert Arrow array value to SqlValue
fn arrow_value_to_sql(array: &dyn Array, idx: usize) -> Result<SqlValue, ExecutorError> {
    if array.is_null(idx) {
        return Ok(SqlValue::Null);
    }

    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| ExecutorError::Other("Failed to downcast Int64Array".to_string()))?;
            Ok(SqlValue::Integer(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| ExecutorError::Other("Failed to downcast Float64Array".to_string()))?;
            Ok(SqlValue::Double(arr.value(idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| ExecutorError::Other("Failed to downcast StringArray".to_string()))?;
            Ok(SqlValue::Varchar(arr.value(idx).to_string()))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| ExecutorError::Other("Failed to downcast BooleanArray".to_string()))?;
            Ok(SqlValue::Boolean(arr.value(idx)))
        }
        _ => Err(ExecutorError::Other(format!(
            "Unsupported Arrow data type for conversion: {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_record_batch_basic() {
        let rows = vec![
            Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("hello".to_string())],
            },
            Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("world".to_string())],
            },
        ];

        let column_names = vec!["id".to_string(), "name".to_string()];
        let batch = rows_to_record_batch(&rows, &column_names).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_round_trip_conversion() {
        let original_rows = vec![
            Row {
                values: vec![
                    SqlValue::Integer(1),
                    SqlValue::Double(3.14),
                    SqlValue::Varchar("test".to_string()),
                    SqlValue::Boolean(true),
                ],
            },
            Row {
                values: vec![
                    SqlValue::Integer(2),
                    SqlValue::Double(2.718),
                    SqlValue::Varchar("data".to_string()),
                    SqlValue::Boolean(false),
                ],
            },
        ];

        let column_names = vec![
            "id".to_string(),
            "value".to_string(),
            "name".to_string(),
            "flag".to_string(),
        ];

        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();
        let converted_rows = record_batch_to_rows(&batch).unwrap();

        assert_eq!(original_rows.len(), converted_rows.len());
        assert_eq!(original_rows[0].values.len(), converted_rows[0].values.len());
    }
}
