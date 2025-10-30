use crate::errors::ExecutorError;

// ========================================================================
// Type Coercion Helper Functions
// ========================================================================

/// Check if a value is an exact numeric type (SMALLINT, INTEGER, BIGINT)
pub(crate) fn is_exact_numeric(value: &types::SqlValue) -> bool {
    matches!(
        value,
        types::SqlValue::Smallint(_) | types::SqlValue::Integer(_) | types::SqlValue::Bigint(_)
    )
}

/// Check if a value is an approximate numeric type (FLOAT, REAL, DOUBLE)
pub(crate) fn is_approximate_numeric(value: &types::SqlValue) -> bool {
    matches!(
        value,
        types::SqlValue::Float(_) | types::SqlValue::Real(_) | types::SqlValue::Double(_)
    )
}

/// Convert exact numeric types to i64 for comparison
pub(crate) fn to_i64(value: &types::SqlValue) -> Result<i64, ExecutorError> {
    match value {
        types::SqlValue::Smallint(n) => Ok(*n as i64),
        types::SqlValue::Integer(n) => Ok(*n),
        types::SqlValue::Bigint(n) => Ok(*n),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: types::SqlValue::Null,
        }),
    }
}

/// Convert approximate numeric types to f64 for comparison
pub(crate) fn to_f64(value: &types::SqlValue) -> Result<f64, ExecutorError> {
    match value {
        types::SqlValue::Float(n) => Ok(*n as f64),
        types::SqlValue::Real(n) => Ok(*n as f64),
        types::SqlValue::Double(n) => Ok(*n),
        types::SqlValue::Integer(n) => Ok(*n as f64),
        types::SqlValue::Smallint(n) => Ok(*n as f64),
        types::SqlValue::Bigint(n) => Ok(*n as f64),
        types::SqlValue::Numeric(s) => s.parse::<f64>().map_err(|_| ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: types::SqlValue::Null,
        }),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: types::SqlValue::Null,
        }),
    }
}

/// Cast a value to the target data type
/// Implements SQL:1999 CAST semantics for explicit type conversion
pub(crate) fn cast_value(
    value: &types::SqlValue,
    target_type: &types::DataType,
) -> Result<types::SqlValue, ExecutorError> {
    use types::DataType::*;
    use types::SqlValue;

    // NULL can be cast to any type and remains NULL
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        // Cast to INTEGER
        Integer => match value {
            SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Integer(*n as i64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Varchar(s) => {
                s.parse::<i64>().map(SqlValue::Integer).map_err(|_| ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "INTEGER".to_string(),
                })
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "INTEGER".to_string(),
            }),
        },

        // Cast to SMALLINT
        Smallint => match value {
            SqlValue::Smallint(n) => Ok(SqlValue::Smallint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Bigint(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Varchar(s) => {
                s.parse::<i16>().map(SqlValue::Smallint).map_err(|_| ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "SMALLINT".to_string(),
                })
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "SMALLINT".to_string(),
            }),
        },

        // Cast to BIGINT
        Bigint => match value {
            SqlValue::Bigint(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Bigint(*n as i64)),
            SqlValue::Varchar(s) => {
                s.parse::<i64>().map(SqlValue::Bigint).map_err(|_| ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "BIGINT".to_string(),
                })
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "BIGINT".to_string(),
            }),
        },

        // Cast to FLOAT
        Float { .. } => match value {
            SqlValue::Float(n) => Ok(SqlValue::Float(*n)),
            SqlValue::Real(n) => Ok(SqlValue::Float(*n)),
            SqlValue::Double(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Integer(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Smallint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Bigint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Varchar(s) => {
                s.parse::<f32>().map(SqlValue::Float).map_err(|_| ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "FLOAT".to_string(),
                })
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "FLOAT".to_string(),
            }),
        },

        // Cast to DOUBLE PRECISION
        DoublePrecision => match value {
            SqlValue::Double(n) => Ok(SqlValue::Double(*n)),
            SqlValue::Float(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Real(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Integer(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Smallint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Varchar(s) => {
                s.parse::<f64>().map(SqlValue::Double).map_err(|_| ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "DOUBLE PRECISION".to_string(),
                })
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "DOUBLE PRECISION".to_string(),
            }),
        },

        // Cast to VARCHAR
        Varchar { max_length } => {
            let string_val = match value {
                SqlValue::Varchar(s) => s.clone(),
                SqlValue::Integer(n) => n.to_string(),
                SqlValue::Smallint(n) => n.to_string(),
                SqlValue::Bigint(n) => n.to_string(),
                SqlValue::Float(n) => n.to_string(),
                SqlValue::Real(n) => n.to_string(),
                SqlValue::Double(n) => n.to_string(),
                SqlValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                SqlValue::Date(s) => s.clone(),
                SqlValue::Time(s) => s.clone(),
                SqlValue::Timestamp(s) => s.clone(),
                _ => {
                    return Err(ExecutorError::CastError {
                        from_type: format!("{:?}", value),
                        to_type: match max_length {
                            Some(len) => format!("VARCHAR({})", len),
                            None => "VARCHAR".to_string(),
                        },
                    })
                }
            };

            // Truncate if exceeds max_length (when specified)
            match max_length {
                Some(len) if string_val.len() > *len => {
                    Ok(SqlValue::Varchar(string_val[..*len].to_string()))
                }
                _ => Ok(SqlValue::Varchar(string_val)),
            }
        }

        // Cast to DATE
        Date => match value {
            SqlValue::Date(s) => Ok(SqlValue::Date(s.clone())),
            SqlValue::Timestamp(s) => {
                // Extract date part from timestamp (YYYY-MM-DD)
                if let Some(date_part) = s.split_whitespace().next() {
                    Ok(SqlValue::Date(date_part.to_string()))
                } else {
                    Ok(SqlValue::Date(s.clone()))
                }
            }
            SqlValue::Varchar(s) => Ok(SqlValue::Date(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "DATE".to_string(),
            }),
        },

        // Cast to TIME
        Time { .. } => match value {
            SqlValue::Time(s) => Ok(SqlValue::Time(s.clone())),
            SqlValue::Timestamp(s) => {
                // Extract time part from timestamp (HH:MM:SS)
                if let Some(time_part) = s.split_whitespace().nth(1) {
                    Ok(SqlValue::Time(time_part.to_string()))
                } else {
                    Ok(SqlValue::Time(s.clone()))
                }
            }
            SqlValue::Varchar(s) => Ok(SqlValue::Time(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIME".to_string(),
            }),
        },

        // Cast to TIMESTAMP
        Timestamp { .. } => match value {
            SqlValue::Timestamp(s) => Ok(SqlValue::Timestamp(s.clone())),
            SqlValue::Date(s) => Ok(SqlValue::Timestamp(format!("{} 00:00:00", s))),
            SqlValue::Time(time_str) => {
                // SQL:1999: CAST(TIME AS TIMESTAMP) uses current date + time value
                use chrono::Local;
                let now = Local::now();
                let date_str = now.format("%Y-%m-%d").to_string();
                Ok(SqlValue::Timestamp(format!("{} {}", date_str, time_str)))
            }
            SqlValue::Varchar(s) => Ok(SqlValue::Timestamp(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIMESTAMP".to_string(),
            }),
        },

        // Unsupported target types
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "CAST to {:?} not yet implemented",
            target_type
        ))),
    }
}
