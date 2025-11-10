//! Logical operator implementations
//!
//! Handles: AND, OR
//! Implements: SQL three-valued logic (TRUE, FALSE, NULL)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

pub(crate) struct LogicalOps;

impl LogicalOps {
    /// AND operator - SQL three-valued logic
    ///
    /// Truth table:
    /// - TRUE AND TRUE = TRUE
    /// - TRUE AND FALSE = FALSE
    /// - TRUE AND NULL = NULL
    /// - FALSE AND anything = FALSE
    /// - NULL AND TRUE = NULL
    /// - NULL AND FALSE = FALSE
    /// - NULL AND NULL = NULL
    #[inline]
    pub fn and(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            (Boolean(a), Boolean(b)) => Ok(Boolean(*a && *b)),
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "AND".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// OR operator - SQL three-valued logic
    ///
    /// Truth table:
    /// - TRUE OR anything = TRUE
    /// - FALSE OR TRUE = TRUE
    /// - FALSE OR FALSE = FALSE
    /// - FALSE OR NULL = NULL
    /// - NULL OR TRUE = TRUE
    /// - NULL OR FALSE = NULL
    /// - NULL OR NULL = NULL
    #[inline]
    pub fn or(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            (Boolean(a), Boolean(b)) => Ok(Boolean(*a || *b)),
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "OR".to_string(),
                right: right.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and_operator() {
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_operator() {
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_type_error() {
        let result = LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Boolean(true));
        assert!(result.is_err());
    }
}
