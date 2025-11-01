#[cfg(test)]
mod evaluator_tests {
    use super::super::ExpressionEvaluator;
    use crate::errors::ExecutorError;
    use catalog::{ColumnSchema, TableSchema};
    use types::{DataType, SqlValue};

    #[test]
    fn test_evaluator_with_outer_context_resolves_inner_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve inner_col from inner row
        let expr = ast::Expression::ColumnRef { table: None, column: "inner_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_resolves_outer_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve outer_col from outer row (not in inner schema)
        let expr = ast::Expression::ColumnRef { table: None, column: "outer_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));
    }

    #[test]
    fn test_evaluator_with_outer_context_inner_shadows_outer() {
        // Both schemas have "col" - inner should win
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        let expr = ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        // Should get inner value (42), not outer (999)
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_column_not_found() {
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr = ast::Expression::ColumnRef { table: None, column: "nonexistent".to_string() };

        let result = evaluator.eval(&expr, &inner_row);
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_evaluator_without_outer_context() {
        // Normal evaluator without outer context
        let schema = TableSchema::new(
            "table".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let expr = ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }
}
