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

    #[test]
    fn test_improved_error_message_includes_searched_tables() {
        // Test that error message includes searched tables
        let inner_schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar, false),
            ],
        );

        let outer_schema = TableSchema::new(
            "orders".to_string(),
            vec![
                ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("amount".to_string(), DataType::Integer, false),
            ],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42), SqlValue::Varchar("Alice".to_string())]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr = ast::Expression::ColumnRef { table: None, column: "email".to_string() };

        let result = evaluator.eval(&expr, &inner_row);

        match result {
            Err(ExecutorError::ColumnNotFound { column_name, searched_tables, available_columns, .. }) => {
                assert_eq!(column_name, "email");
                assert!(searched_tables.contains(&"users".to_string()), "Should have searched 'users' table");
                assert!(searched_tables.contains(&"orders".to_string()), "Should have searched 'orders' table");
                assert!(available_columns.contains(&"id".to_string()), "Should list 'id' as available");
                assert!(available_columns.contains(&"name".to_string()), "Should list 'name' as available");
                assert!(available_columns.contains(&"order_id".to_string()), "Should list 'order_id' as available");
                assert!(available_columns.contains(&"amount".to_string()), "Should list 'amount' as available");
            }
            other => panic!("Expected ColumnNotFound with diagnostic info, got: {:?}", other),
        }
    }

    #[test]
    fn test_improved_error_message_with_table_qualifier() {
        // Test that table qualifier is captured in error message
        let schema = TableSchema::new(
            "products".to_string(),
            vec![
                ColumnSchema::new("product_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(99)]);

        // Try to resolve with table qualifier
        let expr = ast::Expression::ColumnRef {
            table: Some("products".to_string()),
            column: "description".to_string()
        };

        let result = evaluator.eval(&expr, &row);

        match result {
            Err(ExecutorError::ColumnNotFound { column_name, table_name, searched_tables, available_columns }) => {
                assert_eq!(column_name, "description");
                assert_eq!(table_name, "products", "Should capture table qualifier");
                assert!(searched_tables.contains(&"products".to_string()));
                assert!(available_columns.contains(&"product_id".to_string()));
                assert!(available_columns.contains(&"price".to_string()));
            }
            other => panic!("Expected ColumnNotFound with table qualifier, got: {:?}", other),
        }
    }
}
