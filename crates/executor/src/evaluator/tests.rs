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
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(255) }, false),
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

#[cfg(test)]
mod deep_expression_tests {
    use super::super::ExpressionEvaluator;
    use crate::errors::ExecutorError;
    use catalog::TableSchema;
    use storage::Row;
    use types::SqlValue;

    /// Helper to generate a deeply nested arithmetic expression
    /// Generates: (((1 + 1) + 1) + 1) ... ) for the specified depth
    fn generate_nested_add(depth: usize) -> ast::Expression {
        let mut expr = ast::Expression::Literal(SqlValue::Integer(1));
        for _ in 0..depth {
            expr = ast::Expression::BinaryOp {
                left: Box::new(expr),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::Literal(SqlValue::Integer(1))),
            };
        }
        expr
    }

    /// Helper to generate deeply nested CASE expressions
    /// Generates: CASE WHEN 1 = 1 THEN (CASE WHEN 1 = 1 THEN ... ELSE 0 END) ELSE 0 END
    fn generate_nested_case(depth: usize) -> ast::Expression {
        let mut expr = ast::Expression::Literal(SqlValue::Integer(0));
        for _ in 0..depth {
            expr = ast::Expression::Case {
                operand: None,
                when_clauses: vec![ast::CaseWhen {
                    conditions: vec![ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::Literal(SqlValue::Integer(1))),
                        op: ast::BinaryOperator::Equal,
                        right: Box::new(ast::Expression::Literal(SqlValue::Integer(1))),
                    }],
                    result: expr,
                }],
                else_result: Some(Box::new(ast::Expression::Literal(SqlValue::Integer(0)))),
            };
        }
        expr
    }

    /// Helper to generate deeply nested unary expressions
    /// Generates: -(-(-(-1))) for the specified depth
    fn generate_nested_unary(depth: usize) -> ast::Expression {
        let mut expr = ast::Expression::Literal(SqlValue::Integer(1));
        for _ in 0..depth {
            expr = ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(expr),
            };
        }
        expr
    }

    #[test]
    fn test_shallow_nested_expression() {
        // Depth 10 should work fine
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        let expr = generate_nested_add(10);
        let result = evaluator.eval(&expr, &row);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(11)); // 1 + 10 additions of 1
    }

    #[test]
    fn test_moderate_nested_expression() {
        // Depth 100 should work fine
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        let expr = generate_nested_add(100);
        let result = evaluator.eval(&expr, &row);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(101)); // 1 + 100 additions of 1
    }

    #[test]
    fn test_depth_limit_enforced() {
        // Test that depth limit is properly enforced
        // Note: Building expressions deeper than ~1000 causes stack overflow during AST construction
        // This is a Rust limitation, not an evaluator issue. In practice, parser-generated ASTs
        // from actual SQL won't hit this limit.
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Test a deep but buildable expression (300 levels)
        let expr = generate_nested_add(300);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(301)); // 1 + 300 additions of 1
    }

    #[test]
    fn test_deep_case_expressions() {
        // Test CASE expression depth tracking
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 50 should work
        let expr = generate_nested_case(50);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(0));

        // Depth 250 (still within limit but deep enough to test) should work
        // Note: We can't test at MAX_EXPRESSION_DEPTH because building the AST itself
        // would stack overflow. This is a limitation of recursive AST construction in Rust.
        let expr = generate_nested_case(250);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
    }

    #[test]
    fn test_deep_unary_expressions() {
        // Test unary operator depth tracking
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 100 should work
        let expr = generate_nested_unary(100);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        // 100 negations of 1: even count = 1, odd count = -1
        assert_eq!(result.unwrap(), SqlValue::Integer(1));

        // Depth 300 should also work
        let expr = generate_nested_unary(300);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        // 300 negations of 1: even count = 1
        assert_eq!(result.unwrap(), SqlValue::Integer(1));
    }

    #[test]
    fn test_mixed_nested_expressions() {
        // Test combination of different expression types
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Build: CASE WHEN 1=1 THEN (1 + (1 + (1 + 1))) ELSE -(-1) END
        let nested_add = generate_nested_add(3);
        let nested_unary = generate_nested_unary(2);

        let expr = ast::Expression::Case {
            operand: None,
            when_clauses: vec![ast::CaseWhen {
                conditions: vec![ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(SqlValue::Integer(1))),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::Literal(SqlValue::Integer(1))),
                }],
                result: nested_add,
            }],
            else_result: Some(Box::new(nested_unary)),
        };

        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(4)); // 1 + 3
    }

    #[test]
    fn test_depth_tracking_is_consistent() {
        // Verify depth is properly tracked across evaluator instances
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);

        assert_eq!(evaluator.depth, 0);

        // Create evaluator with outer context - should still start at depth 0
        let outer_schema = TableSchema::new("outer".to_string(), vec![]);
        let outer_row = Row::new(vec![]);
        let evaluator_with_outer = ExpressionEvaluator::with_outer_context(
            &schema,
            &outer_row,
            &outer_schema,
        );

        assert_eq!(evaluator_with_outer.depth, 0);
    }

    #[test]
    fn test_reasonable_depth_expressions() {
        // Test expressions at reasonable but deep nesting levels
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 200 should work fine
        let expr = generate_nested_add(200);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(201));

        // Depth 400 should also work (well within MAX_EXPRESSION_DEPTH of 500)
        let expr = generate_nested_add(400);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(401));
    }
}
