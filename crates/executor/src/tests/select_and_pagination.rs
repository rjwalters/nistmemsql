//! SELECT executor and pagination tests
//!
//! These tests will be further split into separate modules in later phases:
//! - Phase 1.3: Move basic SELECT tests
//! - Phase 1.4: Move WHERE clause and aggregate tests

use crate::*;

// ========================================================================
// SELECT Executor Tests
// ========================================================================

    #[test]
    fn test_select_star() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
        assert_eq!(result[1].values[1], types::SqlValue::Varchar("Bob".to_string()));
    }

    #[test]
    fn test_select_with_where() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(17)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(30)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(18))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
    }

    #[test]
    fn test_select_specific_columns() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
                types::SqlValue::Integer(25),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[1], types::SqlValue::Integer(25));
    }

    #[test]
    fn test_order_by_single_column_asc() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(20)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(25)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: ast::OrderDirection::Asc,
            }]),
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[1], types::SqlValue::Integer(20));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(30));
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(35),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(30),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(20),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(4),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(25),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![
                ast::OrderByItem {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    direction: ast::OrderDirection::Asc,
                },
                ast::OrderByItem {
                    expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                    direction: ast::OrderDirection::Desc,
                },
            ]),
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].values[1], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[2], types::SqlValue::Integer(30));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[2], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(2));
        assert_eq!(result[2].values[2], types::SqlValue::Integer(35));
        assert_eq!(result[3].values[1], types::SqlValue::Integer(2));
        assert_eq!(result[3].values[2], types::SqlValue::Integer(20));
    }

    #[test]
    fn test_count_star_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
    }

    #[test]
    fn test_sum_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(200)]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(150)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(450));
    }

    #[test]
    fn test_group_by_with_count() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(150),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "COUNT".to_string(),
                        args: vec![ast::Expression::Wildcard],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        let mut results = result
            .into_iter()
            .map(|row| (row.values[0].clone(), row.values[1].clone()))
            .collect::<Vec<_>>();
        results.sort_by_key(|(dept, _)| dept.clone());
        assert_eq!(results[0], (types::SqlValue::Integer(1), types::SqlValue::Integer(2)));
        assert_eq!(results[1], (types::SqlValue::Integer(2), types::SqlValue::Integer(1)));
    }

    #[test]
    fn test_having_clause() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(50),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "SUM".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(150))),
            }),
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Integer(300));
    }

    #[test]
    fn test_inner_join_two_tables() {
        let mut db = storage::Database::new();

        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(users_schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(50),
            ]),
        )
        .unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(75),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
                right: Box::new(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
                join_type: ast::JoinType::Inner,
                condition: Some(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("users".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("orders".to_string()),
                        column: "user_id".to_string(),
                    }),
                }),
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values.len(), 5); // users (2 cols) + orders (3 cols)
    }

    // ========================================================================
    // LIMIT/OFFSET Tests
    // ========================================================================

    fn make_pagination_stmt(limit: Option<usize>, offset: Option<usize>) -> ast::SelectStmt {
        ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit,
            offset,
        }
    }

    fn make_users_table() -> storage::Database {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Varchar("Carol".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(4),
                types::SqlValue::Varchar("Dave".to_string()),
            ]),
        )
        .unwrap();
        db
    }

    #[test]
    fn test_limit_basic() {
        let db = make_users_table();
        let executor = SelectExecutor::new(&db);
        let stmt = make_pagination_stmt(Some(2), None);

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
    }

    #[test]
    fn test_offset_basic() {
        let db = make_users_table();
        let executor = SelectExecutor::new(&db);
        let stmt = make_pagination_stmt(None, Some(2));

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(4));
    }

    #[test]
    fn test_limit_and_offset() {
        let db = make_users_table();
        let executor = SelectExecutor::new(&db);
        let stmt = make_pagination_stmt(Some(2), Some(1));

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
    }

    #[test]
    fn test_offset_beyond_result_set() {
        let db = make_users_table();
        let executor = SelectExecutor::new(&db);
        let stmt = make_pagination_stmt(None, Some(10));

        let result = executor.execute(&stmt).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_limit_greater_than_result_set() {
        let db = make_users_table();
        let executor = SelectExecutor::new(&db);
        let stmt = make_pagination_stmt(Some(10), None);

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 4);
    }
