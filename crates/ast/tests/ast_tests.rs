use ast::*;
use types::SqlValue;

// ============================================================================
// Statement Tests - Top-level SQL statements
// ============================================================================

#[test]
fn test_create_select_statement() {
    let stmt = Statement::Select(SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    match stmt {
        Statement::Select(_) => {} // Success
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_create_insert_statement() {
    let stmt = Statement::Insert(InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Varchar(
            "Alice".to_string(),
        ))]]),
    });

    match stmt {
        Statement::Insert(_) => {} // Success
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_create_update_statement() {
    let stmt = Statement::Update(UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Literal(SqlValue::Varchar("Bob".to_string())),
        }],
        where_clause: None,
    });

    match stmt {
        Statement::Update(_) => {} // Success
        _ => panic!("Expected Update statement"),
    }
}

#[test]
fn test_create_delete_statement() {
    let stmt =
        Statement::Delete(DeleteStmt { table_name: "users".to_string(), where_clause: None });

    match stmt {
        Statement::Delete(_) => {} // Success
        _ => panic!("Expected Delete statement"),
    }
}

// ============================================================================
// Expression Tests - SQL expressions
// ============================================================================

#[test]
fn test_literal_integer_expression() {
    let expr = Expression::Literal(SqlValue::Integer(42));
    match expr {
        Expression::Literal(SqlValue::Integer(42)) => {} // Success
        _ => panic!("Expected integer literal"),
    }
}

#[test]
fn test_literal_string_expression() {
    let expr = Expression::Literal(SqlValue::Varchar("hello".to_string()));
    match expr {
        Expression::Literal(SqlValue::Varchar(s)) if s == "hello" => {} // Success
        _ => panic!("Expected string literal"),
    }
}

#[test]
fn test_column_reference_expression() {
    let expr = Expression::ColumnRef { table: None, column: "id".to_string() };

    match expr {
        Expression::ColumnRef { table: None, column } if column == "id" => {} // Success
        _ => panic!("Expected column reference"),
    }
}

#[test]
fn test_qualified_column_reference() {
    let expr = Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };

    match expr {
        Expression::ColumnRef { table: Some(t), column: c } if t == "users" && c == "id" => {} // Success
        _ => panic!("Expected qualified column reference"),
    }
}

#[test]
fn test_binary_operation_addition() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Plus,
        left: Box::new(Expression::Literal(SqlValue::Integer(1))),
        right: Box::new(Expression::Literal(SqlValue::Integer(2))),
    };

    match expr {
        Expression::BinaryOp { op: BinaryOperator::Plus, .. } => {} // Success
        _ => panic!("Expected addition operation"),
    }
}

#[test]
fn test_binary_operation_equality() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    };

    match expr {
        Expression::BinaryOp { op: BinaryOperator::Equal, .. } => {} // Success
        _ => panic!("Expected equality operation"),
    }
}

#[test]
fn test_function_call_count_star() {
    let expr = Expression::Function { name: "COUNT".to_string(), args: vec![Expression::Wildcard] };

    match expr {
        Expression::Function { name, .. } if name == "COUNT" => {} // Success
        _ => panic!("Expected function call"),
    }
}

#[test]
fn test_is_null_predicate() {
    let expr = Expression::IsNull {
        expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
        negated: false,
    };

    match expr {
        Expression::IsNull { negated: false, .. } => {} // Success
        _ => panic!("Expected IS NULL predicate"),
    }
}

#[test]
fn test_is_not_null_predicate() {
    let expr = Expression::IsNull {
        expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
        negated: true,
    };

    match expr {
        Expression::IsNull { negated: true, .. } => {} // Success
        _ => panic!("Expected IS NOT NULL predicate"),
    }
}

// ============================================================================
// SelectStmt Tests - SELECT statement structure
// ============================================================================

#[test]
fn test_select_star() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert_eq!(select.select_list.len(), 1);
    match &select.select_list[0] {
        SelectItem::Wildcard => {} // Success
        _ => panic!("Expected wildcard"),
    }
}

#[test]
fn test_select_with_columns() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert_eq!(select.select_list.len(), 2);
}

#[test]
fn test_select_with_alias() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: Some("user_id".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    match &select.select_list[0] {
        SelectItem::Expression { alias: Some(a), .. } if a == "user_id" => {} // Success
        _ => panic!("Expected aliased expression"),
    }
}

#[test]
fn test_select_from_table() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    match &select.from {
        Some(FromClause::Table { name, .. }) if name == "users" => {} // Success
        _ => panic!("Expected table in FROM clause"),
    }
}

#[test]
fn test_select_with_where() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert!(select.where_clause.is_some());
}

#[test]
fn test_select_with_order_by() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef { table: None, column: "name".to_string() },
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    assert!(select.order_by.is_some());
    assert_eq!(select.order_by.as_ref().unwrap().len(), 1);
}

// ============================================================================
// BinaryOperator Tests - All SQL operators
// ============================================================================

#[test]
fn test_arithmetic_operators() {
    let _plus = BinaryOperator::Plus;
    let _minus = BinaryOperator::Minus;
    let _multiply = BinaryOperator::Multiply;
    let _divide = BinaryOperator::Divide;
    // If these compile, the operators exist
}

#[test]
fn test_comparison_operators() {
    let _eq = BinaryOperator::Equal;
    let _ne = BinaryOperator::NotEqual;
    let _lt = BinaryOperator::LessThan;
    let _le = BinaryOperator::LessThanOrEqual;
    let _gt = BinaryOperator::GreaterThan;
    let _ge = BinaryOperator::GreaterThanOrEqual;
    // If these compile, the operators exist
}

#[test]
fn test_logical_operators() {
    let _and = BinaryOperator::And;
    let _or = BinaryOperator::Or;
    // If these compile, the operators exist
}

#[test]
fn test_modulo_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Modulo,
        left: Box::new(Expression::Literal(SqlValue::Integer(10))),
        right: Box::new(Expression::Literal(SqlValue::Integer(3))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Modulo, .. } => {} // Success
        _ => panic!("Expected modulo operation"),
    }
}

#[test]
fn test_concat_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Concat,
        left: Box::new(Expression::Literal(SqlValue::Varchar("Hello".to_string()))),
        right: Box::new(Expression::Literal(SqlValue::Varchar("World".to_string()))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Concat, .. } => {} // Success
        _ => panic!("Expected concat operation"),
    }
}

#[test]
fn test_not_equal_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::NotEqual,
        left: Box::new(Expression::ColumnRef { table: None, column: "status".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::NotEqual, .. } => {} // Success
        _ => panic!("Expected not equal operation"),
    }
}

#[test]
fn test_less_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(18))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::LessThan, .. } => {} // Success
        _ => panic!("Expected less than operation"),
    }
}

#[test]
fn test_greater_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(50000))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::GreaterThan, .. } => {} // Success
        _ => panic!("Expected greater than operation"),
    }
}

#[test]
fn test_and_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, .. } => {} // Success
        _ => panic!("Expected AND operation"),
    }
}

#[test]
fn test_or_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Or, .. } => {} // Success
        _ => panic!("Expected OR operation"),
    }
}

// ============================================================================
// UnaryOperator Tests
// ============================================================================

#[test]
fn test_unary_not_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expression::Literal(SqlValue::Boolean(true))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Not, .. } => {} // Success
        _ => panic!("Expected NOT operation"),
    }
}

#[test]
fn test_unary_minus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Minus, .. } => {} // Success
        _ => panic!("Expected unary minus operation"),
    }
}

#[test]
fn test_unary_plus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Plus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Plus, .. } => {} // Success
        _ => panic!("Expected unary plus operation"),
    }
}

// ============================================================================
// Complex Expression Tests
// ============================================================================

#[test]
fn test_case_expression_simple() {
    let expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef {
            table: None,
            column: "status".to_string(),
        })),
        when_clauses: vec![
            (
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Varchar("active".to_string())),
            ),
            (
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Varchar("inactive".to_string())),
            ),
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar("unknown".to_string())))),
    };
    match expr {
        Expression::Case { .. } => {} // Success
        _ => panic!("Expected CASE expression"),
    }
}

#[test]
fn test_case_expression_searched() {
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![(
            Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            },
            Expression::Literal(SqlValue::Varchar("adult".to_string())),
        )],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar("minor".to_string())))),
    };
    match expr {
        Expression::Case { operand: None, .. } => {} // Success
        _ => panic!("Expected searched CASE expression"),
    }
}

#[test]
fn test_scalar_subquery() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Function {
                name: "AVG".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "salary".to_string() }],
            },
            alias: None,
        }],
        from: Some(FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::ScalarSubquery(Box::new(subquery));
    match expr {
        Expression::ScalarSubquery(_) => {} // Success
        _ => panic!("Expected scalar subquery"),
    }
}

#[test]
fn test_in_expression() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "department_id".to_string() },
            alias: None,
        }],
        from: Some(FromClause::Table { name: "departments".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::In {
        expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
        subquery: Box::new(subquery),
        negated: false,
    };
    match expr {
        Expression::In { negated: false, .. } => {} // Success
        _ => panic!("Expected IN expression"),
    }
}

#[test]
fn test_not_in_expression() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "excluded".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::In {
        expr: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
        subquery: Box::new(subquery),
        negated: true,
    };
    match expr {
        Expression::In { negated: true, .. } => {} // Success
        _ => panic!("Expected NOT IN expression"),
    }
}

#[test]
fn test_wildcard_expression() {
    let expr = Expression::Wildcard;
    match expr {
        Expression::Wildcard => {} // Success
        _ => panic!("Expected wildcard expression"),
    }
}

// ============================================================================
// DDL Tests - CREATE TABLE
// ============================================================================

#[test]
fn test_create_table_statement() {
    let stmt = Statement::CreateTable(CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                constraints: vec![],
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: types::DataType::Varchar { max_length: Some(255) },
                nullable: true,
                constraints: vec![],
            },
        ],
        table_constraints: vec![],
    });

    match stmt {
        Statement::CreateTable(_) => {} // Success
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_column_def() {
    let col = ColumnDef {
        name: "email".to_string(),
        data_type: types::DataType::Varchar { max_length: Some(100) },
        nullable: false,
        constraints: vec![],
    };
    assert_eq!(col.name, "email");
    assert!(!col.nullable);
}

// ============================================================================
// SELECT Advanced Features
// ============================================================================

#[test]
fn test_select_distinct() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "country".to_string() },
            alias: None,
        }],
        from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.distinct);
}

#[test]
fn test_select_with_group_by() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Function {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(FromClause::Table { name: "orders".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![Expression::ColumnRef {
            table: None,
            column: "customer_id".to_string(),
        }]),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.group_by.is_some());
}

#[test]
fn test_select_with_having() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![Expression::ColumnRef { table: None, column: "region".to_string() }]),
        having: Some(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::Function {
                name: "SUM".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1000))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.having.is_some());
}

#[test]
fn test_select_with_limit() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(10),
        offset: None,
    };
    assert_eq!(select.limit, Some(10));
}

#[test]
fn test_select_with_offset() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(20),
        offset: Some(100),
    };
    assert_eq!(select.offset, Some(100));
}

#[test]
fn test_order_by_desc() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "posts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef { table: None, column: "created_at".to_string() },
            direction: OrderDirection::Desc,
        }]),
        limit: None,
        offset: None,
    };
    let order = select.order_by.as_ref().unwrap();
    assert_eq!(order[0].direction, OrderDirection::Desc);
}

// ============================================================================
// JOIN Tests
// ============================================================================

#[test]
fn test_inner_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table { name: "users".to_string(), alias: None }),
        right: Box::new(FromClause::Table { name: "orders".to_string(), alias: None }),
        join_type: JoinType::Inner,
        condition: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "user_id".to_string(),
            }),
        }),
    };
    match from {
        FromClause::Join { join_type: JoinType::Inner, .. } => {} // Success
        _ => panic!("Expected INNER JOIN"),
    }
}

#[test]
fn test_left_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table { name: "customers".to_string(), alias: None }),
        right: Box::new(FromClause::Table { name: "orders".to_string(), alias: None }),
        join_type: JoinType::LeftOuter,
        condition: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::LeftOuter, .. } => {} // Success
        _ => panic!("Expected LEFT OUTER JOIN"),
    }
}

#[test]
fn test_right_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table { name: "products".to_string(), alias: None }),
        right: Box::new(FromClause::Table { name: "categories".to_string(), alias: None }),
        join_type: JoinType::RightOuter,
        condition: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::RightOuter, .. } => {} // Success
        _ => panic!("Expected RIGHT OUTER JOIN"),
    }
}

#[test]
fn test_full_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table { name: "table1".to_string(), alias: None }),
        right: Box::new(FromClause::Table { name: "table2".to_string(), alias: None }),
        join_type: JoinType::FullOuter,
        condition: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::FullOuter, .. } => {} // Success
        _ => panic!("Expected FULL OUTER JOIN"),
    }
}

#[test]
fn test_cross_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table { name: "colors".to_string(), alias: None }),
        right: Box::new(FromClause::Table { name: "sizes".to_string(), alias: None }),
        join_type: JoinType::Cross,
        condition: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::Cross, .. } => {} // Success
        _ => panic!("Expected CROSS JOIN"),
    }
}

// ============================================================================
// FROM Subquery Tests
// ============================================================================

#[test]
fn test_from_subquery() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let from =
        FromClause::Subquery { query: Box::new(subquery), alias: "active_users".to_string() };
    match from {
        FromClause::Subquery { alias, .. } if alias == "active_users" => {} // Success
        _ => panic!("Expected subquery in FROM clause"),
    }
}

#[test]
fn test_table_with_alias() {
    let from = FromClause::Table { name: "employees".to_string(), alias: Some("e".to_string()) };
    match from {
        FromClause::Table { alias: Some(a), .. } if a == "e" => {} // Success
        _ => panic!("Expected table with alias"),
    }
}
