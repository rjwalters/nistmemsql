use vibesql::*;

fn main() {
    let db = storage::Database::new();
    let executor = executor::select::executor::SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                    op: ast::BinaryOperator::Plus,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                },
                alias: Some("sum".to_string()),
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

    let result = executor.execute(&stmt).unwrap();
    println!("Result: {:?}", result[0].values[0]);
    println!("Type: {}", result[0].values[0]);
}
