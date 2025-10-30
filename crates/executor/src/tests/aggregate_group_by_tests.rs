//! GROUP BY clause tests with aggregates
//!
//! Tests for aggregate functions combined with GROUP BY.

use super::super::*;

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
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                    character_unit: None,
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
    results.sort_by(|(dept_a, _), (dept_b, _)| match (dept_a, dept_b) {
        (types::SqlValue::Integer(a), types::SqlValue::Integer(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(results[0], (types::SqlValue::Integer(1), types::SqlValue::Integer(2)));
    assert_eq!(results[1], (types::SqlValue::Integer(2), types::SqlValue::Integer(1)));
}
