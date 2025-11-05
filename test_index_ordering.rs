use ast::{CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection, SelectStmt};
use executor::SelectExecutor;
use storage::Database;
use types::DataType;

#[test]
fn test_index_ordering() {
    let mut db = Database::new();
    
    // Create table
    let create_table_stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ast::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ast::ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
    };
    
    executor::CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();
    
    // Insert data
    let insert_stmt = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: None,
        values: vec![
            vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Charlie".to_string())),
            ],
            vec![
                ast::Expression::Literal(types::SqlValue::Integer(2)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ],
            vec![
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
            ],
        ],
    };
    
    executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    
    // Create index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_users_name".to_string(),
        table_name: "users".to_string(),
        unique: false,
        columns: vec![IndexColumn {
            column_name: "name".to_string(),
            direction: OrderDirection::Asc,
        }],
    };
    
    executor::IndexExecutor::execute(&create_index_stmt, &mut db).unwrap();
    
    // Query with ORDER BY
    let select_stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![ast::OrderByItem {
            expr: ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            },
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
        set_operation: None,
    };
    
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();
    
    // Check that results are ordered
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
    assert_eq!(result[1].values[0], types::SqlValue::Varchar("Bob".to_string()));
    assert_eq!(result[2].values[0], types::SqlValue::Varchar("Charlie".to_string()));
}
