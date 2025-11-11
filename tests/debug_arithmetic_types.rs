use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn debug_arithmetic_types() {
    let mut db = Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL);

    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER);
INSERT INTO t1 VALUES(500, 35);
INSERT INTO t1 VALUES(501, 36);
"#;

    for sql in setup_sql.trim().split(';').filter(|s| !s.trim().is_empty()) {
        let stmt = Parser::parse_sql(sql.trim()).unwrap();
        match stmt {
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Unexpected statement"),
        }
    }

    // Test different arithmetic operations
    let test_cases = vec![
        ("SELECT a * 2 FROM t1", "a * 2"),
        ("SELECT b * 10 FROM t1", "b * 10"),
        ("SELECT a + 100 FROM t1", "a + 100"),
    ];

    for (sql, desc) in test_cases {
        let stmt = Parser::parse_sql(sql).unwrap();
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                let rows = executor.execute(&select_stmt).unwrap();
                
                println!("{}:", desc);
                for row in &rows {
                    println!("  {:?}", row.values[0]);
                }
            }
            _ => {}
        }
    }
}
