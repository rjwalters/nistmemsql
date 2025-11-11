use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn debug_order_by_int() {
    let mut db = Database::new();

    let setup_sql = r#"
CREATE TABLE t1(x INTEGER);
INSERT INTO t1 VALUES(1000);
INSERT INTO t1 VALUES(358);
INSERT INTO t1 VALUES(1050);
INSERT INTO t1 VALUES(364);
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

    let query_sql = "SELECT x FROM t1 ORDER BY x";
    let stmt = Parser::parse_sql(query_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            println!("Results in order:");
            for row in &rows {
                println!("  {}", match &row.values[0] {
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    _ => "???".to_string(),
                });
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
