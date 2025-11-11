use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn debug_case_order() {
    let mut db = Database::new();

    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER);
INSERT INTO t1 VALUES(500, 35, 100);
INSERT INTO t1 VALUES(525, 36, 100);
INSERT INTO t1 VALUES(501, 35, 200);
INSERT INTO t1 VALUES(510, 40, 200);
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

    let query_sql = "SELECT CASE WHEN c > 150 THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
    let stmt = Parser::parse_sql(query_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            println!("Results in order (expect: 350, 360, 1002, 1050):");
            for row in &rows {
                println!("  {}", match &row.values[0] {
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    _ => format!("Other: {:?}", row.values[0]),
                });
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
