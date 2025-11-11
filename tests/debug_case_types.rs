use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn debug_case_types() {
    let mut db = Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL);

    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER);
INSERT INTO t1 VALUES(500, 35, 100);
INSERT INTO t1 VALUES(501, 36, 200);
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
            _ => {}
        }
    }

    let sql = "SELECT CASE WHEN c > 150 THEN a*2 ELSE b*10 END FROM t1";
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            println!("Results from CASE WHEN:");
            for row in &rows {
                println!("  {:?}", row.values[0]);
            }
        }
        _ => {}
    }
}
