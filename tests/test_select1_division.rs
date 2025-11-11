use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_select1_second_query_values() {
    let mut db = Database::new();

    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);
INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113);
INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118);
INSERT INTO t1(c,d,b,e,a) VALUES(123,122,124,120,121);
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

    let query_sql = "SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1 ORDER BY 1,2";
    let stmt = Parser::parse_sql(query_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            println!("\n=== SELECT1 Second Query ===");
            println!("Query: {}", query_sql);
            println!("Result count: {}", rows.len());

            let mut all_values = Vec::new();
            for (i, row) in rows.iter().enumerate() {
                let col1 = match &row.values[0] {
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    v => format!("{:?}", v),
                };
                let col2 = match &row.values[1] {
                    vibesql_types::SqlValue::Float(f) => {
                        if f.fract() == 0.0 {
                            format!("{:.1}", f)
                        } else {
                            f.to_string()
                        }
                    }
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    v => format!("{:?}", v),
                };
                println!("Row {}: {} | {}", i, col1, col2);
                all_values.push(col1);
                all_values.push(col2);
            }

            // Hash the values
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let joined = all_values.join("\n");
            let mut hasher = DefaultHasher::new();
            joined.hash(&mut hasher);
            let hash = format!("{:x}", hasher.finish());
            
            println!("\nAll values (first 500 chars):\n{}", joined.lines().take(10).map(|l| format!("{}\n", l)).collect::<String>());
            println!("Hash: {}", hash);
            println!("Expected hash: 010239dc0b8e04fa7aa927551a6a231f");
        }
        _ => panic!("Expected SELECT"),
    }
}
