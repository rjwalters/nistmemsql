use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn main() {
    let mut db = Database::new();

    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);
INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113);
INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118);
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

            println!("Query: {}", query_sql);
            println!("Result count: {}", rows.len());

            let mut all_values = Vec::new();
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?} | {:?}", i, row.values[0], row.values[1]);
                all_values.push(format!("{}", row.values[0]));
                all_values.push(format!("{}", row.values[1]));
            }

            // Hash the values
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let joined = all_values.join("\n");
            let mut hasher = DefaultHasher::new();
            joined.hash(&mut hasher);
            let hash = format!("{:x}", hasher.finish());
            println!("\nFormatted string (first 500 chars):\n{}", joined.lines().take(5).map(|l| format!("{}\n", l)).collect::<String>());
            println!("\nHash: {}", hash);
            println!("Expected hash: 010239dc0b8e04fa7aa927551a6a231f");
        }
        _ => {}
    }
}
