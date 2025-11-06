use parser::Parser;
use executor::SelectExecutor;
use storage::Database;

#[test]
fn debug_case_subquery() {
    let mut db = Database::new();

    // Create table t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)
    let create_sql = "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();
    match create_stmt {
        ast::Statement::CreateTable(create_stmt) => {
            executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE"),
    }

    // Insert some test data
    let insert_sql = "INSERT INTO t1 VALUES(100, 200, 300, 400, 500), (150, 250, 350, 450, 550)";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();
    match insert_stmt {
        ast::Statement::Insert(insert_stmt) => {
            executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT"),
    }

    // Execute the problematic query
    let query_sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1";
    let query_stmt = Parser::parse_sql(query_sql).unwrap();
    match query_stmt {
        ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            println!("Query results:");
            for row in &rows {
                println!("{:?}", row.values);
            }
        }
        _ => panic!("Expected SELECT"),
    }

    // Also test the subquery alone
    let subquery_sql = "SELECT avg(c) FROM t1";
    let subquery_stmt = Parser::parse_sql(subquery_sql).unwrap();
    match subquery_stmt {
        ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            println!("Subquery avg(c) results:");
            for row in &rows {
                println!("{:?}", row.values);
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
