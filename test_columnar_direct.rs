// Direct test of columnar COUNT(*) with WHERE clause
use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    match stmt {
        Statement::Select(select_stmt) => {
            let select_executor = SelectExecutor::new(db);
            select_executor.execute(&select_stmt).map_err(|e| format!("Select error: {:?}", e))
        }
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute(&create, db).map_err(|e| format!("Create error: {:?}", e))?;
            Ok(vec![])
        }
        Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert).map_err(|e| format!("Insert error: {:?}", e))?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

fn main() {
    let mut db = Database::new();

    // Create table
    execute_sql(&mut db, "CREATE TABLE lineitem (
        l_orderkey INTEGER,
        l_quantity FLOAT,
        l_extendedprice FLOAT,
        l_discount FLOAT,
        l_shipdate DATE
    )").unwrap();

    // Insert 3 matching rows
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (1, 10.0, 1000.0, 0.06, '1994-06-15')").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (2, 15.0, 2000.0, 0.05, '1994-12-31')").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (3, 20.0, 1500.0, 0.07, '1994-01-01')").unwrap();

    // Insert 2 non-matching rows
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (4, 30.0, 3000.0, 0.01, '1994-06-15')").unwrap(); // quantity too high
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (5, 10.0, 3000.0, 0.04, '1994-06-15')").unwrap(); // discount too low

    println!("\nTest 1: Total rows");
    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem").unwrap();
    println!("Result: {:?}", result[0].get(0));
    println!("Expected: Integer(5)");

    println!("\nTest 2: COUNT(*) with WHERE clause");
    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24.0").unwrap();
    println!("Result: {:?}", result[0].get(0));
    println!("Expected: Integer(3)");

    println!("\nTest 3: Individual predicates");
    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem WHERE l_quantity < 24.0").unwrap();
    println!("l_quantity < 24.0: {:?}", result[0].get(0));

    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07").unwrap();
    println!("l_discount BETWEEN 0.05 AND 0.07: {:?}", result[0].get(0));
}
