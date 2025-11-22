use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::Select(select_stmt) => {
            let select_executor = SelectExecutor::new(db);
            select_executor
                .execute(&select_stmt)
                .map_err(|e| format!("Select error: {:?}", e))
        }
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute(&create, db)
                .map_err(|e| format!("Create error: {:?}", e))?;
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
        l_quantity FLOAT,
        l_discount FLOAT,
        l_shipdate DATE
    )").unwrap();

    // Insert 3 rows
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (10.0, 0.06, '1994-06-15')").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (15.0, 0.05, '1994-12-31')").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (20.0, 0.07, '1994-01-01')").unwrap();

    // Test 1: SELECT * (should return 3 rows)
    let result = execute_sql(&mut db, "SELECT * FROM lineitem").unwrap();
    println!("Test 1 - SELECT *: {} rows", result.len());
    for (i, row) in result.iter().enumerate() {
        println!("  Row {}: {:?}", i, row);
    }

    // Test 2: COUNT(*) with no WHERE (should return 3)
    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem").unwrap();
    println!("\nTest 2 - COUNT(*): {:?}", result[0].get(0));

    // Test 3: SELECT with simple WHERE
    let result = execute_sql(&mut db, "SELECT * FROM lineitem WHERE l_quantity < 24").unwrap();
    println!("\nTest 3 - WHERE l_quantity < 24: {} rows", result.len());

    // Test 4: SELECT with DATE WHERE
    let result = execute_sql(&mut db, "SELECT * FROM lineitem WHERE l_shipdate >= '1994-01-01'").unwrap();
    println!("\nTest 4 - WHERE l_shipdate >= '1994-01-01': {} rows", result.len());

    // Test 5: SELECT with BETWEEN
    let result = execute_sql(&mut db, "SELECT * FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07").unwrap();
    println!("\nTest 5 - WHERE l_discount BETWEEN 0.05 AND 0.07: {} rows", result.len());

    // Test 6: Full WHERE clause
    let result = execute_sql(&mut db, "SELECT * FROM lineitem
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24").unwrap();
    println!("\nTest 6 - Full WHERE clause: {} rows (expected 3)", result.len());

    // Test 7: COUNT(*) with full WHERE
    let result = execute_sql(&mut db, "SELECT COUNT(*) FROM lineitem
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24").unwrap();
    println!("\nTest 7 - COUNT(*) with WHERE: {:?} (expected 3)", result[0].get(0));
}
