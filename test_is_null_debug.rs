use executor::SelectExecutor;
use parser::Parser;
use storage::Database;

fn main() {
    let db = Database::new();

    // First test just the arithmetic expression
    let arith_sql = "SELECT (15 * -99)";
    println!("Testing arithmetic: {}", arith_sql);
    let arith_stmt = Parser::parse_sql(arith_sql).expect("Failed to parse");
    match arith_stmt {
        ast::Statement::Select(arith_select) => {
            let arith_executor = SelectExecutor::new(&db);
            match arith_executor.execute(&arith_select) {
                Ok(arith_rows) => {
                    println!("Arithmetic result: {} rows", arith_rows.len());
                    for row in &arith_rows {
                        println!("  Row: {:?}", row);
                    }
                }
                Err(e) => {
                    println!("Arithmetic error: {:?}", e);
                }
            }
        }
        _ => println!("Not a SELECT statement"),
    }

    // Test IS NULL evaluation
    let sql = "SELECT 1 WHERE (15 * -99) IS NULL";
    println!("Testing IS NULL: {}", sql);

    // Test integer expression in WHERE (should work now)
    let sql2 = "SELECT 1 WHERE (15 * -99)";
    println!("Testing integer WHERE: {}", sql2);

    // Test both queries
    for (i, sql) in [sql, sql2].iter().enumerate() {
        println!("\n--- Test {} ---", i + 1);
        // Parse the SQL
        let stmt = match Parser::parse_sql(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("Parse error: {:?}", e);
                continue;
            }
        };

        // Debug: print the AST
        println!("Parsed AST: {:?}", stmt);

        match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                match executor.execute(&select_stmt) {
                    Ok(rows) => {
                        println!("Success: {} rows", rows.len());
                        for row in &rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    Err(e) => {
                        println!("Execution error: {:?}", e);
                    }
                }
            }
            _ => println!("Not a SELECT statement"),
        }
    }
}
