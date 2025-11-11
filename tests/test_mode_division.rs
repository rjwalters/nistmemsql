use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_division_in_mysql_mode() {
    let mut db = Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL);
    
    println!("Testing division in MySQL mode:");
    
    // Test cases
    let test_cases = vec![
        "SELECT 5 / 2",
        "SELECT 10 / 3",
        "SELECT 1 / 5",
    ];
    
    for query_sql in test_cases {
        println!("\n  Query: {}", query_sql);
        
        let stmt = Parser::parse_sql(query_sql).unwrap();
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            if let Some(row) = rows.first() {
                if let Some(val) = row.values.first() {
                    match val {
                        vibesql_types::SqlValue::Numeric(n) => {
                            println!("    Result: Numeric({})", n);
                        }
                        vibesql_types::SqlValue::Float(f) => {
                            println!("    Result: Float({})", f);
                        }
                        vibesql_types::SqlValue::Integer(i) => {
                            println!("    Result: Integer({})", i);
                        }
                        _ => println!("    Result: {:?}", val),
                    }
                }
            }
        }
    }
}
