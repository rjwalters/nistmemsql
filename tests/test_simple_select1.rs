use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_simple_division() {
    let mut db = Database::new();
    
    // Create a simple table
    let setup_sql = "CREATE TABLE t1(a INTEGER, b INTEGER)";
    let stmt = Parser::parse_sql(setup_sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    }
    
    // Insert a row
    let insert_sql = "INSERT INTO t1 VALUES (1, 2)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }
    
    // Test division queries
    let test_cases = vec![
        ("SELECT 5 / 2", "Integer division"),
        ("SELECT 5.0 / 2.0", "Float division"),
        ("SELECT (1 + 2 + 3 + 4 + 5) / 5", "Integer sum division"),
    ];
    
    for (query, desc) in test_cases {
        println!("\n=== {} ===", desc);
        println!("Query: {}", query);
        
        let stmt = Parser::parse_sql(query).unwrap();
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            for row in rows {
                println!("Result: {:?}", row.values);
                if let Some(val) = row.values.first() {
                    match val {
                        vibesql_types::SqlValue::Integer(i) => {
                            println!("  Type: Integer, Value: {}", i);
                        }
                        vibesql_types::SqlValue::Float(f) => {
                            println!("  Type: Float, Value: {}", f);
                        }
                        vibesql_types::SqlValue::Numeric(n) => {
                            println!("  Type: Numeric, Value: {}", n);
                        }
                        _ => println!("  Type: {:?}", val),
                    }
                }
            }
        }
    }
}
