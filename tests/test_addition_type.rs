use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_addition_type() {
    let mut db = Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL);
    println!("Database SQL mode: {:?}", db.sql_mode());
    
    // Create test table
    let setup_sql = "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)";
    let stmt = Parser::parse_sql(setup_sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    }
    
    // Insert test data
    let insert_sql = "INSERT INTO t1 VALUES (1, 2, 3, 4, 5)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }
    
    // Test intermediate expressions
    let test_cases = vec![
        ("SELECT a FROM t1", "Single column"),
        ("SELECT a+b FROM t1", "Integer + Integer"),
        ("SELECT a+b+c FROM t1", "Integer + Integer + Integer"),
        ("SELECT a+b+c+d+e FROM t1", "Sum of all columns"),
        ("SELECT (a+b+c+d+e) FROM t1", "Sum in parentheses"),
        ("SELECT (a+b+c+d+e)/5 FROM t1", "Sum divided by literal"),
    ];
    
    for (query_sql, desc) in test_cases {
        println!("\n{}: {}", desc, query_sql);
        
        let stmt = Parser::parse_sql(query_sql).unwrap();
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            if let Some(row) = rows.first() {
                if let Some(val) = row.values.first() {
                    match val {
                        vibesql_types::SqlValue::Integer(n) => {
                            println!("  Result: Integer({})", n);
                        }
                        vibesql_types::SqlValue::Numeric(n) => {
                            println!("  Result: Numeric({})", n);
                        }
                        vibesql_types::SqlValue::Float(f) => {
                            println!("  Result: Float({})", f);
                        }
                        _ => println!("  Result: {:?}", val),
                    }
                }
            }
        }
    }
}
