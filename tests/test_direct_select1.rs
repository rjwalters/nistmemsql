use std::fs;

#[test]
fn test_select1_direct() {
    let test_content = fs::read_to_string("third_party/sqllogictest/test/select1.test")
        .expect("Failed to read select1.test");
    
    println!("=== select1.test ===");
    println!("File size: {} bytes", test_content.len());
    println!("Number of lines: {}", test_content.lines().count());
    
    // Count different line types
    let mut query_count = 0;
    let mut statement_count = 0;
    let mut hash_count = 0;
    
    for line in test_content.lines() {
        if line.trim().starts_with("query ") {
            query_count += 1;
        } else if line.trim().starts_with("statement ") {
            statement_count += 1;
        } else if line.contains("hashing to") {
            hash_count += 1;
        }
    }
    
    println!("\nTest content statistics:");
    println!("  Statements: {}", statement_count);
    println!("  Queries: {}", query_count);
    println!("  Hash results: {}", hash_count);
    println!("  Total test cases: {}", statement_count + query_count);
    
    // Parse using sqllogictest Runner directly
    use sqllogictest::Runner;
    
    let mut runner = Runner::new(|| async { Ok(test_db::TestDB::new()) });
    runner.with_hash_threshold(8);
    
    match runner.run_script(&test_content) {
        Ok(_) => println!("\n✅ select1.test PASSED"),
        Err(e) => println!("\n❌ select1.test FAILED: {}", e),
    }
}

// Minimal test database for direct execution
mod test_db {
    use async_trait::async_trait;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use vibesql_executor::SelectExecutor;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    pub struct TestError(String);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Error for TestError {}

    pub struct TestDB {
        db: Database,
    }

    impl TestDB {
        pub fn new() -> Self {
            TestDB {
                db: Database::new().with_sql_mode(vibesql_types::SqlMode::MySQL),
            }
        }
    }

    #[async_trait]
    impl AsyncDB for TestDB {
        type Error = TestError;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let stmt = Parser::parse_sql(sql)
                .map_err(|e| TestError(format!("Parse error: {:?}", e)))?;

            match stmt {
                vibesql_ast::Statement::CreateTable(create_stmt) => {
                    vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                        .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                    Ok(DBOutput::StatementComplete(0))
                }
                vibesql_ast::Statement::Insert(insert_stmt) => {
                    vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                        .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                    Ok(DBOutput::StatementComplete(0))
                }
                vibesql_ast::Statement::Select(select_stmt) => {
                    let executor = SelectExecutor::new(&self.db);
                    let rows = executor
                        .execute(&select_stmt)
                        .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;

                    // Convert rows to string format
                    let types: Vec<DefaultColumnType> = if rows.is_empty() {
                        vec![]
                    } else {
                        rows[0]
                            .values
                            .iter()
                            .map(|val| match val {
                                vibesql_types::SqlValue::Integer(_)
                                | vibesql_types::SqlValue::Smallint(_)
                                | vibesql_types::SqlValue::Bigint(_)
                                | vibesql_types::SqlValue::Unsigned(_) => DefaultColumnType::Integer,
                                vibesql_types::SqlValue::Float(_)
                                | vibesql_types::SqlValue::Real(_)
                                | vibesql_types::SqlValue::Double(_)
                                | vibesql_types::SqlValue::Numeric(_) => DefaultColumnType::FloatingPoint,
                                _ => DefaultColumnType::Text,
                            })
                            .collect()
                    };

                    let formatted_rows: Vec<Vec<String>> = rows
                        .iter()
                        .map(|row| {
                            row.values
                                .iter()
                                .map(|val| match val {
                                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                                    vibesql_types::SqlValue::Float(f) => {
                                        if f.fract() == 0.0 {
                                            format!("{:.1}", f)
                                        } else {
                                            f.to_string()
                                        }
                                    }
                                    vibesql_types::SqlValue::Numeric(n) => n.to_string(),
                                    _ => format!("{:?}", val),
                                })
                                .collect()
                        })
                        .collect();

                    Ok(DBOutput::Rows { types, rows: formatted_rows })
                }
                _ => Err(TestError("Unsupported statement type".to_string())),
            }
        }

        async fn shutdown(&mut self) {}
    }
}
