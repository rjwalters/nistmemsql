use storage::Database;
use ast::{CreateIndexStmt, IndexColumn, OrderDirection, Statement};
use crate::{IndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor};

#[test]
fn test_multi_column_index_creation() {
    let mut db = Database::new();
    
    // Create a test table
    let create_table_sql = r#"
        CREATE TABLE users (
            id INTEGER,
            email VARCHAR(100),
            name VARCHAR(50),
            age INTEGER
        )
    "#;
    
    let stmt = parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
    
    // Test multi-column index creation
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_users_email_name".to_string(),
        table_name: "users".to_string(),
        unique: false,
        columns: vec![
            IndexColumn { column_name: "email".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "name".to_string(), direction: OrderDirection::Desc },
        ],
    };
    
    // This should succeed now
    let result = IndexExecutor::execute(&create_index_stmt, &mut db);
    assert!(result.is_ok(), "Multi-column index creation failed: {:?}", result);
    
    // Verify the index was created
    assert!(db.index_exists("idx_users_email_name"));
    
    // Insert some data
    let insert_sql = "INSERT INTO users VALUES (1, 'alice@example.com', 'Alice', 25)";
    let stmt = parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT statement"),
    }
    
    // Query the data back
    let select_sql = "SELECT * FROM users";
    let stmt = parser::Parser::parse_sql(select_sql).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values.len(), 4);
        }
        _ => panic!("Expected SELECT statement"),
    }
    
    println!("✅ Multi-column index creation test passed!");
}
