//! Test SELECT without FROM clause

use executor::SelectExecutor;
use parser::Parser;
use storage::Database;

#[test]
fn test_select_1() {
    let db = Database::new();

    // Parse SELECT 1;
    let stmt = Parser::parse_sql("SELECT 1;").expect("Failed to parse");

    match stmt {
        ast::Statement::Select(select_stmt) => {
            eprintln!("FROM clause: {:?}", select_stmt.from);
            assert!(select_stmt.from.is_none(), "FROM clause should be None");

            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute");

            eprintln!("Number of rows: {}", rows.len());
            assert_eq!(rows.len(), 1, "Should return exactly 1 row");

            eprintln!("First row: {:?}", rows[0]);
            assert_eq!(rows[0].values.len(), 1, "Should have exactly 1 column");
            assert_eq!(rows[0].values[0], types::SqlValue::Integer(1));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_expression() {
    let db = Database::new();

    // Parse SELECT 1 + 1;
    let stmt = Parser::parse_sql("SELECT 1 + 1;").expect("Failed to parse");

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert!(select_stmt.from.is_none(), "FROM clause should be None");

            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute");

            assert_eq!(rows.len(), 1, "Should return exactly 1 row");
            assert_eq!(rows[0].values.len(), 1, "Should have exactly 1 column");
            assert_eq!(rows[0].values[0], types::SqlValue::Integer(2));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_multiple_expressions() {
    let db = Database::new();

    // Parse SELECT 1, 2, 3;
    let stmt = Parser::parse_sql("SELECT 1, 2, 3;").expect("Failed to parse");

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert!(select_stmt.from.is_none(), "FROM clause should be None");

            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute");

            assert_eq!(rows.len(), 1, "Should return exactly 1 row");
            assert_eq!(rows[0].values.len(), 3, "Should have exactly 3 columns");
            assert_eq!(rows[0].values[0], types::SqlValue::Integer(1));
            assert_eq!(rows[0].values[1], types::SqlValue::Integer(2));
            assert_eq!(rows[0].values[2], types::SqlValue::Integer(3));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_count_star_without_from() {
    let db = Database::new();

    // Parse SELECT COUNT(*);
    let stmt = Parser::parse_sql("SELECT COUNT(*);").expect("Failed to parse");

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert!(select_stmt.from.is_none(), "FROM clause should be None");

            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute");

            eprintln!("Rows: {:?}", rows);
            assert_eq!(rows.len(), 1, "Should return exactly 1 row");
            assert_eq!(rows[0].values.len(), 1, "Should have exactly 1 column");
            // COUNT(*) with no rows should return 0
            assert_eq!(rows[0].values[0], types::SqlValue::Integer(0));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_aggregate_expression_without_from() {
    let db = Database::new();

    // Parse SELECT COUNT(*) + 1;
    let stmt = Parser::parse_sql("SELECT COUNT(*) + 1;").expect("Failed to parse");

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert!(select_stmt.from.is_none(), "FROM clause should be None");

            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute");

            assert_eq!(rows.len(), 1, "Should return exactly 1 row");
            assert_eq!(rows[0].values.len(), 1, "Should have exactly 1 column");
            // COUNT(*) + 1 = 0 + 1 = 1
            assert_eq!(rows[0].values[0], types::SqlValue::Integer(1));
        }
        _ => panic!("Expected SELECT statement"),
    }
}
