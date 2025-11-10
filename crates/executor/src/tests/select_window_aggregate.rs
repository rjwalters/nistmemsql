//! Tests for aggregate window functions in SELECT statements

use catalog::{ColumnSchema, TableSchema};
use parser::Parser;
use storage::Database;
use types::{DataType, SqlValue};

use crate::SelectExecutor;

fn create_test_db() -> Database {
    let mut db = Database::new();

    // Create a simple test table
    // Table and column names must be uppercase to match SQL identifier normalization
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
            ColumnSchema::new("DAY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("SALES").unwrap();
    use storage::Row;
    table
        .insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Integer(1)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200), SqlValue::Integer(2)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(300), SqlValue::Integer(3)]))
        .unwrap();

    db
}

#[test]
fn test_count_star_window_function() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, COUNT(*) OVER () as total_count FROM sales
    let query = "SELECT id, COUNT(*) OVER () as total_count FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows (one for each row in sales)
        assert_eq!(result.len(), 3);

        // Each row should have 2 columns: id and total_count
        assert_eq!(result[0].values.len(), 2);

        // All rows should have count = 3 (total rows)
        assert_eq!(result[0].values[1], SqlValue::Numeric(3.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(3.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(3.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_sum_window_running_total() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, SUM(amount) OVER (ORDER BY day) as running_total FROM sales
    let query = "SELECT id, SUM(amount) OVER (ORDER BY day) as running_total FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Each row should have 2 columns: id and running_total
        assert_eq!(result[0].values.len(), 2);

        // Verify running totals: 100, 300, 600
        assert_eq!(result[0].values[1], SqlValue::Numeric(100.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(300.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(600.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_avg_window_function() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, AVG(amount) OVER () as avg_amount FROM sales
    let query = "SELECT id, AVG(amount) OVER () as avg_amount FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 2);

        // Average of 100, 200, 300 = 200
        assert_eq!(result[0].values[1], SqlValue::Numeric(200.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(200.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(200.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_min_max_window_functions() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, MIN(amount) OVER () as min_amt, MAX(amount) OVER () as max_amt FROM sales
    let query =
        "SELECT id, MIN(amount) OVER () as min_amt, MAX(amount) OVER () as max_amt FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 3); // id, min_amt, max_amt

        // MIN = 100, MAX = 300 for all rows
        assert_eq!(result[0].values[1], SqlValue::Integer(100));
        assert_eq!(result[0].values[2], SqlValue::Integer(300));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_in_expression() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT amount, amount * 100 / SUM(amount) OVER () as percentage FROM sales
    // This tests window functions in complex expressions
    let query = "SELECT amount, amount * 100 / SUM(amount) OVER () as percentage FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 2);

        // Total sum = 600
        // Row 1: 100 * 100 / 600 = 16.666...
        // Row 2: 200 * 100 / 600 = 33.333...
        // Row 3: 300 * 100 / 600 = 50
        assert_eq!(result[0].values[1], SqlValue::Float(16.666666666666668));
        assert_eq!(result[1].values[1], SqlValue::Float(33.33333333333333));
        assert_eq!(result[2].values[1], SqlValue::Float(50.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_with_moving_frame() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as
    // moving_avg FROM sales
    let query = "SELECT id, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Row 1: AVG(100) = 100
        // Row 2: AVG(100, 200) = 150
        // Row 3: AVG(200, 300) = 250
        assert_eq!(result[0].values[1], SqlValue::Numeric(100.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(150.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(250.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_multiple_window_functions_same_query() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, COUNT(*) OVER () as cnt, SUM(amount) OVER () as total FROM sales
    let query = "SELECT id, COUNT(*) OVER () as cnt, SUM(amount) OVER () as total FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 3); // id, cnt, total

        // All rows should have cnt=3 and total=600
        for row in &result {
            assert_eq!(row.values[1], SqlValue::Numeric(3.0));
            assert_eq!(row.values[2], SqlValue::Numeric(600.0));
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_with_partition_by() {
    let mut db = Database::new();

    // Create table with department data
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("DEPT".to_string(), DataType::Integer, false),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data - 2 departments
    use storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(2),
            SqlValue::Integer(70000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(2),
            SqlValue::Integer(80000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM employees
    let query = "SELECT dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM employees";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 4);

        // Department 1: AVG(50000, 60000) = 55000
        // Department 2: AVG(70000, 80000) = 75000
        // Check that rows with dept=1 have avg=55000 and dept=2 have avg=75000
        for row in &result {
            if row.values[0] == SqlValue::Integer(1) {
                assert_eq!(row.values[1], SqlValue::Numeric(55000.0));
            } else if row.values[0] == SqlValue::Integer(2) {
                assert_eq!(row.values[1], SqlValue::Numeric(75000.0));
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_window_function() {
    use storage::Row;
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(70000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test ORDER BY referencing a window function from SELECT
    let query = "SELECT name, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM employees ORDER BY ROW_NUMBER() OVER (ORDER BY salary)";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Results should be ordered by ROW_NUMBER (which is 1, 2, 3)
        // Since ROW_NUMBER is ordered by salary ASC, Alice (50000) should be first
        assert_eq!(result[0].values[0], SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[1], SqlValue::Integer(1));

        assert_eq!(result[1].values[0], SqlValue::Varchar("Bob".to_string()));
        assert_eq!(result[1].values[1], SqlValue::Integer(2));

        assert_eq!(result[2].values[0], SqlValue::Varchar("Charlie".to_string()));
        assert_eq!(result[2].values[1], SqlValue::Integer(3));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_window_function_not_in_select() {
    use storage::Row;
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(70000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test ORDER BY with window function NOT in SELECT list
    let query = "SELECT name FROM employees ORDER BY ROW_NUMBER() OVER (ORDER BY salary DESC)";
    let stmt = Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Results should be ordered by ROW_NUMBER with salary DESC
        // Charlie (70000) gets ROW_NUMBER=1, Bob (60000) gets 2, Alice (50000) gets 3
        assert_eq!(result[0].values[0], SqlValue::Varchar("Charlie".to_string()));
        assert_eq!(result[1].values[0], SqlValue::Varchar("Bob".to_string()));
        assert_eq!(result[2].values[0], SqlValue::Varchar("Alice".to_string()));
    } else {
        panic!("Expected SELECT statement");
    }
}
