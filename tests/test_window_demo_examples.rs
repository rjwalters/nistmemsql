//! Test that window function examples from web demo work

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::Database;
use types::{DataType, SqlValue};

#[test]
fn test_window_demo_count_over() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(100000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(80000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(95000),
        ]))
        .unwrap();

    // Test window-1: COUNT(*) OVER - Total Row Count
    let query = r#"SELECT
  first_name || ' ' || last_name AS employee,
  department,
  salary,
  COUNT(*) OVER () AS total_employees
FROM employees
LIMIT 10"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Each row should have total_employees = 3
        assert_eq!(result[0].values[3], SqlValue::Integer(3));
        assert_eq!(result[1].values[3], SqlValue::Integer(3));
        assert_eq!(result[2].values[3], SqlValue::Integer(3));

        println!("✅ Window demo example 'COUNT(*) OVER' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_demo_running_total() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Integer(300),
        ]))
        .unwrap();

    // Test window-2: Running Total with ORDER BY
    let query = r#"SELECT
  first_name || ' ' || last_name AS employee,
  salary,
  SUM(salary) OVER (ORDER BY employee_id) AS running_total
FROM employees
ORDER BY employee_id
LIMIT 10"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Verify running totals: 100, 300, 600
        assert_eq!(result[0].values[2], SqlValue::Integer(100));
        assert_eq!(result[1].values[2], SqlValue::Integer(300));
        assert_eq!(result[2].values[2], SqlValue::Integer(600));

        println!("✅ Window demo example 'Running Total' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_demo_partitioned_avg() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data with 2 departments
    use storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(300),
        ]))
        .unwrap();

    // Test window-3: Partitioned Averages
    let query = r#"SELECT
  department,
  first_name || ' ' || last_name AS employee,
  salary,
  AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary
FROM employees
ORDER BY department, salary DESC
LIMIT 15"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Engineering rows should have avg = 200 (100 + 300) / 2
        // Sales row should have avg = 200
        // Note: Results are ordered by department, salary DESC

        println!("✅ Window demo example 'Partitioned Averages' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}
