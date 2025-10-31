//! Tests for CREATE VIEW and DROP VIEW (SQL:1999 views)

use ast::Statement;
use executor::advanced_objects::{execute_create_view, execute_drop_view};
use parser::Parser;
use storage::Database;

#[test]
fn test_create_view_simple() {
    let mut db = Database::new();

    // Create view
    let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE VIEW");

    match stmt {
        Statement::CreateView(view_stmt) => {
            execute_create_view(&view_stmt, &mut db).expect("Failed to create view");

            // Verify view exists in catalog
            let view = db.catalog.get_view("ACTIVE_USERS");
            assert!(view.is_some(), "View should exist in catalog");

            let view = view.unwrap();
            assert_eq!(view.name, "ACTIVE_USERS");
            assert!(view.columns.is_none());
            assert!(!view.with_check_option);
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_create_view_with_column_list() {
    let mut db = Database::new();

    let sql = "CREATE VIEW emp_view (employee_id, employee_name) AS SELECT id, name FROM employees";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            execute_create_view(&view_stmt, &mut db).expect("Failed to create view");

            let view = db.catalog.get_view("EMP_VIEW");
            assert!(view.is_some());

            let view = view.unwrap();
            assert_eq!(view.name, "EMP_VIEW");
            assert!(view.columns.is_some());

            let cols = view.columns.as_ref().unwrap();
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0], "EMPLOYEE_ID");
            assert_eq!(cols[1], "EMPLOYEE_NAME");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_create_view_with_check_option() {
    let mut db = Database::new();

    let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            execute_create_view(&view_stmt, &mut db).expect("Failed to create view");

            let view = db.catalog.get_view("ACTIVE_USERS");
            assert!(view.is_some());

            let view = view.unwrap();
            assert!(view.with_check_option);
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_drop_view_simple() {
    let mut db = Database::new();

    // Create view first
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(view_stmt) = stmt {
        execute_create_view(&view_stmt, &mut db).expect("Failed to create view");
    }

    // Verify view exists
    assert!(db.catalog.get_view("MY_VIEW").is_some());

    // Drop view
    let sql = "DROP VIEW my_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP VIEW");

    match stmt {
        Statement::DropView(drop_stmt) => {
            execute_drop_view(&drop_stmt, &mut db).expect("Failed to drop view");

            // Verify view no longer exists
            assert!(db.catalog.get_view("MY_VIEW").is_none());
        }
        _ => panic!("Expected DropView statement"),
    }
}

#[test]
fn test_create_view_duplicate_error() {
    let mut db = Database::new();

    // Create first view
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(view_stmt) = stmt {
        execute_create_view(&view_stmt, &mut db).expect("Failed to create view");
    }

    // Try to create duplicate view
    let sql = "CREATE VIEW my_view AS SELECT * FROM employees";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            let result = execute_create_view(&view_stmt, &mut db);
            assert!(result.is_err(), "Should fail when creating duplicate view");

            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("already exists"), "Error should mention view already exists");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_drop_view_not_found_error() {
    let mut db = Database::new();

    let sql = "DROP VIEW nonexistent_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::DropView(drop_stmt) => {
            let result = execute_drop_view(&drop_stmt, &mut db);
            assert!(result.is_err(), "Should fail when dropping non-existent view");

            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("not found"), "Error should mention view not found");
        }
        _ => panic!("Expected DropView statement"),
    }
}
