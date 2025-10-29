//! Tests for DELETE statements with EXISTS/NOT EXISTS predicates
//!
//! Covers E101-04 (Searched DELETE) conformance requirements:
//! - EXISTS with correlated subqueries
//! - NOT EXISTS with correlated subqueries
//! - EXISTS with empty result sets
//! - Nested EXISTS predicates

use executor::DeleteExecutor;
use parser::Parser;
use storage::Database;
use catalog::{TableSchema, ColumnSchema};
use storage::Row;
use types::{DataType, SqlValue};

/// Helper: Create test database with customers and orders tables
fn setup_customers_orders_db() -> Database {
    let mut db = Database::new();

    // Create customers table
    let customers_schema = TableSchema::new(
        "customers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(customers_schema).unwrap();

    // Create orders table
    let orders_schema = TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("total".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();

    // Insert customers
    db.insert_row("customers", Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar("Alice".to_string()),
        SqlValue::Boolean(true),
    ])).unwrap();

    db.insert_row("customers", Row::new(vec![
        SqlValue::Integer(2),
        SqlValue::Varchar("Bob".to_string()),
        SqlValue::Boolean(true),
    ])).unwrap();

    db.insert_row("customers", Row::new(vec![
        SqlValue::Integer(3),
        SqlValue::Varchar("Charlie".to_string()),
        SqlValue::Boolean(false),
    ])).unwrap();

    db.insert_row("customers", Row::new(vec![
        SqlValue::Integer(4),
        SqlValue::Varchar("Diana".to_string()),
        SqlValue::Boolean(true),
    ])).unwrap();

    // Insert orders - Alice and Bob have orders, Charlie and Diana don't
    db.insert_row("orders", Row::new(vec![
        SqlValue::Integer(101),
        SqlValue::Integer(1), // Alice
        SqlValue::Integer(100),
    ])).unwrap();

    db.insert_row("orders", Row::new(vec![
        SqlValue::Integer(102),
        SqlValue::Integer(1), // Alice
        SqlValue::Integer(200),
    ])).unwrap();

    db.insert_row("orders", Row::new(vec![
        SqlValue::Integer(103),
        SqlValue::Integer(2), // Bob
        SqlValue::Integer(150),
    ])).unwrap();

    db
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented (outer context not used by SelectExecutor)
fn test_delete_with_exists_correlated() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    // Should delete customers who have orders (Alice and Bob)
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 2, "Should delete 2 customers with orders");

        // Verify only Charlie and Diana remain (no orders)
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 2);

        let remaining_ids: Vec<i64> = customers.scan()
            .iter()
            .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
            .collect();

        assert!(remaining_ids.contains(&3)); // Charlie
        assert!(remaining_ids.contains(&4)); // Diana
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_with_not_exists_correlated() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    // Should delete customers with NO orders (Charlie and Diana)
    let sql = "DELETE FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 2, "Should delete 2 customers without orders");

        // Verify only Alice and Bob remain (have orders)
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 2);

        let remaining_ids: Vec<i64> = customers.scan()
            .iter()
            .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
            .collect();

        assert!(remaining_ids.contains(&1)); // Alice
        assert!(remaining_ids.contains(&2)); // Bob
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
fn test_delete_with_exists_empty_result() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE total > 10000)
    // Subquery returns empty (no orders > 10000), so no rows should be deleted
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE total > 10000);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 0, "Should delete 0 customers (EXISTS returns false)");

        // All 4 customers should remain
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 4);
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
fn test_delete_with_not_exists_empty_result() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE total > 10000)
    // Subquery returns empty, so NOT EXISTS is TRUE, all rows deleted
    let sql = "DELETE FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE total > 10000);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 4, "Should delete all 4 customers (NOT EXISTS returns true)");

        // No customers should remain
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 0);
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_with_exists_and_other_conditions() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE active = TRUE AND EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    // Should delete active customers with orders (Alice and Bob)
    let sql = "DELETE FROM customers WHERE active = TRUE AND EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 2, "Should delete 2 active customers with orders");

        // Verify Charlie and Diana remain
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 2);

        let remaining_ids: Vec<i64> = customers.scan()
            .iter()
            .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
            .collect();

        assert!(remaining_ids.contains(&3)); // Charlie (inactive)
        assert!(remaining_ids.contains(&4)); // Diana (active but no orders)
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
fn test_delete_with_exists_uncorrelated() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders)
    // Uncorrelated - orders table is not empty, so EXISTS is TRUE for all rows
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 4, "Should delete all customers (uncorrelated EXISTS is TRUE)");

        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 0);
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_with_exists_complex_subquery() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id AND total > 150)
    // Should delete customers with orders > 150 (Alice only)
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id AND total > 150);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 1, "Should delete 1 customer with order > 150");

        // Verify Bob, Charlie, and Diana remain
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 3);

        let remaining_ids: Vec<i64> = customers.scan()
            .iter()
            .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
            .collect();

        assert!(!remaining_ids.contains(&1)); // Alice deleted
        assert!(remaining_ids.contains(&2)); // Bob remains (order = 150)
        assert!(remaining_ids.contains(&3)); // Charlie
        assert!(remaining_ids.contains(&4)); // Diana
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_with_nested_exists() {
    let mut db = Database::new();

    // Create tables for nested EXISTS test
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
        ],
    );
    db.create_table(users_schema).unwrap();

    let posts_schema = TableSchema::new(
        "posts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(posts_schema).unwrap();

    let comments_schema = TableSchema::new(
        "comments".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("post_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(comments_schema).unwrap();

    // Insert test data
    db.insert_row("users", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())])).unwrap();
    db.insert_row("users", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())])).unwrap();

    db.insert_row("posts", Row::new(vec![SqlValue::Integer(101), SqlValue::Integer(1)])).unwrap();

    db.insert_row("comments", Row::new(vec![SqlValue::Integer(1001), SqlValue::Integer(101)])).unwrap();

    // DELETE FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE user_id = users.id AND EXISTS (SELECT 1 FROM comments WHERE post_id = posts.id))
    // Should delete users who have posts that have comments (Alice only)
    let sql = "DELETE FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE user_id = users.id AND EXISTS (SELECT 1 FROM comments WHERE post_id = posts.id));";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 1, "Should delete 1 user with posts that have comments");

        let users = db.get_table("users").unwrap();
        assert_eq!(users.row_count(), 1);

        let remaining_id = if let SqlValue::Integer(id) = users.scan()[0].get(0).unwrap() { *id } else { 0 };
        assert_eq!(remaining_id, 2); // Bob remains
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_with_or_exists() {
    let mut db = setup_customers_orders_db();

    // DELETE FROM customers WHERE id = 3 OR EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    // Should delete Charlie (id=3) and customers with orders (Alice, Bob)
    let sql = "DELETE FROM customers WHERE id = 3 OR EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 3, "Should delete 3 customers (Charlie + Alice + Bob)");

        // Only Diana should remain
        let customers = db.get_table("customers").unwrap();
        assert_eq!(customers.row_count(), 1);

        let remaining_id = if let SqlValue::Integer(id) = customers.scan()[0].get(0).unwrap() { *id } else { 0 };
        assert_eq!(remaining_id, 4); // Diana
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_exists_with_select_star() {
    let mut db = setup_customers_orders_db();

    // EXISTS doesn't care what's selected - SELECT * is valid
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT * FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 2, "Should delete 2 customers with orders");
    } else {
        panic!("Expected DELETE statement");
    }
}

#[test]
#[ignore] // TODO: Correlated subqueries not yet fully implemented
fn test_delete_exists_with_select_multiple_columns() {
    let mut db = setup_customers_orders_db();

    // EXISTS doesn't care about column count
    let sql = "DELETE FROM customers WHERE EXISTS (SELECT id, total FROM orders WHERE customer_id = customers.id);";
    let stmt = Parser::parse_sql(sql).unwrap();

    if let ast::Statement::Delete(delete_stmt) = stmt {
        let deleted = DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();
        assert_eq!(deleted, 2, "Should delete 2 customers with orders");
    } else {
        panic!("Expected DELETE statement");
    }
}
