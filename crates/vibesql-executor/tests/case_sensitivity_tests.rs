//! Comprehensive tests for case-insensitive identifier lookups
//!
//! Tests cover:
//! - Basic table lookups with different cases
//! - View lookups with different cases
//! - DROP statements with case variations
//! - Setting toggle behavior
//! - Mixed case scenarios

use vibesql_catalog::{Catalog, ColumnSchema, TableSchema, ViewDefinition};
use vibesql_storage::Database;
use vibesql_types::DataType;

/// Test basic table creation and lookup with different cases (case-insensitive mode)
#[test]
fn test_table_lookup_case_insensitive() {
    let mut db = Database::new();

    // Create table with lowercase name
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Should find table with exact case
    assert!(db.get_table("users").is_some());

    // Should find table with uppercase (case-insensitive by default)
    assert!(db.get_table("USERS").is_some());

    // Should find table with mixed case
    assert!(db.get_table("Users").is_some());
    assert!(db.get_table("uSeRs").is_some());
}

/// Test table creation with uppercase and lookup with lowercase
#[test]
fn test_table_lookup_created_uppercase() {
    let mut db = Database::new();

    // Create table with uppercase name
    let schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("NAME".to_string(), DataType::Varchar { max_length: Some(100) }, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Should find with exact case
    assert!(db.get_table("PRODUCTS").is_some());

    // Should find with lowercase (case-insensitive)
    assert!(db.get_table("products").is_some());

    // Should find with mixed case
    assert!(db.get_table("Products").is_some());
}

/// Test mixed case table creation
#[test]
fn test_table_lookup_mixed_case() {
    let mut db = Database::new();

    // Create table with mixed case name
    let schema = TableSchema::new(
        "MyTable".to_string(),
        vec![
            ColumnSchema::new("MyColumn".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Should find with different case variations
    assert!(db.get_table("MyTable").is_some());
    assert!(db.get_table("MYTABLE").is_some());
    assert!(db.get_table("mytable").is_some());
    assert!(db.get_table("myTABLE").is_some());
}

/// Test DROP TABLE with different cases (case-insensitive mode)
#[test]
fn test_drop_table_case_insensitive() {
    let mut db = Database::new();

    // Create table with lowercase
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Drop with uppercase should work
    assert!(db.drop_table("ORDERS").is_ok());

    // Table should be gone
    assert!(db.get_table("orders").is_none());
    assert!(db.get_table("ORDERS").is_none());
}

/// Test DROP TABLE with mixed case
#[test]
fn test_drop_table_mixed_case() {
    let mut db = Database::new();

    // Create with mixed case
    let schema = TableSchema::new(
        "CustomerOrders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Drop with different case
    assert!(db.drop_table("customerorders").is_ok());

    // Verify it's dropped
    assert!(db.get_table("CustomerOrders").is_none());
}

/// Test case-sensitive mode behavior
#[test]
fn test_case_sensitive_mode() {
    let mut db = Database::new();

    // Enable case-sensitive mode
    db.catalog_mut().set_case_sensitive_identifiers(true);

    // Create table with lowercase
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema1).unwrap();

    // Should find with exact case only
    assert!(db.get_table("users").is_some());
    assert!(db.get_table("USERS").is_none());
    assert!(db.get_table("Users").is_none());

    // Create another table with uppercase
    let schema2 = TableSchema::new(
        "USERS".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    // This should succeed since case-sensitive mode allows both "users" and "USERS"
    db.create_table(schema2).unwrap();

    // Now both should exist separately
    assert!(db.get_table("users").is_some());
    assert!(db.get_table("USERS").is_some());
}

/// Test toggling case sensitivity setting
#[test]
fn test_toggle_case_sensitivity() {
    let mut db = Database::new();

    // Default is case-insensitive
    assert!(!db.catalog().case_sensitive_identifiers());

    // Create table in case-insensitive mode
    let schema = TableSchema::new(
        "products".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Should find with different cases
    assert!(db.get_table("PRODUCTS").is_some());

    // Switch to case-sensitive
    db.catalog_mut().set_case_sensitive_identifiers(true);
    assert!(db.catalog().case_sensitive_identifiers());

    // Now lookups should be case-sensitive
    // Note: The table is stored as "PRODUCTS" due to previous normalization
    assert!(db.get_table("products").is_none());
    assert!(db.get_table("PRODUCTS").is_some());

    // Switch back to case-insensitive
    db.catalog_mut().set_case_sensitive_identifiers(false);

    // Should work again
    assert!(db.get_table("products").is_some());
    assert!(db.get_table("PRODUCTS").is_some());
}

/// Test view creation and lookup with case insensitivity
#[test]
fn test_view_lookup_case_insensitive() {
    let mut db = Database::new();

    // Create a base table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create a view with lowercase
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        projection: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let view = ViewDefinition {
        name: "active_users".to_string(),
        query: select_stmt,
        columns: None,
    };
    db.catalog_mut().create_view(view).unwrap();

    // Should find view with exact case
    assert!(db.catalog().get_view("active_users").is_some());

    // Should find with different cases (case-insensitive by default)
    assert!(db.catalog().get_view("ACTIVE_USERS").is_some());
    assert!(db.catalog().get_view("Active_Users").is_some());
    assert!(db.catalog().get_view("AcTiVe_UsErS").is_some());
}

/// Test view creation with uppercase and lookup with lowercase
#[test]
fn test_view_lookup_created_uppercase() {
    let mut db = Database::new();

    // Create a base table
    let schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create view with uppercase
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        projection: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "ID".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "ORDERS".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let view = ViewDefinition {
        name: "ORDER_VIEW".to_string(),
        query: select_stmt,
        columns: None,
    };
    db.catalog_mut().create_view(view).unwrap();

    // Should find with different cases
    assert!(db.catalog().get_view("ORDER_VIEW").is_some());
    assert!(db.catalog().get_view("order_view").is_some());
    assert!(db.catalog().get_view("Order_View").is_some());
}

/// Test DROP VIEW with case insensitivity
#[test]
fn test_drop_view_case_insensitive() {
    let mut db = Database::new();

    // Create a base table
    let schema = TableSchema::new(
        "products".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create view
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        projection: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let view = ViewDefinition {
        name: "product_view".to_string(),
        query: select_stmt,
        columns: None,
    };
    db.catalog_mut().create_view(view).unwrap();

    // Drop with uppercase
    assert!(db.catalog_mut().drop_view("PRODUCT_VIEW", false).is_ok());

    // Should be gone
    assert!(db.catalog().get_view("product_view").is_none());
    assert!(db.catalog().get_view("PRODUCT_VIEW").is_none());
}

/// Test view in case-sensitive mode
#[test]
fn test_view_case_sensitive_mode() {
    let mut db = Database::new();

    // Enable case-sensitive mode
    db.catalog_mut().set_case_sensitive_identifiers(true);

    // Create base table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create view with lowercase
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        projection: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let view = ViewDefinition {
        name: "user_view".to_string(),
        query: select_stmt,
        columns: None,
    };
    db.catalog_mut().create_view(view).unwrap();

    // Should only find with exact case
    assert!(db.catalog().get_view("user_view").is_some());
    assert!(db.catalog().get_view("USER_VIEW").is_none());
    assert!(db.catalog().get_view("User_View").is_none());
}

/// Test that table not found errors work correctly
#[test]
fn test_table_not_found_case_variations() {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "existing_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Non-existent table should return None regardless of case
    assert!(db.get_table("nonexistent").is_none());
    assert!(db.get_table("NONEXISTENT").is_none());
    assert!(db.get_table("NonExistent").is_none());
}

/// Test DROP of non-existent table with case variations
#[test]
fn test_drop_nonexistent_table_case_variations() {
    let db = Database::new();

    // Dropping non-existent table should fail regardless of case
    assert!(db.drop_table("nonexistent").is_err());
    assert!(db.drop_table("NONEXISTENT").is_err());
    assert!(db.drop_table("NonExistent").is_err());
}
