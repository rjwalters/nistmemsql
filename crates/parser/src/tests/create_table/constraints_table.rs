use super::super::*;

// ========================================================================
// Constraint Tests - Table-level constraints
// ========================================================================

#[test]
fn test_parse_create_table_with_table_level_primary_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            PRIMARY KEY (order_id, product_id)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint {
                    kind: ast::TableConstraintKind::PrimaryKey { columns },
                    ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0], "ORDER_ID");
                    assert_eq!(columns[1], "PRODUCT_ID");
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint {
                    kind:
                        ast::TableConstraintKind::ForeignKey {
                            columns,
                            references_table,
                            references_columns,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "CUSTOMER_ID");
                    assert_eq!(references_table, "CUSTOMERS");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "ID");
                    assert!(on_delete.is_none());
                    assert!(on_update.is_none());
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key_on_delete_update() {
    let result = Parser::parse_sql(
        "CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY with ON DELETE/UPDATE");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            // Find the REFERENCES constraint in column constraints
            let column = &create.columns[1]; // parent_id column
            assert_eq!(column.constraints.len(), 1);
            match &column.constraints[0] {
                ast::ColumnConstraint {
                    kind:
                        ast::ColumnConstraintKind::References {
                            table,
                            column: col,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(table, "parent");
                    assert_eq!(col, "id");
                    assert_eq!(on_delete, &Some(ast::ReferentialAction::Cascade));
                    assert_eq!(on_update, &Some(ast::ReferentialAction::SetNull));
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_foreign_key_on_delete_update() {
    let result = Parser::parse_sql(
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            customer_id INT,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE NO ACTION ON UPDATE SET DEFAULT
        );",
    );
    assert!(result.is_ok(), "Should parse table-level FOREIGN KEY with ON DELETE/UPDATE");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint {
                    kind:
                        ast::TableConstraintKind::ForeignKey {
                            columns,
                            references_table,
                            references_columns,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "CUSTOMER_ID");
                    assert_eq!(references_table, "CUSTOMERS");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "ID");
                    assert_eq!(on_delete, &Some(ast::ReferentialAction::NoAction));
                    assert_eq!(on_update, &Some(ast::ReferentialAction::SetDefault));
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key_on_delete_only() {
    let result = Parser::parse_sql(
        "CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES parent(id) ON DELETE SET DEFAULT
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY with ON DELETE only");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            let column = &create.columns[1];
            match &column.constraints[0] {
                ast::ColumnConstraint {
                    kind: ast::ColumnConstraintKind::References { on_delete, on_update, .. },
                    ..
                } => {
                    assert_eq!(on_delete, &Some(ast::ReferentialAction::SetDefault));
                    assert!(on_update.is_none());
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_unique() {
    let result = Parser::parse_sql(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100),
            username VARCHAR(50),
            UNIQUE (email, username)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint {
                    kind: ast::TableConstraintKind::Unique { columns }, ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0], "EMAIL");
                    assert_eq!(columns[1], "USERNAME");
                }
                _ => panic!("Expected UNIQUE constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_check() {
    let result = Parser::parse_sql(
        "CREATE TABLE products (
            price NUMERIC(10, 2),
            discount NUMERIC(10, 2),
            CHECK (discount < price)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            assert!(matches!(
                create.table_constraints[0],
                ast::TableConstraint { kind: ast::TableConstraintKind::Check { .. }, .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
