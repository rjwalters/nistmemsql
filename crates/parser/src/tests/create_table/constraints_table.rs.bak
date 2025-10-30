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
                    assert_eq!(columns[0], "order_id");
                    assert_eq!(columns[1], "product_id");
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
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "customer_id");
                    assert_eq!(references_table, "customers");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "id");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
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
                    assert_eq!(columns[0], "email");
                    assert_eq!(columns[1], "username");
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
