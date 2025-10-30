use super::super::*;

// ========================================================================
// Constraint Tests (Issue #214) - Column-level constraints
// ========================================================================

#[test]
fn test_parse_create_table_with_primary_key() {
    let result =
        Parser::parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));");
    assert!(result.is_ok(), "Should parse column-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::PrimaryKey, .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_unique() {
    let result = Parser::parse_sql("CREATE TABLE users (email VARCHAR(100) UNIQUE);");
    assert!(result.is_ok(), "Should parse UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::Unique, .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_check_constraint() {
    let result =
        Parser::parse_sql("CREATE TABLE products (price NUMERIC(10, 2) CHECK (price > 0));");
    assert!(result.is_ok(), "Should parse CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::Check(_), .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_references() {
    let result =
        Parser::parse_sql("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id));");
    assert!(result.is_ok(), "Should parse REFERENCES constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0] {
                ast::ColumnConstraint {
                    kind: ast::ColumnConstraintKind::References { table, column },
                    ..
                } => {
                    assert_eq!(table, "customers");
                    assert_eq!(column, "id");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_multiple_constraints() {
    let result = Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            salary NUMERIC(10, 2) CHECK (salary > 0),
            department_id INTEGER REFERENCES departments(id)
        );",
    );
    assert!(result.is_ok(), "Should parse multiple column constraints");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4);

            // id has PRIMARY KEY
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::PrimaryKey, .. }
            ));

            // email has UNIQUE
            assert_eq!(create.columns[1].constraints.len(), 1);
            assert!(matches!(
                create.columns[1].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::Unique, .. }
            ));

            // salary has CHECK
            assert_eq!(create.columns[2].constraints.len(), 1);
            assert!(matches!(
                create.columns[2].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::Check(_), .. }
            ));

            // department_id has REFERENCES
            assert_eq!(create.columns[3].constraints.len(), 1);
            assert!(matches!(
                create.columns[3].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::References { .. }, .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
