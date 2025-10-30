use super::super::*;

// ========================================================================
// Northwind Schema Examples - Real-world test cases
// ========================================================================

#[test]
fn test_parse_northwind_categories_table() {
    let result = Parser::parse_sql(
        "CREATE TABLE Categories (
            CategoryID INTEGER PRIMARY KEY,
            CategoryName VARCHAR(15),
            Description VARCHAR(255)
        );",
    );
    assert!(result.is_ok(), "Should parse northwind Categories table");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "CATEGORIES");
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "CATEGORYID");
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
fn test_parse_northwind_products_table() {
    let result = Parser::parse_sql(
        "CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            category_id INTEGER,
            unit_price DECIMAL(10, 2),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );",
    );
    assert!(result.is_ok(), "Should parse northwind products table with FOREIGN KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "PRODUCTS");
            assert_eq!(create.columns.len(), 4);

            // product_id has PRIMARY KEY
            assert_eq!(create.columns[0].name, "PRODUCT_ID");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint { kind: ast::ColumnConstraintKind::PrimaryKey, .. }
            ));

            // product_name has NOT NULL (nullable = false)
            assert_eq!(create.columns[1].name, "PRODUCT_NAME");
            assert!(!create.columns[1].nullable, "product_name should be NOT NULL");

            // Table has FOREIGN KEY constraint
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
                    assert_eq!(columns[0], "CATEGORY_ID");
                    assert_eq!(references_table, "CATEGORIES");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "CATEGORY_ID");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
