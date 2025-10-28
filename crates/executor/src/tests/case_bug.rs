//! Test to reproduce CASE expression bug (issue #240)

use crate::SelectExecutor;
use storage::Database;
use types::SqlValue;

#[test]
fn test_case_expression_with_from_clause() {
    let mut db = Database::new();

    // Create products table
    let create_stmt = parser::Parser::parse_sql(
        "CREATE TABLE products (
            product_name VARCHAR(100),
            unit_price INTEGER
        )",
    )
    .unwrap();

    if let ast::Statement::CreateTable(create_table) = create_stmt {
        crate::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
    }

    // Insert test data
    let insert_stmt = parser::Parser::parse_sql(
        "INSERT INTO products (product_name, unit_price) VALUES
        ('Widget', 5),
        ('Gadget', 25),
        ('Gizmo', 75)",
    )
    .unwrap();

    if let ast::Statement::Insert(insert) = insert_stmt {
        crate::InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    // Parse and execute the query
    let query = r#"SELECT
  product_name,
  unit_price,
  CASE
    WHEN unit_price < 10 THEN 'Budget'
    WHEN unit_price < 50 THEN 'Standard'
    ELSE 'Premium'
  END as price_category
FROM products
ORDER BY unit_price"#;

    let parsed_stmt = parser::Parser::parse_sql(query).unwrap();

    if let ast::Statement::Select(parsed) = parsed_stmt {
        println!("Parsed query: {:#?}", parsed);
        println!("FROM clause present: {}", parsed.from.is_some());

        if let Some(ref from) = parsed.from {
            println!("FROM clause: {:?}", from);
        }

        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&parsed);

        match result {
            Ok(rows) => {
                println!("Success! Got {} rows", rows.len());
                assert_eq!(rows.len(), 3);

                // Verify the results
                assert_eq!(rows[0].values[0], SqlValue::Varchar("Widget".to_string()));
                assert_eq!(rows[0].values[1], SqlValue::Integer(5));
                assert_eq!(rows[0].values[2], SqlValue::Varchar("Budget".to_string()));

                assert_eq!(rows[1].values[0], SqlValue::Varchar("Gadget".to_string()));
                assert_eq!(rows[1].values[1], SqlValue::Integer(25));
                assert_eq!(rows[1].values[2], SqlValue::Varchar("Standard".to_string()));

                assert_eq!(rows[2].values[0], SqlValue::Varchar("Gizmo".to_string()));
                assert_eq!(rows[2].values[1], SqlValue::Integer(75));
                assert_eq!(rows[2].values[2], SqlValue::Varchar("Premium".to_string()));
            }
            Err(e) => {
                panic!("Query failed with error: {:?}", e);
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
}
