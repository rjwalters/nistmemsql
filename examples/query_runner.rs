//! Helper program to run queries and format results for examples.ts
//! Usage: cargo run --example query_runner

use executor::SelectExecutor;
use parser::Parser;
use storage::Database;
use catalog::{ColumnSchema, TableSchema};
use types::{DataType, SqlValue};

fn create_full_northwind_db() -> Database {
    let mut db = Database::new();

    // Create products table with all columns
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("product_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("product_name".to_string(), DataType::Varchar { max_length: 100 }, false),
            ColumnSchema::new("category_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("unit_price".to_string(), DataType::Float, false),
            ColumnSchema::new("units_in_stock".to_string(), DataType::Integer, false),
            ColumnSchema::new("units_on_order".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Create categories table
    let categories_schema = TableSchema::new(
        "categories".to_string(),
        vec![
            ColumnSchema::new("category_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("category_name".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("description".to_string(), DataType::Varchar { max_length: 200 }, false),
        ],
    );
    db.create_table(categories_schema).unwrap();

    // Insert products (matching basic-1 expected results)
    use storage::Row;
    let products_table = db.get_table_mut("products").unwrap();

    let products_data = vec![
        (1, "Chai", 1, 18.0, 39, 0),
        (2, "Chang", 1, 19.0, 17, 40),
        (3, "Aniseed Syrup", 2, 10.0, 13, 70),
        (4, "Chef Anton's Cajun Seasoning", 2, 22.0, 53, 0),
        (5, "Chef Anton's Gumbo Mix", 2, 21.35, 0, 0),
        (6, "Grandma's Boysenberry Spread", 2, 25.0, 120, 0),
        (7, "Northwoods Cranberry Sauce", 2, 40.0, 6, 0),
        (8, "Mishi Kobe Niku", 6, 97.0, 29, 0),
        (9, "Ikura", 8, 31.0, 31, 0),
        (10, "Queso Cabrales", 4, 21.0, 22, 30),
        (11, "Queso Manchego La Pastora", 4, 38.0, 86, 0),
        (12, "Sir Rodney's Marmalade", 3, 81.0, 40, 0),
        (13, "Carnarvon Tigers", 8, 62.5, 42, 0),
        (14, "Alice Mutton", 6, 39.0, 0, 0),
        (15, "Tofu", 7, 23.25, 35, 0),
    ];

    for (id, name, cat_id, price, stock, on_order) in products_data {
        products_table.insert(Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Varchar(name.to_string()),
            SqlValue::Integer(cat_id),
            SqlValue::Float(price),
            SqlValue::Integer(stock),
            SqlValue::Integer(on_order),
        ])).unwrap();
    }

    // Insert categories
    let categories_table = db.get_table_mut("categories").unwrap();
    categories_table.insert(Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar("Beverages".to_string()),
        SqlValue::Varchar("Soft drinks, coffees, teas, beers, and ales".to_string()),
    ])).unwrap();
    categories_table.insert(Row::new(vec![
        SqlValue::Integer(2),
        SqlValue::Varchar("Condiments".to_string()),
        SqlValue::Varchar("Sweet and savory sauces".to_string()),
    ])).unwrap();

    db
}

fn format_result(result: executor::SelectResult) -> String {
    let mut output = String::from("-- EXPECTED:\n");

    // Header row
    output.push_str("-- | ");
    output.push_str(&result.columns.join(" | "));
    output.push_str(" |\n");

    // Data rows
    for row in &result.rows {
        output.push_str("-- | ");
        let values: Vec<String> = (0..result.columns.len())
            .map(|i| {
                match row.get(i) {
                    Some(val) => match val {
                        SqlValue::Integer(i) => i.to_string(),
                        SqlValue::Float(f) => f.to_string(),
                        SqlValue::Varchar(s) => s.clone(),
                        SqlValue::Null => "NULL".to_string(),
                        other => format!("{:?}", other), // Fallback for other types
                    },
                    None => "NULL".to_string(),
                }
            })
            .collect();
        output.push_str(&values.join(" | "));
        output.push_str(" |\n");
    }

    // Row count
    output.push_str(&format!("-- ({} rows)", result.rows.len()));

    output
}

fn main() {
    let db = create_full_northwind_db();
    let executor = SelectExecutor::new(&db);

    // Example query (replace with the query you want to test)
    let query = r#"SELECT DISTINCT category_id
FROM products
ORDER BY category_id;"#;

    println!("Running query:\n{}\n", query);

    match Parser::parse_sql(query) {
        Ok(ast::Statement::Select(select_stmt)) => {
            match executor.execute_with_columns(&select_stmt) {
                Ok(result) => {
                    println!("{}", format_result(result));
                }
                Err(e) => {
                    eprintln!("Error executing query: {:?}", e);
                }
            }
        }
        Ok(other) => {
            eprintln!("Expected SELECT statement, got: {:?}", other);
        }
        Err(e) => {
            eprintln!("Parse error: {:?}", e);
        }
    }
}
