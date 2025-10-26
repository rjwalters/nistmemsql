use super::*;

// ========================================================================
// CREATE TABLE Statement Tests
// ========================================================================

#[test]
fn test_parse_create_table_basic() {
    let result = Parser::parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[1].name, "name");
            match create.columns[0].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer data type"),
            }
            match create.columns[1].data_type {
                types::DataType::Varchar { max_length: 100 } => {} // Success
                _ => panic!("Expected VARCHAR(100) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_various_types() {
    let result =
        Parser::parse_sql("CREATE TABLE test (id INT, flag BOOLEAN, birth DATE, code CHAR(5));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "test");
            assert_eq!(create.columns.len(), 4);
            match create.columns[0].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer"),
            }
            match create.columns[1].data_type {
                types::DataType::Boolean => {} // Success
                _ => panic!("Expected Boolean"),
            }
            match create.columns[2].data_type {
                types::DataType::Date => {} // Success
                _ => panic!("Expected Date"),
            }
            match create.columns[3].data_type {
                types::DataType::Character { length: 5 } => {} // Success
                _ => panic!("Expected CHAR(5)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
