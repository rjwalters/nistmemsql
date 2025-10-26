use super::*;

// ========================================================================
// INSERT Statement Tests
// ========================================================================

#[test]
fn test_parse_insert_basic() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0], "id");
            assert_eq!(insert.columns[1], "name");
            assert_eq!(insert.values.len(), 1); // One row
            assert_eq!(insert.values[0].len(), 2); // Two values
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_multiple_rows() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            assert_eq!(insert.values.len(), 2); // Two rows
        }
        _ => panic!("Expected INSERT statement"),
    }
}
