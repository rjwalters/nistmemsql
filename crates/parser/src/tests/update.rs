use super::*;

// ========================================================================
// UPDATE Statement Tests
// ========================================================================

#[test]
fn test_parse_update_basic() {
    let result = Parser::parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 1);
            assert_eq!(update.assignments[0].column, "name");
            assert!(update.where_clause.is_some());
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_multiple_columns() {
    let result = Parser::parse_sql("UPDATE users SET name = 'Bob', age = 30;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 2);
            assert_eq!(update.assignments[0].column, "name");
            assert_eq!(update.assignments[1].column, "age");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}
