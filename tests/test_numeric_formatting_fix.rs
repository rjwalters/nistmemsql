//! Test for issue #1265: Fix numeric formatting in MySQL dialect mode
//!
//! Integer arithmetic should return integers, not decimals with .000

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

#[test]
fn test_integer_arithmetic_returns_integers() {
    let db = Database::new().with_sql_mode(vibesql_types::SqlMode::MySQL);

    // Test 1: Simple integer multiplication
    let sql = "SELECT - 61 * + ( + 62 )";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 1);
        // Result should be -3782 as Integer, not as Numeric/Float
        match &rows[0].values[0] {
            SqlValue::Integer(i) => {
                assert_eq!(*i, -3782, "Should get -3782 as integer");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    // Test 2: Negation and multiplication
    let sql = "SELECT ALL - - 59 * 3";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 1);
        match &rows[0].values[0] {
            SqlValue::Integer(i) => {
                assert_eq!(*i, 177, "Should get 177 as integer");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    // Test 3: Simple negation and multiplication
    let sql = "SELECT - 80 * + + 1";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 1);
        match &rows[0].values[0] {
            SqlValue::Integer(i) => {
                assert_eq!(*i, -80, "Should get -80 as integer");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    // Test 4: DIV operator should return integer
    let sql = "SELECT 90 DIV 1";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 1);
        match &rows[0].values[0] {
            SqlValue::Integer(i) => {
                assert_eq!(*i, 90, "DIV should return integer");
            }
            other => panic!("Expected Integer for DIV, got {:?}", other),
        }
    }
}

#[test]
fn test_integer_display_formatting() {
    // Test that Integer SqlValue displays without decimal places
    let val = SqlValue::Integer(-3782);
    assert_eq!(val.to_string(), "-3782");

    let val = SqlValue::Integer(177);
    assert_eq!(val.to_string(), "177");

    let val = SqlValue::Integer(-80);
    assert_eq!(val.to_string(), "-80");
}
