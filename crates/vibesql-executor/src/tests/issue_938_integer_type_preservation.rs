//! Test for issue #938 - Integer type preservation in unary operations
//!
//! Ensures that integer expressions with unary operators return Integer types,
//! not Numeric or Float types.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::SelectExecutor;

#[test]
fn test_integer_unary_operations_from_table() {
    let mut db = Database::new();

    // Setup table
    let create_sql = "CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let insert_sql = "INSERT INTO tab1 VALUES(51,14,96)";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Test the problematic query from issue #938
    // SELECT ALL + ( + - ( - 92 ) ) FROM tab1
    // Should return Integer(92), not Integer(92) or Float(92.0)
    let select_sql = "SELECT + ( + - ( - 92 ) ) FROM tab1";
    let select_stmt = Parser::parse_sql(select_sql).unwrap();
    if let vibesql_ast::Statement::Select(stmt) = select_stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&stmt).unwrap();

        assert_eq!(rows.len(), 1, "Expected 1 row");
        assert_eq!(rows[0].values.len(), 1, "Expected 1 column");

        // The result MUST be Integer(92), not any float type
        match &rows[0].values[0] {
            SqlValue::Integer(92) => {} // Success!
            SqlValue::Numeric(f) if *f == 92.0 => {
                panic!("BUG: Integer expression returned Numeric({}). Expected Integer(92).", f);
            }
            SqlValue::Float(f) if *f == 92.0 => {
                panic!("BUG: Integer expression returned Float({}). Expected Integer(92).", f);
            }
            other => {
                panic!("Expected Integer(92), got {:?}", other);
            }
        }
    }
}

#[test]
fn test_integer_subtraction_unary() {
    let mut db = Database::new();

    // Setup table
    let create_sql = "CREATE TABLE tab0(col0 INTEGER)";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let insert_sql = "INSERT INTO tab0 VALUES(1)";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Test: SELECT ALL - - ( - 56 ) FROM tab0
    // Should return Integer(-56), not Numeric(-56.0)
    let select_sql = "SELECT - - ( - 56 ) FROM tab0";
    let select_stmt = Parser::parse_sql(select_sql).unwrap();
    if let vibesql_ast::Statement::Select(stmt) = select_stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&stmt).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.len(), 1);

        match &rows[0].values[0] {
            SqlValue::Integer(-56) => {} // Success!
            other => {
                panic!("Expected Integer(-56), got {:?}", other);
            }
        }
    }
}

#[test]
fn test_simple_integer_literal() {
    let mut db = Database::new();

    // Setup table
    let create_sql = "CREATE TABLE t(x INTEGER)";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let insert_sql = "INSERT INTO t VALUES(1)";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Even a simple integer literal should remain an integer
    let select_sql = "SELECT 42 FROM t";
    let select_stmt = Parser::parse_sql(select_sql).unwrap();
    if let vibesql_ast::Statement::Select(stmt) = select_stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&stmt).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.len(), 1);

        match &rows[0].values[0] {
            SqlValue::Integer(42) => {} // Success!
            other => {
                panic!("Expected Integer(42), got {:?}", other);
            }
        }
    }
}
