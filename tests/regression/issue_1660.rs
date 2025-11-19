//! Test for issue #1660: Database.reset() must clear IndexManager state
//!
//! This test reproduces the bug where index-based queries return 0 rows
//! when database is reset between test runs.

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_reset_clears_index_state() {
    let mut db = Database::new();

    // ========== FIRST TEST RUN ==========
    eprintln!("\n========== FIRST TEST RUN ==========");

    // Create table
    let sql = "CREATE TABLE tab1 (pk INTEGER, col1 REAL)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    }

    // Create index BEFORE inserting rows
    let sql = "CREATE INDEX idx1 ON tab1(col1)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::CreateIndex(create_idx_stmt) = stmt {
        vibesql_executor::IndexExecutor::execute(&create_idx_stmt, &mut db).unwrap();
    }

    // Insert rows into tab1
    let sql = "INSERT INTO tab1 VALUES (1, 10.5)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let sql = "INSERT INTO tab1 VALUES (2, 20.5)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let sql = "INSERT INTO tab1 VALUES (3, 30.5)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    // Query using index (should return 3 rows)
    let sql = "SELECT pk FROM tab1 WHERE col1 > 5.0";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        eprintln!("First run: Found {} rows", rows.len());
        assert_eq!(rows.len(), 3, "First run should return 3 rows");
    }

    // ========== RESET DATABASE ==========
    eprintln!("\n========== RESET DATABASE ==========");
    db.reset();

    // ========== SECOND TEST RUN ==========
    eprintln!("\n========== SECOND TEST RUN ==========");

    // Create same table again
    let sql = "CREATE TABLE tab1 (pk INTEGER, col1 REAL)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    }

    // Create same index BEFORE inserting rows
    let sql = "CREATE INDEX idx1 ON tab1(col1)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::CreateIndex(create_idx_stmt) = stmt {
        vibesql_executor::IndexExecutor::execute(&create_idx_stmt, &mut db).unwrap();
    }

    // Insert rows into tab1 (fewer rows this time)
    let sql = "INSERT INTO tab1 VALUES (1, 15.5)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let sql = "INSERT INTO tab1 VALUES (2, 25.5)";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    // Query using index (should return 2 rows, NOT 0!)
    let sql = "SELECT pk FROM tab1 WHERE col1 > 5.0";
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        eprintln!("Second run: Found {} rows", rows.len());
        assert_eq!(rows.len(), 2, "Second run should return 2 rows, not 0!");
    }
}
