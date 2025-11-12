//! Tests for TRUNCATE TABLE executor

use crate::{CreateTableExecutor, TruncateTableExecutor};
use vibesql_ast::{ColumnDef, CreateTableStmt, TruncateTableStmt};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_truncate_single_table() {
    let mut db = Database::new();

    // Create a table
    let create_stmt = CreateTableStmt {
        table_name: "USERS".to_string(),
        columns: vec![
            ColumnDef {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: 100 },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert some rows
    db.insert_row("USERS", Row::new(vec![SqlValue::Integer(1), SqlValue::VarChar("Alice".to_string())]))
        .unwrap();
    db.insert_row("USERS", Row::new(vec![SqlValue::Integer(2), SqlValue::VarChar("Bob".to_string())]))
        .unwrap();
    db.insert_row("USERS", Row::new(vec![SqlValue::Integer(3), SqlValue::VarChar("Carol".to_string())]))
        .unwrap();

    assert_eq!(db.get_table("USERS").unwrap().row_count(), 3);

    // Truncate the table
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["USERS".to_string()],
        if_exists: false,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 3);
    assert_eq!(db.get_table("USERS").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_multiple_tables() {
    let mut db = Database::new();

    // Create first table
    let create_stmt1 = CreateTableStmt {
        table_name: "ORDERS".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt1, &mut db).unwrap();

    // Create second table
    let create_stmt2 = CreateTableStmt {
        table_name: "ORDER_ITEMS".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt2, &mut db).unwrap();

    // Create third table
    let create_stmt3 = CreateTableStmt {
        table_name: "ORDER_HISTORY".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt3, &mut db).unwrap();

    // Insert rows into each table
    db.insert_row("ORDERS", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("ORDERS", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(10)])).unwrap();
    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(20)])).unwrap();
    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(30)])).unwrap();

    db.insert_row("ORDER_HISTORY", Row::new(vec![SqlValue::Integer(100)])).unwrap();

    assert_eq!(db.get_table("ORDERS").unwrap().row_count(), 2);
    assert_eq!(db.get_table("ORDER_ITEMS").unwrap().row_count(), 3);
    assert_eq!(db.get_table("ORDER_HISTORY").unwrap().row_count(), 1);

    // Truncate all three tables
    let truncate_stmt = TruncateTableStmt {
        table_names: vec![
            "ORDERS".to_string(),
            "ORDER_ITEMS".to_string(),
            "ORDER_HISTORY".to_string(),
        ],
        if_exists: false,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 6); // 2 + 3 + 1
    assert_eq!(db.get_table("ORDERS").unwrap().row_count(), 0);
    assert_eq!(db.get_table("ORDER_ITEMS").unwrap().row_count(), 0);
    assert_eq!(db.get_table("ORDER_HISTORY").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_if_exists_table_not_exists() {
    let mut db = Database::new();

    // Try to truncate a non-existent table with IF EXISTS
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["NONEXISTENT".to_string()],
        if_exists: true,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 0);
}

#[test]
fn test_truncate_without_if_exists_table_not_exists() {
    let mut db = Database::new();

    // Try to truncate a non-existent table without IF EXISTS - should error
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["NONEXISTENT".to_string()],
        if_exists: false,
    };

    let result = TruncateTableExecutor::execute(&truncate_stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_truncate_multiple_tables_if_exists_mixed() {
    let mut db = Database::new();

    // Create one table
    let create_stmt = CreateTableStmt {
        table_name: "EXISTING".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert rows
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // Truncate one existing and one non-existing table with IF EXISTS
    let truncate_stmt = TruncateTableStmt {
        table_names: vec![
            "EXISTING".to_string(),
            "NONEXISTENT1".to_string(),
            "NONEXISTENT2".to_string(),
        ],
        if_exists: true,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 2); // Only from EXISTING
    assert_eq!(db.get_table("EXISTING").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_multiple_tables_all_or_nothing_validation() {
    let mut db = Database::new();

    // Create one table
    let create_stmt = CreateTableStmt {
        table_name: "EXISTING".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert rows
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Try to truncate one existing and one non-existing without IF EXISTS
    // Should fail and not truncate anything
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["EXISTING".to_string(), "NONEXISTENT".to_string()],
        if_exists: false,
    };

    let result = TruncateTableExecutor::execute(&truncate_stmt, &mut db);
    assert!(result.is_err());

    // Verify the existing table was NOT truncated (all-or-nothing)
    assert_eq!(db.get_table("EXISTING").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_empty_table() {
    let mut db = Database::new();

    // Create a table
    let create_stmt = CreateTableStmt {
        table_name: "EMPTY".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Truncate empty table
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["EMPTY".to_string()],
        if_exists: false,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 0);
    assert_eq!(db.get_table("EMPTY").unwrap().row_count(), 0);
}
