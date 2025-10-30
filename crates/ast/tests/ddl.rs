use ast::*;

// ============================================================================
// DDL Tests - CREATE TABLE
// ============================================================================

#[test]
fn test_create_table_statement() {
    let stmt = Statement::CreateTable(CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                constraints: vec![],
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: types::DataType::Varchar { max_length: Some(255) },
                nullable: true,
                constraints: vec![],
            },
        ],
        table_constraints: vec![],
    });

    match stmt {
        Statement::CreateTable(_) => {} // Success
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_column_def() {
    let col = ColumnDef {
        name: "email".to_string(),
        data_type: types::DataType::Varchar { max_length: Some(100) },
        nullable: false,
        constraints: vec![],
    };
    assert_eq!(col.name, "email");
    assert!(!col.nullable);
}
