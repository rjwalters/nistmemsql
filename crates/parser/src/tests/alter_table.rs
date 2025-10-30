use super::*;

// ========================================================================
// ALTER TABLE Statement Tests
// ========================================================================

#[test]
fn test_parse_alter_table_add_column() {
    let result = Parser::parse_sql("ALTER TABLE users ADD COLUMN email VARCHAR(100);");
    if let Err(ref e) = result {
        println!("Parse error: {:?}", e);
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => {
            match alter {
                ast::AlterTableStmt::AddColumn(add) => {
                    assert_eq!(add.table_name, "users");
                    assert_eq!(add.column_def.name, "email");
                    match add.column_def.data_type {
                        types::DataType::Varchar { max_length: Some(100) } => {} // Success
                        _ => panic!("Expected VARCHAR(100) data type"),
                    }
                    assert!(add.column_def.nullable); // NULL by default
                    assert!(add.column_def.constraints.is_empty());
                }
                _ => panic!("Expected ADD COLUMN"),
            }
        }
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_drop_column() {
    let result = Parser::parse_sql("ALTER TABLE users DROP COLUMN email;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::DropColumn(drop) => {
                assert_eq!(drop.table_name, "users");
                assert_eq!(drop.column_name, "email");
                assert!(!drop.if_exists);
            }
            _ => panic!("Expected DROP COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_drop_column_if_exists() {
    let result = Parser::parse_sql("ALTER TABLE users DROP COLUMN IF EXISTS email;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::DropColumn(drop) => {
                assert_eq!(drop.table_name, "users");
                assert_eq!(drop.column_name, "email");
                assert!(drop.if_exists);
            }
            _ => panic!("Expected DROP COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_alter_column_set_not_null() {
    let result = Parser::parse_sql("ALTER TABLE users ALTER COLUMN email SET NOT NULL;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AlterColumn(alter_col) => match alter_col {
                ast::AlterColumnStmt::SetNotNull { table_name, column_name } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(column_name, "email");
                }
                _ => panic!("Expected SET NOT NULL"),
            },
            _ => panic!("Expected ALTER COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_alter_column_drop_not_null() {
    let result = Parser::parse_sql("ALTER TABLE users ALTER COLUMN email DROP NOT NULL;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AlterColumn(alter_col) => match alter_col {
                ast::AlterColumnStmt::DropNotNull { table_name, column_name } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(column_name, "email");
                }
                _ => panic!("Expected DROP NOT NULL"),
            },
            _ => panic!("Expected ALTER COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}
