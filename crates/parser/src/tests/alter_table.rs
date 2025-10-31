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
                    assert_eq!(add.table_name, "USERS");
                    assert_eq!(add.column_def.name, "EMAIL");
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
                assert_eq!(drop.table_name, "USERS");
                assert_eq!(drop.column_name, "EMAIL");
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
                assert_eq!(drop.table_name, "USERS");
                assert_eq!(drop.column_name, "EMAIL");
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
                    assert_eq!(table_name, "USERS");
                    assert_eq!(column_name, "EMAIL");
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
                    assert_eq!(table_name, "USERS");
                    assert_eq!(column_name, "EMAIL");
                }
                _ => panic!("Expected DROP NOT NULL"),
            },
            _ => panic!("Expected ALTER COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

// ========================================================================
// ALTER TABLE ADD Constraint (without CONSTRAINT keyword) Tests
// SQL:1999 Feature F031-04
// ========================================================================

#[test]
fn test_alter_table_add_check_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD CHECK (x > 0);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "T");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match add.constraint.kind {
                    ast::TableConstraintKind::Check { .. } => {} // Success
                    _ => panic!("Expected CHECK constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_unique_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD UNIQUE (col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "T");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    ast::TableConstraintKind::Unique { columns } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0], "COL");
                    }
                    _ => panic!("Expected UNIQUE constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_primary_key_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD PRIMARY KEY (col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "T");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    ast::TableConstraintKind::PrimaryKey { columns } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0], "COL");
                    }
                    _ => panic!("Expected PRIMARY KEY constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_foreign_key_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD FOREIGN KEY (col) REFERENCES other(other_col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "T");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    ast::TableConstraintKind::ForeignKey {
                        columns,
                        references_table,
                        references_columns,
                    } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0], "COL");
                        assert_eq!(references_table, "OTHER");
                        assert_eq!(references_columns.len(), 1);
                        assert_eq!(references_columns[0], "OTHER_COL");
                    }
                    _ => panic!("Expected FOREIGN KEY constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_named_check_with_keyword() {
    // Ensure backward compatibility - named constraints with CONSTRAINT keyword still work
    let result = Parser::parse_sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (x > 0);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::AlterTable(alter) => match alter {
            ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "T");
                assert_eq!(add.constraint.name, Some("CK".to_string()));
                match add.constraint.kind {
                    ast::TableConstraintKind::Check { .. } => {} // Success
                    _ => panic!("Expected CHECK constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}
