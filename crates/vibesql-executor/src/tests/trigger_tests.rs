//! Tests for TRIGGER execution

use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_ast::{CreateTriggerStmt, DropTriggerStmt};
use vibesql_storage::Database;

#[test]
fn test_create_trigger() {
    let mut db = Database::new();

    // Create a table first (since trigger references it)
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Verify trigger was created
    let trigger = db.catalog.get_trigger("my_trigger");
    assert!(trigger.is_some(), "Trigger not found after creation");
    let trigger = trigger.unwrap();
    assert_eq!(trigger.name, "my_trigger");
    assert_eq!(trigger.timing, TriggerTiming::Before);
    assert_eq!(trigger.event, TriggerEvent::Insert);
}

#[test]
fn test_create_trigger_duplicate_error() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Try to create another trigger with the same name
    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_err(), "Should fail when creating duplicate trigger");
}

#[test]
fn test_drop_trigger() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    crate::advanced_objects::execute_create_trigger(&stmt, &mut db).unwrap();

    // Verify it was created
    assert!(db.catalog.get_trigger("my_trigger").is_some());

    // Drop the trigger
    let drop_stmt = DropTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        cascade: false,
    };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_ok(), "Failed to drop trigger: {:?}", result.err());

    // Verify it was dropped
    assert!(db.catalog.get_trigger("my_trigger").is_none(), "Trigger still exists after drop");
}

#[test]
fn test_drop_trigger_not_found() {
    let mut db = Database::new();

    let drop_stmt = DropTriggerStmt {
        trigger_name: "nonexistent_trigger".to_string(),
        cascade: false,
    };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_err(), "Should fail when dropping non-existent trigger");
}

#[test]
fn test_create_trigger_all_variations() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Test different timing values
    let timings = vec![
        (TriggerTiming::Before, "before"),
        (TriggerTiming::After, "after"),
        (TriggerTiming::InsteadOf, "insteadof"),
    ];

    for (timing, suffix) in timings {
        let stmt = CreateTriggerStmt {
            trigger_name: format!("trigger_{}", suffix),
            timing: timing.clone(),
            event: TriggerEvent::Insert,
            table_name: "test_table".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
        };

        let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
        assert!(result.is_ok(), "Failed to create {} trigger: {:?}", suffix, result.err());

        let trigger = db.catalog.get_trigger(&format!("trigger_{}", suffix)).unwrap();
        assert_eq!(trigger.timing, timing);
    }
}

// ============================================================================
// Integration tests for trigger firing during DML operations
// ============================================================================

use crate::{InsertExecutor, SelectExecutor, UpdateExecutor, DeleteExecutor, CreateTableExecutor};

/// Helper to create audit log table
fn create_audit_table(db: &mut Database) {
    let stmt = vibesql_ast::CreateTableStmt {
        table_name: "AUDIT_LOG".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "event".to_string(),
            data_type: vibesql_types::DataType::Varchar { max_length: Some(255) },
            nullable: true,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt, db).expect("Failed to create audit_log table");
}

/// Helper to create users table
fn create_users_table(db: &mut Database) {
    let stmt = vibesql_ast::CreateTableStmt {
        table_name: "USERS".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            vibesql_ast::ColumnDef {
                name: "username".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt, db).expect("Failed to create users table");
}

/// Helper to count rows in audit log
fn count_audit_rows(db: &Database) -> usize {
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,        from: Some(vibesql_ast::FromClause::Table {
            name: "AUDIT_LOG".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let executor = SelectExecutor::new(db);
    let result = executor.execute(&select).expect("Failed to select from audit_log");
    result.len()
}

#[test]
fn test_after_insert_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create AFTER INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_insert".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('User inserted')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired by checking audit log
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_after_insert_trigger_fires_for_each_row() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create AFTER INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_insert".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Insert')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert 3 rows - should fire trigger 3 times
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("bob".to_string())),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("charlie".to_string())),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired 3 times (once per row)
    assert_eq!(count_audit_rows(&db), 3);
}

#[test]
fn test_before_insert_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create BEFORE INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_before_insert".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Before insert')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_after_update_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Create AFTER UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_update".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None),
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('User updated')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Update the user - should fire trigger
    let update = vibesql_ast::UpdateStmt {
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice_updated".to_string())),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "id".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        })),
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_before_update_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Create BEFORE UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_before_update".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Update(None),
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Before update')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Update the user - should fire trigger
    let update = vibesql_ast::UpdateStmt {
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice_updated".to_string())),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "id".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        })),
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_after_delete_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Create AFTER DELETE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_delete".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Delete,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('User deleted')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Delete the user - should fire trigger
    let delete = vibesql_ast::DeleteStmt {
        only: false,
        table_name: "USERS".to_string(),
        where_clause: Some(vibesql_ast::WhereClause::Condition(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "id".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        })),
    };
    DeleteExecutor::execute(&delete, &mut db).expect("Failed to delete");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_before_delete_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Create BEFORE DELETE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_before_delete".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Delete,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Before delete')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Delete the user - should fire trigger
    let delete = vibesql_ast::DeleteStmt {
        only: false,
        table_name: "USERS".to_string(),
        where_clause: Some(vibesql_ast::WhereClause::Condition(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "id".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        })),
    };
    DeleteExecutor::execute(&delete, &mut db).expect("Failed to delete");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_multiple_triggers_fire_in_order() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create first AFTER INSERT trigger
    let trigger1 = CreateTriggerStmt {
        trigger_name: "log_insert_1".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('First trigger')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger1, &mut db)
        .expect("Failed to create trigger 1");

    // Create second AFTER INSERT trigger
    let trigger2 = CreateTriggerStmt {
        trigger_name: "log_insert_2".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Second trigger')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger2, &mut db)
        .expect("Failed to create trigger 2");

    // Insert a row - should fire both triggers
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify both triggers fired
    assert_eq!(count_audit_rows(&db), 2);
}

#[test]
fn test_trigger_with_multiple_statements() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create trigger with multiple statements (separated by semicolons)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_multiple".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "BEGIN INSERT INTO audit_log (event) VALUES ('First'); INSERT INTO audit_log (event) VALUES ('Second') END"
                .to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger with both statements
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify both statements executed
    assert_eq!(count_audit_rows(&db), 2);
}

// ============================================================================
// Additional test cases for Phase 3 completion
// ============================================================================

#[test]
fn test_when_clause_filters_firing() {
    let mut db = Database::new();

    // Create table with amount column
    let table_stmt = vibesql_ast::CreateTableStmt {
        table_name: "TRANSACTIONS".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            vibesql_ast::ColumnDef {
                name: "amount".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&table_stmt, &mut db).expect("Failed to create transactions table");
    create_audit_table(&mut db);

    // Create trigger with WHEN (amount > 100) condition
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_high_amount".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "TRANSACTIONS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: Some(Box::new(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "amount".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100))),
        })),
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('High amount')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert row with amount=50 (should NOT fire)
    let insert1 = vibesql_ast::InsertStmt {
        table_name: "TRANSACTIONS".to_string(),
        columns: vec!["id".to_string(), "amount".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(50)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert1).expect("Failed to insert");

    // Verify trigger did NOT fire
    assert_eq!(count_audit_rows(&db), 0);

    // Insert row with amount=150 (should fire)
    let insert2 = vibesql_ast::InsertStmt {
        table_name: "TRANSACTIONS".to_string(),
        columns: vec!["id".to_string(), "amount".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(150)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert2).expect("Failed to insert");

    // Verify trigger fired only once (for amount=150)
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_trigger_failure_causes_rollback() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create trigger that always fails (inserts into non-existent table)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "failing_trigger".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO nonexistent_table (col) VALUES (1)".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Try to insert - should fail due to trigger error
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert);

    // Verify insert failed
    assert!(result.is_err(), "Insert should have failed due to trigger error");

    // Verify row was NOT inserted (rollback occurred)
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "USERS".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let executor = SelectExecutor::new(&db);
    let rows = executor.execute(&select).expect("Failed to select");

    // Table should be empty (no rows inserted)
    assert_eq!(rows.len(), 0, "Table should be empty after failed trigger");
}

#[test]
fn test_recursion_prevention() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create trigger that inserts into the same table (infinite loop)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "recursive_trigger".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO users (id, username) VALUES (999, 'recursive')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Try to insert - should fail with recursion depth error
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert);

    // Verify insert failed with recursion error
    assert!(result.is_err(), "Insert should have failed due to recursion limit");
    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(err_msg.contains("recursion") || err_msg.contains("depth"),
            "Error should mention recursion or depth: {}", err_msg);
}

#[test]
fn test_before_trigger_executes_first() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create counter table to track execution order
    let counter_stmt = vibesql_ast::CreateTableStmt {
        table_name: "COUNTER".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "value".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&counter_stmt, &mut db).expect("Failed to create counter table");

    // Initialize counter to 0
    let init_insert = vibesql_ast::InsertStmt {
        table_name: "COUNTER".to_string(),
        columns: vec!["value".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &init_insert).expect("Failed to initialize counter");

    // Create BEFORE INSERT trigger that increments counter
    let before_trigger = CreateTriggerStmt {
        trigger_name: "before_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "UPDATE counter SET value = value + 1".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&before_trigger, &mut db)
        .expect("Failed to create before trigger");

    // Insert a row
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("alice".to_string())),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify counter was incremented (BEFORE trigger executed)
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                column: "value".to_string(),
                table: None,
            },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "COUNTER".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select).expect("Failed to select counter");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));

    // Verify user was actually inserted (main operation completed)
    let user_select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "USERS".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let user_result = executor.execute(&user_select).expect("Failed to select users");
    assert_eq!(user_result.len(), 1, "User should be inserted after BEFORE trigger");
}

// ===== Phase 4: OLD/NEW Pseudo-Variable Tests =====

#[test]
fn test_new_in_insert_trigger() {
    // Test that NEW pseudo-variable works in INSERT triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (msg VARCHAR(200));";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses NEW to log inserted employee
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_new_employee".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (msg) VALUES (NEW.name);".to_string()
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Insert a row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice', 50000);";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Verify trigger logged the name from NEW
    let select_sql = "SELECT msg FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = crate::SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_old_and_new_in_update_trigger() {
    // Test that both OLD and NEW pseudo-variables work in UPDATE triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Insert initial row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice', 50000);";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (old_salary INT, new_salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses both OLD and NEW
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_salary_change".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None), // No specific column list
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (old_salary, new_salary) VALUES (OLD.salary, NEW.salary);".to_string()
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Update salary
    let update_sql = "UPDATE employees SET salary = 55000 WHERE id = 1;";
    let stmt = vibesql_parser::Parser::parse_sql(update_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Update(stmt) => {
            crate::UpdateExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected Update"),
    }

    // Verify trigger logged both OLD and NEW salaries
    let select_sql = "SELECT old_salary, new_salary FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = crate::SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(50000)); // OLD.salary
    assert_eq!(result.rows[0].values[1], vibesql_types::SqlValue::Integer(55000)); // NEW.salary
}

#[test]
fn test_old_in_delete_trigger() {
    // Test that OLD pseudo-variable works in DELETE triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50));";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Insert row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice');";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            crate::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (deleted_name VARCHAR(50));";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses OLD
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_deletion".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Delete,
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (deleted_name) VALUES (OLD.name);".to_string()
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Delete row
    let delete_sql = "DELETE FROM employees WHERE id = 1;";
    let stmt = vibesql_parser::Parser::parse_sql(delete_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Delete(stmt) => {
            crate::DeleteExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected Delete"),
    }

    // Verify trigger logged the deleted name from OLD
    let select_sql = "SELECT deleted_name FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = crate::SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Varchar("Alice".to_string()));
}
