use vibesql_catalog::{ColumnSchema, TableSchema, TriggerDefinition};
use vibesql_storage::Database;
use vibesql_types::DataType;

/// Test that basic triggers persist across save/load cycles
#[test]
fn test_basic_trigger_persistence() {
    let temp_file = "/tmp/test_trigger_basic.vbsql";
    let _ = std::fs::remove_file(temp_file);

    // Create database with table and trigger
    let mut db = Database::new();

    let items_schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(items_schema).unwrap();

    let audit_schema = TableSchema::new(
        "audit_log".to_string(),
        vec![ColumnSchema::new(
            "msg".to_string(),
            DataType::Varchar { max_length: Some(100) },
            false,
        )],
    );
    db.create_table(audit_schema).unwrap();

    // Create trigger
    let trigger = TriggerDefinition::new(
        "log_insert".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None, // no WHEN clause
        vibesql_ast::TriggerAction::RawSql("INSERT INTO audit_log VALUES ('Item inserted')".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Verify trigger exists before save
    let triggers = db.catalog.list_triggers();
    assert_eq!(triggers.len(), 1);
    assert!(triggers.contains(&"log_insert".to_string()));

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify trigger exists after load
    let loaded_triggers = loaded_db.catalog.list_triggers();
    assert_eq!(loaded_triggers.len(), 1);
    assert!(loaded_triggers.contains(&"log_insert".to_string()));

    // Verify trigger details
    let loaded_trigger = loaded_db.catalog.get_trigger("log_insert").unwrap();
    assert_eq!(loaded_trigger.name, "log_insert");
    assert_eq!(loaded_trigger.table_name, "items");
    assert_eq!(loaded_trigger.timing, vibesql_ast::TriggerTiming::After);
    assert_eq!(loaded_trigger.event, vibesql_ast::TriggerEvent::Insert);
    assert_eq!(loaded_trigger.granularity, vibesql_ast::TriggerGranularity::Row);
    assert!(loaded_trigger.when_condition.is_none());

    std::fs::remove_file(temp_file).unwrap();
}

/// Test that multiple triggers persist correctly
#[test]
fn test_multiple_triggers_persist() {
    let temp_file = "/tmp/test_trigger_multiple.vbsql";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let log_schema = TableSchema::new(
        "log".to_string(),
        vec![ColumnSchema::new(
            "event".to_string(),
            DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(log_schema).unwrap();

    // Create multiple triggers
    let triggers = vec![
        TriggerDefinition::new(
            "t1".to_string(),
            vibesql_ast::TriggerTiming::Before,
            vibesql_ast::TriggerEvent::Insert,
            "items".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("INSERT INTO log VALUES ('before')".to_string()),
        ),
        TriggerDefinition::new(
            "t2".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "items".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("INSERT INTO log VALUES ('after')".to_string()),
        ),
        TriggerDefinition::new(
            "t3".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Update(None),
            "items".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("INSERT INTO log VALUES ('update')".to_string()),
        ),
    ];

    for trigger in triggers {
        db.catalog.create_trigger(trigger).unwrap();
    }

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify all triggers exist
    let loaded_triggers = loaded_db.catalog.list_triggers();
    assert_eq!(loaded_triggers.len(), 3);
    assert!(loaded_triggers.contains(&"t1".to_string()));
    assert!(loaded_triggers.contains(&"t2".to_string()));
    assert!(loaded_triggers.contains(&"t3".to_string()));

    // Verify each trigger's timing
    let t1 = loaded_db.catalog.get_trigger("t1").unwrap();
    assert_eq!(t1.timing, vibesql_ast::TriggerTiming::Before);

    let t2 = loaded_db.catalog.get_trigger("t2").unwrap();
    assert_eq!(t2.timing, vibesql_ast::TriggerTiming::After);

    let t3 = loaded_db.catalog.get_trigger("t3").unwrap();
    assert_eq!(t3.event, vibesql_ast::TriggerEvent::Update(None));

    std::fs::remove_file(temp_file).unwrap();
}

/// Test that dropped triggers stay dropped after reload
#[test]
fn test_dropped_trigger_stays_dropped() {
    let temp_file = "/tmp/test_trigger_dropped.vbsql";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "items".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create two triggers
    let t1 = TriggerDefinition::new(
        "t1".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
    );
    let t2 = TriggerDefinition::new(
        "t2".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 2".to_string()),
    );

    db.catalog.create_trigger(t1).unwrap();
    db.catalog.create_trigger(t2).unwrap();

    // Drop t1
    db.catalog.drop_trigger("t1").unwrap();

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify only t2 exists
    let loaded_triggers = loaded_db.catalog.list_triggers();
    assert_eq!(loaded_triggers.len(), 1);
    assert!(loaded_triggers.contains(&"t2".to_string()));
    assert!(!loaded_triggers.contains(&"t1".to_string()));

    std::fs::remove_file(temp_file).unwrap();
}

/// Test triggers with UPDATE OF specific columns
#[test]
fn test_trigger_update_of_columns() {
    let temp_file = "/tmp/test_trigger_update_of.vbsql";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Real, false),
            ColumnSchema::new("stock".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create trigger for UPDATE OF price
    let trigger = TriggerDefinition::new(
        "log_price_change".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Update(Some(vec!["price".to_string()])),
        "products".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify trigger with UPDATE OF columns
    let loaded_trigger = loaded_db.catalog.get_trigger("log_price_change").unwrap();
    match &loaded_trigger.event {
        vibesql_ast::TriggerEvent::Update(Some(cols)) => {
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0], "price");
        }
        _ => panic!("Expected UPDATE OF event with columns"),
    }

    std::fs::remove_file(temp_file).unwrap();
}

/// Test INSTEAD OF triggers
#[test]
fn test_instead_of_trigger() {
    let temp_file = "/tmp/test_trigger_instead_of.vbsql";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "items".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create INSTEAD OF trigger
    let trigger = TriggerDefinition::new(
        "instead_trigger".to_string(),
        vibesql_ast::TriggerTiming::InsteadOf,
        vibesql_ast::TriggerEvent::Delete,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 'blocked'".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify INSTEAD OF timing
    let loaded_trigger = loaded_db.catalog.get_trigger("instead_trigger").unwrap();
    assert_eq!(loaded_trigger.timing, vibesql_ast::TriggerTiming::InsteadOf);
    assert_eq!(loaded_trigger.event, vibesql_ast::TriggerEvent::Delete);

    std::fs::remove_file(temp_file).unwrap();
}

/// Test statement-level triggers
#[test]
fn test_statement_level_trigger() {
    let temp_file = "/tmp/test_trigger_statement.vbsql";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "items".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create statement-level trigger
    let trigger = TriggerDefinition::new(
        "stmt_trigger".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Statement,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let loaded_db = Database::load_binary(temp_file).unwrap();

    // Verify statement-level granularity
    let loaded_trigger = loaded_db.catalog.get_trigger("stmt_trigger").unwrap();
    assert_eq!(loaded_trigger.granularity, vibesql_ast::TriggerGranularity::Statement);

    std::fs::remove_file(temp_file).unwrap();
}

/// Test compressed format persistence
#[test]
fn test_trigger_persistence_compressed() {
    let temp_file = "/tmp/test_trigger_compressed.vbsqlz";
    let _ = std::fs::remove_file(temp_file);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "items".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create trigger
    let trigger = TriggerDefinition::new(
        "test_trigger".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Save and reload using compressed format
    db.save_compressed(temp_file).unwrap();
    let loaded_db = Database::load_compressed(temp_file).unwrap();

    // Verify trigger persists in compressed format
    let loaded_triggers = loaded_db.catalog.list_triggers();
    assert_eq!(loaded_triggers.len(), 1);
    assert!(loaded_triggers.contains(&"test_trigger".to_string()));

    std::fs::remove_file(temp_file).unwrap();
}

/// Test multiple save/load cycles
#[test]
fn test_multiple_save_load_cycles() {
    let temp_file1 = "/tmp/test_trigger_cycle1.vbsql";
    let temp_file2 = "/tmp/test_trigger_cycle2.vbsql";
    let _ = std::fs::remove_file(temp_file1);
    let _ = std::fs::remove_file(temp_file2);

    let mut db = Database::new();

    let schema = TableSchema::new(
        "items".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create trigger
    let trigger = TriggerDefinition::new(
        "persistent_trigger".to_string(),
        vibesql_ast::TriggerTiming::After,
        vibesql_ast::TriggerEvent::Insert,
        "items".to_string(),
        vibesql_ast::TriggerGranularity::Row,
        None,
        vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // First cycle: save and load
    db.save_binary(temp_file1).unwrap();
    let db2 = Database::load_binary(temp_file1).unwrap();

    // Verify trigger exists after first load
    assert_eq!(db2.catalog.list_triggers().len(), 1);

    // Second cycle: save loaded database and load again
    db2.save_binary(temp_file2).unwrap();
    let db3 = Database::load_binary(temp_file2).unwrap();

    // Verify trigger exists after second load
    assert_eq!(db3.catalog.list_triggers().len(), 1);
    let final_trigger = db3.catalog.get_trigger("persistent_trigger").unwrap();
    assert_eq!(final_trigger.name, "persistent_trigger");

    std::fs::remove_file(temp_file1).unwrap();
    std::fs::remove_file(temp_file2).unwrap();
}
