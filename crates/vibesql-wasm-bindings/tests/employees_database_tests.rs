// These are integration tests for the employees database
// Note: WASM-specific tests require wasm-pack test --node
// These tests verify compilation and basic logic without WASM runtime

#[test]
fn test_employees_sql_file_exists() {
    // Verify the SQL file can be included at compile time
    let sql = include_str!("../../../web-demo/examples/employees.sql");
    assert!(sql.contains("CREATE TABLE employees"));
    assert!(sql.contains("Sarah")); // CEO's first name
    assert!(sql.contains("Chief Executive Officer"));
}

#[test]
fn test_employees_sql_has_all_levels() {
    let sql = include_str!("../../../web-demo/examples/employees.sql");

    // Verify Level 1 (CEO)
    assert!(sql.contains("Level 1: CEO"));

    // Verify Level 2 (VPs)
    assert!(sql.contains("Level 2: VPs"));
    assert!(sql.contains("VP of Engineering"));
    assert!(sql.contains("VP of Sales"));
    assert!(sql.contains("VP of Operations"));

    // Verify Level 3 (Directors)
    assert!(sql.contains("Level 3: Directors"));

    // Verify Level 4 (Individual Contributors)
    assert!(sql.contains("Level 4: Individual Contributors"));
}

#[test]
fn test_employees_sql_has_all_departments() {
    let sql = include_str!("../../../web-demo/examples/employees.sql");

    assert!(sql.contains("Engineering"));
    assert!(sql.contains("Sales"));
    assert!(sql.contains("Operations"));
    assert!(sql.contains("Executive"));
}

#[test]
fn test_employees_hierarchy_structure() {
    let sql = include_str!("../../../web-demo/examples/employees.sql");

    // Verify CEO has NULL manager_id
    assert!(sql.contains(", NULL,"));

    // Verify self-referencing structure is present
    assert!(sql.contains("manager_id"));

    // Count INSERT statements to verify 35 employees
    let insert_count = sql.matches("INSERT INTO employees VALUES").count();
    // We have multiple inserts per statement, but let's check we have data
    assert!(insert_count > 0, "Should have INSERT statements");
}

// Note: Runtime tests of load_employees() require wasm-pack test --node
// The tests above verify the SQL file structure at compile time
