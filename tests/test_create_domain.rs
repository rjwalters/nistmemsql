//! Tests for CREATE DOMAIN and DROP DOMAIN statements

use ast::Statement;
use executor::DomainExecutor;
use parser::Parser;
use storage::Database;

#[test]
fn test_create_domain_basic() {
    let mut db = Database::new();

    // Create a simple domain with just a base type
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain: {:?}", result);

        // Verify domain was created
        assert!(db.catalog.domain_exists("POSITIVEINT"));
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_with_default() {
    let mut db = Database::new();

    // Create domain with DEFAULT value
    let sql = "CREATE DOMAIN Age AS INTEGER DEFAULT 0";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with DEFAULT: {:?}", result);

        let domain = db.catalog.get_domain("AGE");
        assert!(domain.is_some());
        assert!(domain.unwrap().default.is_some());
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_with_check_constraint() {
    let mut db = Database::new();

    // Create domain with CHECK constraint
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER CHECK (VALUE > 0)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with CHECK: {:?}", result);

        let domain = db.catalog.get_domain("POSITIVEINT");
        assert!(domain.is_some());
        assert_eq!(domain.unwrap().constraints.len(), 1);
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_with_default_and_check() {
    let mut db = Database::new();

    // Create domain with both DEFAULT and CHECK
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER DEFAULT 1 CHECK (VALUE > 0)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with DEFAULT and CHECK: {:?}", result);

        let domain = db.catalog.get_domain("POSITIVEINT");
        assert!(domain.is_some());
        let domain = domain.unwrap();
        assert!(domain.default.is_some());
        assert_eq!(domain.constraints.len(), 1);
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_varchar() {
    let mut db = Database::new();

    // Create domain with VARCHAR type
    let sql = "CREATE DOMAIN Email AS VARCHAR(255)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create VARCHAR domain: {:?}", result);

        assert!(db.catalog.domain_exists("EMAIL"));
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_multiple_check_constraints() {
    let mut db = Database::new();

    // Create domain with multiple CHECK constraints
    let sql = "CREATE DOMAIN RangeInt AS INTEGER CHECK (VALUE >= 0) CHECK (VALUE <= 100)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with multiple CHECKs: {:?}", result);

        let domain = db.catalog.get_domain("RANGEINT");
        assert!(domain.is_some());
        assert_eq!(domain.unwrap().constraints.len(), 2);
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_create_domain_duplicate_error() {
    let mut db = Database::new();

    // Create domain
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        DomainExecutor::execute_create_domain(&create_stmt, &mut db).unwrap();
    }

    // Try to create the same domain again - should fail
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail when creating duplicate domain");
    }
}

#[test]
fn test_drop_domain_basic() {
    let mut db = Database::new();

    // Create domain
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        DomainExecutor::execute_create_domain(&create_stmt, &mut db).unwrap();
        assert!(db.catalog.domain_exists("POSITIVEINT"));
    }

    // Drop domain
    let sql = "DROP DOMAIN PositiveInt";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropDomain(drop_stmt) = stmt {
        let result = DomainExecutor::execute_drop_domain(&drop_stmt, &mut db);
        assert!(result.is_ok(), "Failed to drop domain: {:?}", result);

        assert!(!db.catalog.domain_exists("POSITIVEINT"));
    } else {
        panic!("Expected DropDomain statement");
    }
}

#[test]
fn test_drop_domain_restrict() {
    let mut db = Database::new();

    // Create domain
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        DomainExecutor::execute_create_domain(&create_stmt, &mut db).unwrap();
    }

    // Drop with RESTRICT
    let sql = "DROP DOMAIN PositiveInt RESTRICT";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropDomain(drop_stmt) = stmt {
        let result = DomainExecutor::execute_drop_domain(&drop_stmt, &mut db);
        assert!(result.is_ok(), "Failed to drop domain with RESTRICT: {:?}", result);

        assert!(!db.catalog.domain_exists("POSITIVEINT"));
    } else {
        panic!("Expected DropDomain statement");
    }
}

#[test]
fn test_drop_domain_cascade() {
    let mut db = Database::new();

    // Create domain
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        DomainExecutor::execute_create_domain(&create_stmt, &mut db).unwrap();
    }

    // Drop with CASCADE
    let sql = "DROP DOMAIN PositiveInt CASCADE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropDomain(drop_stmt) = stmt {
        let result = DomainExecutor::execute_drop_domain(&drop_stmt, &mut db);
        assert!(result.is_ok(), "Failed to drop domain with CASCADE: {:?}", result);

        assert!(!db.catalog.domain_exists("POSITIVEINT"));
    } else {
        panic!("Expected DropDomain statement");
    }
}

#[test]
fn test_drop_domain_not_found() {
    let mut db = Database::new();

    // Try to drop a domain that doesn't exist
    let sql = "DROP DOMAIN NonExistent";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropDomain(drop_stmt) = stmt {
        let result = DomainExecutor::execute_drop_domain(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail when dropping non-existent domain");
    } else {
        panic!("Expected DropDomain statement");
    }
}

#[test]
fn test_domain_case_insensitivity() {
    let mut db = Database::new();

    // Create domain with mixed case
    let sql = "CREATE DOMAIN PositiveInt AS INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateDomain(create_stmt) = stmt {
        DomainExecutor::execute_create_domain(&create_stmt, &mut db).unwrap();
    }

    // Should be able to find it with different case
    assert!(db.catalog.domain_exists("POSITIVEINT"));
    assert!(db.catalog.domain_exists("positiveint"));

    // Drop with different case
    let sql = "DROP DOMAIN positiveint";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropDomain(drop_stmt) = stmt {
        let result = DomainExecutor::execute_drop_domain(&drop_stmt, &mut db);
        assert!(result.is_ok());
    }
}

#[test]
fn test_domain_with_complex_check() {
    let mut db = Database::new();

    // Create domain with complex CHECK expression
    let sql = "CREATE DOMAIN RangeInt AS INTEGER CHECK (VALUE >= 0 AND VALUE <= 100)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with complex CHECK: {:?}", result);

        let domain = db.catalog.get_domain("RANGEINT");
        assert!(domain.is_some());
        assert_eq!(domain.unwrap().constraints.len(), 1);
    } else {
        panic!("Expected CreateDomain statement");
    }
}

#[test]
fn test_domain_with_default_current_date() {
    let mut db = Database::new();

    // Create domain with special DEFAULT value
    let sql = "CREATE DOMAIN CreationDate AS DATE DEFAULT CURRENT_DATE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    if let Statement::CreateDomain(create_stmt) = stmt {
        let result = DomainExecutor::execute_create_domain(&create_stmt, &mut db);
        assert!(result.is_ok(), "Failed to create domain with CURRENT_DATE: {:?}", result);

        let domain = db.catalog.get_domain("CREATIONDATE");
        assert!(domain.is_some());
        assert!(domain.unwrap().default.is_some());
    } else {
        panic!("Expected CreateDomain statement");
    }
}
