//! Tests for GRANT statement parsing

use crate::Parser;

#[test]
fn test_parse_grant_select_on_table() {
    let sql = "GRANT SELECT ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_case_insensitive() {
    let sql = "grant select on table employees to clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "EMPLOYEES");
            assert_eq!(grant_stmt.grantees, vec!["CLERK"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_qualified_table_name() {
    let sql = "GRANT SELECT ON TABLE public.users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC.USERS");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_multiple_privileges() {
    let sql = "GRANT SELECT, INSERT, UPDATE ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 3);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.privileges[1], ast::PrivilegeType::Insert);
            assert_eq!(grant_stmt.privileges[2], ast::PrivilegeType::Update);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_privilege_types() {
    let sql = "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE orders TO clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 4);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.privileges[1], ast::PrivilegeType::Insert);
            assert_eq!(grant_stmt.privileges[2], ast::PrivilegeType::Update);
            assert_eq!(grant_stmt.privileges[3], ast::PrivilegeType::Delete);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_multiple_grantees() {
    let sql = "GRANT SELECT ON TABLE users TO role1, role2, role3";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.grantees.len(), 3);
            assert_eq!(grant_stmt.grantees, vec!["ROLE1", "ROLE2", "ROLE3"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_multiple_privileges_and_grantees() {
    let sql = "GRANT SELECT, INSERT ON TABLE users TO r1, r2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 2);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.privileges[1], ast::PrivilegeType::Insert);
            assert_eq!(grant_stmt.grantees.len(), 2);
            assert_eq!(grant_stmt.grantees, vec!["R1", "R2"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_privileges_with_keyword() {
    let sql = "GRANT ALL PRIVILEGES ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::AllPrivileges);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_without_privileges_keyword() {
    let sql = "GRANT ALL ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::AllPrivileges);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_case_insensitive() {
    let sql = "grant all on table employees to clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::AllPrivileges);
            assert_eq!(grant_stmt.object_name.to_string(), "EMPLOYEES");
            assert_eq!(grant_stmt.grantees, vec!["CLERK"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

// Phase 2.5: Schema privilege tests

#[test]
fn test_parse_grant_usage_on_schema() {
    let sql = "GRANT USAGE ON SCHEMA public TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_create_on_schema() {
    let sql = "GRANT CREATE ON SCHEMA public TO admin_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Create);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_usage_and_create_on_schema() {
    let sql = "GRANT USAGE, CREATE ON SCHEMA myschema TO developer";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 2);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Usage);
            assert_eq!(grant_stmt.privileges[1], ast::PrivilegeType::Create);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "MYSCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["DEVELOPER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_on_schema() {
    let sql = "GRANT ALL PRIVILEGES ON SCHEMA public TO admin_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::AllPrivileges);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN_ROLE"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

// Phase 2.4: WITH GRANT OPTION tests

#[test]
fn test_parse_grant_with_grant_option() {
    let sql = "GRANT SELECT ON TABLE users TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_without_grant_option() {
    let sql = "GRANT SELECT ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.with_grant_option, false);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_schema_with_grant_option() {
    let sql = "GRANT USAGE ON SCHEMA public TO user_role WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_grant_with_grant_option_case_insensitive() {
    let sql = "grant select on table users to manager with grant option";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_grant_all_privileges_with_grant_option() {
    let sql = "GRANT ALL PRIVILEGES ON TABLE users TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::AllPrivileges);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
