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
            assert_eq!(grant_stmt.privileges[2], ast::PrivilegeType::Update(None));
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
            assert_eq!(grant_stmt.privileges[2], ast::PrivilegeType::Update(None));
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
            assert!(!grant_stmt.with_grant_option);
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

// Issue #566: USAGE privilege with implicit object type defaults to Schema

#[test]
fn test_parse_grant_usage_implicit_schema() {
    // When USAGE privilege is specified without object type keyword, it should default to Schema
    let sql = "GRANT USAGE ON my_schema TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "MY_SCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_usage_implicit_schema_with_grant_option() {
    // USAGE with implicit object type + WITH GRANT OPTION
    let sql = "GRANT USAGE ON test_schema TO admin WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "TEST_SCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_select_implicit_table() {
    // Other privileges (non-USAGE) should still default to Table when object type not specified
    let sql = "GRANT SELECT ON users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_insert_implicit_table() {
    // INSERT privilege should default to Table (existing behavior preserved)
    let sql = "GRANT INSERT ON orders TO clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Insert);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "ORDERS");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

// SQL:1999 Core Feature E081-06: REFERENCES privilege (table-level)

#[test]
fn test_parse_grant_references_on_table() {
    let sql = "GRANT REFERENCES ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::References(None));
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_case_insensitive() {
    let sql = "grant references on table employees to clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::References(None));
            assert_eq!(grant_stmt.object_name.to_string(), "EMPLOYEES");
            assert_eq!(grant_stmt.grantees, vec!["CLERK"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_combined_with_select() {
    let sql = "GRANT REFERENCES, SELECT ON TABLE products TO buyer";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 2);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::References(None));
            assert_eq!(grant_stmt.privileges[1], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.object_name.to_string(), "PRODUCTS");
            assert_eq!(grant_stmt.grantees, vec!["BUYER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_with_grant_option_table_level() {
    let sql = "GRANT REFERENCES ON TABLE orders TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::References(None));
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "ORDERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

// Column-level privilege tests (SQL:1999 Feature E081-05, E081-07)

#[test]
fn test_parse_grant_update_column_single() {
    let sql = "GRANT UPDATE(salary) ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::Update(Some(vec!["SALARY".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "EMPLOYEES");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_columns_multiple() {
    let sql = "GRANT UPDATE(salary, bonus, commission) ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::Update(Some(vec![
                    "SALARY".to_string(),
                    "BONUS".to_string(),
                    "COMMISSION".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_column_single() {
    let sql = "GRANT REFERENCES(id) ON TABLE users TO foreign_table";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::References(Some(vec!["ID".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_columns_multiple() {
    let sql = "GRANT REFERENCES(user_id, account_id) ON TABLE accounts TO orders";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::References(Some(vec![
                    "USER_ID".to_string(),
                    "ACCOUNT_ID".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_mixed_table_and_column_privileges() {
    let sql = "GRANT SELECT, UPDATE(salary), DELETE ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 3);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(
                grant_stmt.privileges[1],
                ast::PrivilegeType::Update(Some(vec!["SALARY".to_string()]))
            );
            assert_eq!(grant_stmt.privileges[2], ast::PrivilegeType::Delete);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_with_grant_option() {
    let sql = "GRANT UPDATE(salary) ON TABLE employees TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::Update(Some(vec!["SALARY".to_string()]))
            );
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_qualified_table() {
    let sql = "GRANT UPDATE(salary) ON TABLE hr.employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::Update(Some(vec!["SALARY".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "HR.EMPLOYEES");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_case_insensitive() {
    let sql = "grant update(salary) on table employees to manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                ast::PrivilegeType::Update(Some(vec!["SALARY".to_string()]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_table_level() {
    // Test that UPDATE without columns still works (table-level privilege)
    let sql = "GRANT UPDATE ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Update(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_table_level() {
    // Test that REFERENCES without columns still works (table-level privilege)
    let sql = "GRANT REFERENCES ON TABLE users TO foreign_table";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::References(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

// Issue #561: GRANT/REVOKE on FUNCTION, PROCEDURE, ROUTINE objects

#[test]
fn test_parse_grant_execute_on_function() {
    let sql = "GRANT EXECUTE ON FUNCTION my_func TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Function);
            assert_eq!(grant_stmt.object_name.to_string(), "MY_FUNC");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_procedure() {
    let sql = "GRANT EXECUTE ON PROCEDURE proc_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Procedure);
            assert_eq!(grant_stmt.object_name.to_string(), "PROC_NAME");
            assert_eq!(grant_stmt.grantees, vec!["USER1"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_routine() {
    let sql = "GRANT EXECUTE ON ROUTINE routine_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Routine);
            assert_eq!(grant_stmt.object_name.to_string(), "ROUTINE_NAME");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_method() {
    let sql = "GRANT EXECUTE ON METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Method);
            assert_eq!(grant_stmt.object_name.to_string(), "METHOD_NAME");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_constructor_method() {
    let sql = "GRANT EXECUTE ON CONSTRUCTOR METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::ConstructorMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "METHOD_NAME");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_static_method() {
    let sql = "GRANT EXECUTE ON STATIC METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::StaticMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "METHOD_NAME");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_instance_method() {
    let sql = "GRANT EXECUTE ON INSTANCE METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::InstanceMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "METHOD_NAME");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_implicit_routine() {
    // EXECUTE without object type should default to ROUTINE
    let sql = "GRANT EXECUTE ON my_func TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Routine);
            assert_eq!(grant_stmt.object_name.to_string(), "MY_FUNC");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_with_grant_option() {
    let sql = "GRANT EXECUTE ON FUNCTION my_func TO user1 WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Function);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
