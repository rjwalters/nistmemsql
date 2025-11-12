//! Tests for stored procedure and function functionality

use vibesql_ast::*;
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::advanced_objects;
use crate::errors::ExecutorError;

fn setup_test_table(db: &mut Database) {
    // CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50))
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

#[test]
fn test_create_procedure_simple() {
    let mut db = Database::new();
    
    let proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
    };
    
    let result = advanced_objects::execute_create_procedure(&proc, &mut db);
    assert!(result.is_ok());
    
    // Verify procedure was created
    assert!(db.catalog.procedure_exists("test_proc"));
}

#[test]
fn test_drop_procedure_simple() {
    let mut db = Database::new();
    
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
    };
    
    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();
    assert!(db.catalog.procedure_exists("test_proc"));
    
    let drop_proc = DropProcedureStmt {
        procedure_name: "test_proc".to_string(),
        if_exists: false,
    };
    
    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_ok());
    assert!(!db.catalog.procedure_exists("test_proc"));
}

#[test]
fn test_drop_procedure_if_exists_not_found() {
    let mut db = Database::new();
    
    let drop_proc = DropProcedureStmt {
        procedure_name: "nonexistent".to_string(),
        if_exists: true,
    };
    
    // Should not error with if_exists
    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_drop_procedure_without_if_exists_not_found() {
    let mut db = Database::new();
    
    let drop_proc = DropProcedureStmt {
        procedure_name: "nonexistent".to_string(),
        if_exists: false,
    };
    
    // Should error without if_exists
    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_create_function_simple() {
    let mut db = Database::new();
    
    let func = CreateFunctionStmt {
        function_name: "test_func".to_string(),
        parameters: vec![],
        return_type: DataType::Integer,
        body: ProcedureBody::BeginEnd(vec![]),
    };
    
    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
    
    // Verify function was created
    assert!(db.catalog.function_exists("test_func"));
}

#[test]
fn test_drop_function_simple() {
    let mut db = Database::new();
    
    let create_func = CreateFunctionStmt {
        function_name: "test_func".to_string(),
        parameters: vec![],
        return_type: DataType::Integer,
        body: ProcedureBody::BeginEnd(vec![]),
    };
    
    advanced_objects::execute_create_function(&create_func, &mut db).unwrap();
    assert!(db.catalog.function_exists("test_func"));
    
    let drop_func = DropFunctionStmt {
        function_name: "test_func".to_string(),
        if_exists: false,
    };
    
    let result = advanced_objects::execute_drop_function(&drop_func, &mut db);
    assert!(result.is_ok());
    assert!(!db.catalog.function_exists("test_func"));
}

#[test]
fn test_call_procedure_simple() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    let create_proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_proc".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    // Phase 1: Procedures are stored but not yet executable
    // They're stored as RawSql debug strings, not executable AST
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedFeature(_)));
}

// ============================================================================
// Phase 3: Control Flow Tests
// ============================================================================

mod control_flow_tests {
    use super::*;
    use crate::procedural::{ExecutionContext, ControlFlow};
    use crate::procedural::executor::execute_procedural_statement;

    // Test 1: Simple IF with boolean condition
    #[test]
    fn test_if_simple_boolean_true() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // DECLARE result VARCHAR(20);
        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF TRUE THEN
        //   SET result = 'executed';
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("executed".to_string()))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("executed".to_string())));
    }

    #[test]
    fn test_if_simple_boolean_false() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF FALSE THEN
        //   SET result = 'executed';
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("executed".to_string()))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        // Variable should remain unchanged
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("initial".to_string())));
    }

    #[test]
    fn test_if_with_else() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF FALSE THEN
        //   SET result = 'then';
        // ELSE
        //   SET result = 'else';
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("then".to_string()))),
                }
            ],
            else_statements: Some(vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("else".to_string()))),
                }
            ]),
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("else".to_string())));
    }

    #[test]
    fn test_if_integer_condition() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF 1 THEN (non-zero = true)
        //   SET result = 'nonzero';
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Integer(1))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("nonzero".to_string()))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("nonzero".to_string())));

        // Test with 0 (should be false)
        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));
        let if_stmt_zero = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Integer(0))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("zero".to_string()))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt_zero, &mut ctx, &mut db);
        assert!(result.is_ok());
        // Should not execute then branch
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("initial".to_string())));
    }

    #[test]
    fn test_if_null_condition() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF NULL THEN (NULL = false)
        //   SET result = 'executed';
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Null)),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Varchar("executed".to_string()))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        // Should not execute then branch
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("initial".to_string())));
    }

    // Test 2: WHILE loop basic functionality
    #[test]
    fn test_while_loop_basic() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // DECLARE counter INT DEFAULT 0;
        ctx.set_variable("counter", SqlValue::Integer(0));

        // WHILE counter < 3 DO
        //   SET counter = counter + 1;
        // END WHILE;
        // Note: We need to use a simple approach since we can't evaluate complex expressions yet
        // We'll manually test iterations

        // For this test, we'll verify WHILE executes when condition is true
        // and skips when condition is false

        // First: condition is true, should execute once
        let while_stmt = ProceduralStatement::While {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            statements: vec![
                ProceduralStatement::Set {
                    name: "counter".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
                // Add a LEAVE to prevent infinite loop
                ProceduralStatement::Leave("".to_string()),
            ],
        };

        // Push empty label for unlabeled WHILE
        ctx.push_label("");
        let result = execute_procedural_statement(&while_stmt, &mut ctx, &mut db);
        ctx.pop_label("");

        assert!(result.is_ok());
        assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_while_loop_false_condition() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("counter", SqlValue::Integer(0));

        // WHILE FALSE DO
        //   SET counter = 100;  -- Should never execute
        // END WHILE;
        let while_stmt = ProceduralStatement::While {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
            statements: vec![
                ProceduralStatement::Set {
                    name: "counter".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Integer(100))),
                },
            ],
        };

        let result = execute_procedural_statement(&while_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        // Counter should remain 0
        assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(0)));
    }

    // Test 3: LOOP with LEAVE
    #[test]
    fn test_loop_with_leave() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("counter", SqlValue::Integer(0));

        // Push label for loop
        ctx.push_label("my_loop");

        // my_loop: LOOP
        //   SET counter = counter + 1;
        //   LEAVE my_loop;
        // END LOOP;
        let loop_stmt = ProceduralStatement::Loop {
            statements: vec![
                ProceduralStatement::Set {
                    name: "counter".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
                ProceduralStatement::Leave("my_loop".to_string()),
            ],
        };

        let result = execute_procedural_statement(&loop_stmt, &mut ctx, &mut db);
        ctx.pop_label("my_loop");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_loop_leave_invalid_label() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // LOOP
        //   LEAVE nonexistent_label;  -- Should error
        // END LOOP;
        let leave_stmt = ProceduralStatement::Leave("nonexistent_label".to_string());

        let result = execute_procedural_statement(&leave_stmt, &mut ctx, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::LabelNotFound(_)));
    }

    // Test 4: REPEAT/UNTIL
    #[test]
    fn test_repeat_until() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("counter", SqlValue::Integer(0));

        // REPEAT
        //   SET counter = 1;
        // UNTIL TRUE END REPEAT;
        let repeat_stmt = ProceduralStatement::Repeat {
            statements: vec![
                ProceduralStatement::Set {
                    name: "counter".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
            ],
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        };

        let result = execute_procedural_statement(&repeat_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Continue);
        // Body should execute at least once
        assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_repeat_executes_at_least_once() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("executed", SqlValue::Boolean(false));

        // REPEAT
        //   SET executed = TRUE;
        // UNTIL TRUE END REPEAT;  -- Exit immediately after first iteration
        let repeat_stmt = ProceduralStatement::Repeat {
            statements: vec![
                ProceduralStatement::Set {
                    name: "executed".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Boolean(true))),
                },
            ],
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        };

        let result = execute_procedural_statement(&repeat_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(ctx.get_variable("executed"), Some(&SqlValue::Boolean(true)));
    }

    // Test 5: ITERATE validates label exists
    // Note: Full ITERATE behavior testing would require more complex setup to avoid infinite loops
    #[test]
    fn test_iterate_validates_label() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // Test that ITERATE validates the label exists
        ctx.push_label("valid_loop");

        let iterate_stmt = ProceduralStatement::Iterate("valid_loop".to_string());
        let result = execute_procedural_statement(&iterate_stmt, &mut ctx, &mut db);

        // Should return ControlFlow::Iterate
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Iterate("valid_loop".to_string()));

        ctx.pop_label("valid_loop");
    }

    #[test]
    fn test_iterate_invalid_label() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // ITERATE nonexistent_label;  -- Should error
        let iterate_stmt = ProceduralStatement::Iterate("nonexistent_label".to_string());

        let result = execute_procedural_statement(&iterate_stmt, &mut ctx, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::LabelNotFound(_)));
    }

    // Test 6: Nested IF statements
    #[test]
    fn test_nested_if() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Varchar("initial".to_string()));

        // IF TRUE THEN
        //   IF TRUE THEN
        //     SET result = 'nested';
        //   END IF;
        // END IF;
        let nested_if = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![
                ProceduralStatement::If {
                    condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
                    then_statements: vec![
                        ProceduralStatement::Set {
                            name: "result".to_string(),
                            value: Box::new(Expression::Literal(SqlValue::Varchar("nested".to_string()))),
                        }
                    ],
                    else_statements: None,
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&nested_if, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar("nested".to_string())));
    }

    // Test 7: RETURN in IF branch
    #[test]
    fn test_if_with_return() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        // IF TRUE THEN
        //   RETURN 42;
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![
                ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(42)))),
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ControlFlow::Return(SqlValue::Integer(42)));
    }

    // Test 8: Variable persistence across control flow
    #[test]
    fn test_variable_persistence() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("x", SqlValue::Integer(1));

        // IF TRUE THEN
        //   SET x = 2;
        // END IF;
        let if_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![
                ProceduralStatement::Set {
                    name: "x".to_string(),
                    value: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }
            ],
            else_statements: None,
        };

        let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
        assert!(result.is_ok());

        // Variable modification should persist after IF
        assert_eq!(ctx.get_variable("x"), Some(&SqlValue::Integer(2)));
    }
}
