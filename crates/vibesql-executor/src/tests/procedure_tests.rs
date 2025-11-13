//! Tests for stored procedure and function functionality

use vibesql_ast::*;
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::advanced_objects;
use crate::errors::ExecutorError;

// Helper function to create a simple procedure with defaults for Phase 6 fields
fn create_simple_procedure(
    name: &str,
    parameters: Vec<ProcedureParameter>,
    body: Vec<ProceduralStatement>,
) -> CreateProcedureStmt {
    CreateProcedureStmt {
        procedure_name: name.to_string(),
        parameters,
        body: ProcedureBody::BeginEnd(body),
        sql_security: None,
        comment: None,
        language: None,
}

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
        sql_security: None,
        comment: None,
        language: None,
    
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
        sql_security: None,
        comment: None,
        language: None,
    
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
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
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
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
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
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_proc".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    // Phase 2: Empty procedures should execute successfully
    assert!(result.is_ok());
}

// ========== Phase 2 Tests: Parameter Binding and Execution ==========

#[test]
fn test_call_procedure_with_in_parameter() {
    let mut db = Database::new();

    // CREATE PROCEDURE greet(IN name VARCHAR(50))
    // BEGIN
    //   DECLARE greeting VARCHAR(100);
    //   SET greeting = name;
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "greet".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: "name".to_string(),
            data_type: DataType::Varchar { max_length: Some(50) },
        }],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "greeting".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "greeting".to_string(),
                value: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL greet('Alice');
    let call = CallStmt {
        procedure_name: "greet".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Varchar("Alice".to_string()))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_call_procedure_with_multiple_parameters() {
    let mut db = Database::new();

    // CREATE PROCEDURE add_numbers(IN a INT, IN b INT)
    // BEGIN
    //   DECLARE result INT;
    //   SET result = a + b;
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "add_numbers".to_string(),
        parameters: vec![
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "a".to_string(),
                data_type: DataType::Integer,
            },
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "b".to_string(),
                data_type: DataType::Integer,
            },
        ],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "result".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "a".to_string(),
                    }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "b".to_string(),
                    }),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL add_numbers(10, 20);
    let call = CallStmt {
        procedure_name: "add_numbers".to_string(),
        arguments: vec![
            Expression::Literal(SqlValue::Integer(10)),
            Expression::Literal(SqlValue::Integer(20)),
        ],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_declare_with_default_value() {
    let mut db = Database::new();

    // CREATE PROCEDURE test_defaults()
    // BEGIN
    //   DECLARE counter INT DEFAULT 42;
    //   DECLARE message VARCHAR(20) DEFAULT 'Hello';
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_defaults".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "counter".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(42)))),
            },
            ProceduralStatement::Declare {
                name: "message".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                default_value: Some(Box::new(Expression::Literal(SqlValue::Varchar("Hello".to_string())))),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_defaults".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_variable_in_expression() {
    let mut db = Database::new();

    // CREATE PROCEDURE test_math()
    // BEGIN
    //   DECLARE x INT DEFAULT 10;
    //   DECLARE y INT;
    //   SET y = x * 2;
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_math".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "x".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(10)))),
            },
            ProceduralStatement::Declare {
                name: "y".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "y".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "x".to_string(),
                    }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_math".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_concat_function_in_procedure() {
    let mut db = Database::new();

    // CREATE PROCEDURE test_concat(IN first VARCHAR(50), IN last VARCHAR(50))
    // BEGIN
    //   DECLARE full_name VARCHAR(100);
    //   SET full_name = CONCAT(first, ' ', last);
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_concat".to_string(),
        parameters: vec![
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "first".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
            },
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "last".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
            },
        ],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "full_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "full_name".to_string(),
                value: Box::new(Expression::Function {
                    name: "CONCAT".to_string(),
                    args: vec![
                        Expression::ColumnRef {
                            table: None,
                            column: "first".to_string(),
                        },
                        Expression::Literal(SqlValue::Varchar(" ".to_string())),
                        Expression::ColumnRef {
                            table: None,
                            column: "last".to_string(),
                        },
                    ],
                    character_unit: None,
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_concat".to_string(),
        arguments: vec![
            Expression::Literal(SqlValue::Varchar("John".to_string())),
            Expression::Literal(SqlValue::Varchar("Doe".to_string())),
        ],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_parameter_count_mismatch() {
    let mut db = Database::new();

    let create_proc = CreateProcedureStmt {
        procedure_name: "needs_param".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: "x".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // Call with no arguments (expects 1)
    let call = CallStmt {
        procedure_name: "needs_param".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::Other(_)));
}

#[test]
fn test_out_parameter_not_yet_supported() {
    let mut db = Database::new();

    let create_proc = CreateProcedureStmt {
        procedure_name: "test_out".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::Out,
            name: "result".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
        };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_out".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(0))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    // Phase 2: OUT parameters not yet supported
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedFeature(_)));
}

#[test]
fn test_procedure_not_found() {
    let mut db = Database::new();

    let call = CallStmt {
        procedure_name: "nonexistent".to_string(),
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::Other(_)));
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

// ============================================================================
// Phase 6 Robustness: Edge Case Tests
// ============================================================================

mod edge_case_tests {
    use super::*;
    use crate::procedural::ExecutionContext;

    // Edge Case 1: Empty Procedure Body
    #[test]
    fn test_empty_procedure_body() {
        let mut db = Database::new();

        // CREATE PROCEDURE do_nothing()
        // BEGIN
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "do_nothing".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        // CALL do_nothing();
        let call = CallStmt {
            procedure_name: "do_nothing".to_string(),
            arguments: vec![],
        };

        // Should succeed with no operations
        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 2: NULL Parameter Values
    #[test]
    fn test_null_parameter_in_procedure() {
        let mut db = Database::new();

        // CREATE PROCEDURE handle_null(IN x INT)
        // BEGIN
        //   DECLARE result INT;
        //   SET result = x;  -- result becomes NULL
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "handle_null".to_string(),
            parameters: vec![ProcedureParameter {
                mode: ParameterMode::In,
                name: "x".to_string(),
                data_type: DataType::Integer,
            }],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "result".to_string(),
                    data_type: DataType::Integer,
                    default_value: None,
                },
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "x".to_string(),
                    }),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        // CALL handle_null(NULL);
        let call = CallStmt {
            procedure_name: "handle_null".to_string(),
            arguments: vec![Expression::Literal(SqlValue::Null)],
        };

        // Should handle NULL parameter gracefully
        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_declare_with_null_default() {
        let mut db = Database::new();

        // CREATE PROCEDURE test_null_default()
        // BEGIN
        //   DECLARE x INT;  -- Defaults to NULL
        //   DECLARE y INT DEFAULT NULL;  -- Explicitly NULL
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "test_null_default".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "x".to_string(),
                    data_type: DataType::Integer,
                    default_value: None,  // Implicitly NULL
                },
                ProceduralStatement::Declare {
                    name: "y".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Null))),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "test_null_default".to_string(),
            arguments: vec![],
        };

        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 3: Parameter Name Conflicts with Variables
    #[test]
    fn test_parameter_variable_shadowing() {
        let mut db = Database::new();

        // CREATE PROCEDURE test_shadowing(IN x INT)
        // BEGIN
        //   DECLARE x INT DEFAULT 10;  -- Shadows parameter
        //   -- This is allowed in SQL but inner scope takes precedence
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "test_shadowing".to_string(),
            parameters: vec![ProcedureParameter {
                mode: ParameterMode::In,
                name: "x".to_string(),
                data_type: DataType::Integer,
            }],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "x".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(10)))),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "test_shadowing".to_string(),
            arguments: vec![Expression::Literal(SqlValue::Integer(5))],
        };

        // Should succeed - local variable shadows parameter
        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 4: Very Long Procedure Bodies
    #[test]
    fn test_very_long_procedure_body() {
        let mut db = Database::new();

        // Create a procedure with 100 SET statements
        let mut statements = vec![];

        // Declare initial variable
        statements.push(ProceduralStatement::Declare {
            name: "counter".to_string(),
            data_type: DataType::Integer,
            default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
        });

        // Add 100 SET statements
        for i in 1..=100 {
            statements.push(ProceduralStatement::Set {
                name: "counter".to_string(),
                value: Box::new(Expression::Literal(SqlValue::Integer(i))),
            });
        }

        let create_proc = CreateProcedureStmt {
            procedure_name: "long_procedure".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(statements),
        sql_security: None,
        comment: None,
        language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "long_procedure".to_string(),
            arguments: vec![],
        };

        // Should handle long body without issues
        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 5: Deeply Nested Control Flow
    #[test]
    fn test_deeply_nested_control_flow() {
        let mut db = Database::new();
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("result", SqlValue::Integer(0));

        // Create 10 levels of nested IF statements
        let mut innermost = vec![
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }
        ];

        let mut nested_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: innermost.clone(),
            else_statements: None,
        };

        // Nest 9 more levels
        for _ in 0..9 {
            nested_stmt = ProceduralStatement::If {
                condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
                then_statements: vec![nested_stmt],
                else_statements: None,
            };
        }

        let result = crate::procedural::executor::execute_procedural_statement(
            &nested_stmt,
            &mut ctx,
            &mut db,
        );

        assert!(result.is_ok());
        assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Integer(10)));
    }

    // Edge Case 6: Special Characters in Names
    #[test]
    fn test_procedure_with_special_chars_in_name() {
        let mut db = Database::new();

        // CREATE PROCEDURE `proc-with-dash`()
        // BEGIN
        //   DECLARE result INT DEFAULT 42;
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "proc-with-dash".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "result".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(42)))),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "proc-with-dash".to_string(),
            arguments: vec![],
        };

        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_procedure_with_spaces_in_name() {
        let mut db = Database::new();

        // CREATE PROCEDURE `proc with spaces`()
        let create_proc = CreateProcedureStmt {
            procedure_name: "proc with spaces".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "proc with spaces".to_string(),
            arguments: vec![],
        };

        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 7: Large Numbers in Arithmetic
    #[test]
    fn test_large_number_arithmetic() {
        let mut db = Database::new();

        // CREATE PROCEDURE test_large_numbers()
        // BEGIN
        //   DECLARE large INT DEFAULT 1000000;
        //   DECLARE result INT;
        //   SET result = large * 2;
        // END;
        let create_proc = CreateProcedureStmt {
            procedure_name: "test_large_numbers".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "large".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(1_000_000)))),
                },
                ProceduralStatement::Declare {
                    name: "result".to_string(),
                    data_type: DataType::Integer,
                    default_value: None,
                },
                ProceduralStatement::Set {
                    name: "result".to_string(),
                    value: Box::new(Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "large".to_string(),
                        }),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                    }),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "test_large_numbers".to_string(),
            arguments: vec![],
        };

        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }

    // Edge Case 8: Multiple Variables with Same Operations
    #[test]
    fn test_multiple_variable_operations() {
        let mut db = Database::new();

        // Test that multiple variables don't interfere with each other
        let create_proc = CreateProcedureStmt {
            procedure_name: "test_multi_vars".to_string(),
            parameters: vec![],
            body: ProcedureBody::BeginEnd(vec![
                ProceduralStatement::Declare {
                    name: "a".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(1)))),
                },
                ProceduralStatement::Declare {
                    name: "b".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(2)))),
                },
                ProceduralStatement::Declare {
                    name: "c".to_string(),
                    data_type: DataType::Integer,
                    default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(3)))),
                },
                ProceduralStatement::Set {
                    name: "a".to_string(),
                    value: Box::new(Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "b".to_string(),
                        }),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "c".to_string(),
                        }),
                    }),
                },
            ]),
            sql_security: None,
            comment: None,
            language: None,
        };

        advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

        let call = CallStmt {
            procedure_name: "test_multi_vars".to_string(),
            arguments: vec![],
        };

        let result = advanced_objects::execute_call(&call, &mut db);
        assert!(result.is_ok());
    }
}
