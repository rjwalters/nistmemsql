//! Procedural execution engine for stored procedures and functions
//!
//! This module provides the execution environment for:
//! - Stored procedure bodies (CREATE PROCEDURE ... BEGIN ... END)
//! - User-defined function bodies (CREATE FUNCTION ... BEGIN ... END)
//! - Procedural statements (DECLARE, SET, IF, WHILE, LOOP, REPEAT, RETURN)
//!
//! ## Module Structure
//!
//! - `context`: Execution context with variables, parameters, and scope management
//! - `executor`: Execute individual procedural statements
//! - `control_flow`: Control flow execution (IF, WHILE, LOOP, REPEAT)

pub mod context;
pub mod executor;
pub mod control_flow;
pub mod function;

pub use context::{ExecutionContext, ControlFlow};
pub use executor::execute_procedural_statement;
pub use function::execute_user_function;
