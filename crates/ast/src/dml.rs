//! Data Manipulation Language (DML) statements
//!
//! This module contains INSERT, UPDATE, and DELETE statement types.

use crate::Expression;

// ============================================================================
// INSERT Statement
// ============================================================================

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>, // Can insert multiple rows
}

// ============================================================================
// UPDATE Statement
// ============================================================================

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

/// Column assignment (column = value)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

// ============================================================================
// DELETE Statement
// ============================================================================

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}
