//! Cursor DDL operations
//!
//! This module contains AST nodes for cursor-related DDL operations (SQL:1999 Feature E121):
//! - DECLARE CURSOR
//! - OPEN CURSOR
//! - FETCH
//! - CLOSE CURSOR

/// DECLARE CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct DeclareCursorStmt {
    pub cursor_name: String,
    pub insensitive: bool,
    pub scroll: bool,
    pub hold: Option<bool>, /* Some(true) = WITH HOLD, Some(false) = WITHOUT HOLD, None = not
                             * specified */
    pub query: Box<crate::SelectStmt>,
    pub updatability: CursorUpdatability,
}

/// Cursor updatability specification
#[derive(Debug, Clone, PartialEq)]
pub enum CursorUpdatability {
    ReadOnly,
    Update { columns: Option<Vec<String>> }, /* Some(cols) = UPDATE OF cols, None = UPDATE (all
                                              * columns) */
    Unspecified,
}

/// OPEN CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct OpenCursorStmt {
    pub cursor_name: String,
}

/// FETCH statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct FetchStmt {
    pub cursor_name: String,
    pub orientation: FetchOrientation,
    pub into_variables: Option<Vec<String>>, // INTO variable_list
}

/// Fetch orientation specification
#[derive(Debug, Clone, PartialEq)]
pub enum FetchOrientation {
    Next,
    Prior,
    First,
    Last,
    Absolute(i64), // ABSOLUTE n
    Relative(i64), // RELATIVE n
}

/// CLOSE CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct CloseCursorStmt {
    pub cursor_name: String,
}
