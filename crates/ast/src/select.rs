//! SELECT statement types
//!
//! This module contains all types related to SELECT queries including
//! SELECT items, FROM clauses, JOINs, and ORDER BY.

use crate::Expression;

// ============================================================================
// SELECT Statement
// ============================================================================

/// SELECT statement structure
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    pub select_list: Vec<SelectItem>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Item in the SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// SELECT *
    Wildcard,
    /// SELECT expr [AS alias]
    Expression { expr: Expression, alias: Option<String> },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromClause>,
        right: Box<FromClause>,
        join_type: JoinType,
        condition: Option<Expression>,
    },
    /// Subquery in FROM clause (derived table)
    /// SQL:1999 requires AS alias for derived tables
    /// Example: FROM (SELECT * FROM users WHERE active = TRUE) AS active_users
    Subquery {
        query: Box<SelectStmt>,
        alias: String,
    },
}

/// JOIN types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: OrderDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}
