//! SELECT statement types
//!
//! This module contains all types related to SELECT queries including
//! SELECT items, FROM clauses, JOINs, and ORDER BY.

use crate::Expression;

// ============================================================================
// Common Table Expressions (CTEs)
// ============================================================================

/// Common Table Expression (CTE) definition
///
/// CTEs are temporary named result sets defined with the WITH clause that exist
/// only for the duration of a single query.
///
/// Example: `WITH regional_sales AS (SELECT region, SUM(amount) FROM orders GROUP BY region)`
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    /// Name of the CTE
    pub name: String,
    /// Optional column name list (e.g., `WITH cte (col1, col2) AS (...)`)
    pub columns: Option<Vec<String>>,
    /// The query defining the CTE
    pub query: Box<SelectStmt>,
}

// ============================================================================
// SELECT Statement
// ============================================================================

/// SELECT statement structure
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    /// Optional WITH clause containing CTEs
    pub with_clause: Option<Vec<CommonTableExpr>>,
    pub distinct: bool,
    pub select_list: Vec<SelectItem>,
    /// Optional INTO clause for DDL SELECT INTO statements (SQL:1999 Feature E111)
    /// Creates a new table from the query results
    pub into_table: Option<String>,
    /// Optional INTO clause for procedural SELECT INTO statements
    /// Stores query results into procedural variables (e.g., SELECT col INTO @var)
    pub into_variables: Option<Vec<String>>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Set operation (UNION, INTERSECT, EXCEPT) combining this query with another
    pub set_operation: Option<SetOperation>,
}

/// Set operation combining two SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub op: SetOperator,
    pub all: bool, // true = ALL, false = DISTINCT (default)
    pub right: Box<SelectStmt>,
}

/// Item in the SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// SELECT * [AS (col1, col2, ...)]
    /// SQL:1999 Feature E051-07: Derived column lists
    Wildcard { alias: Option<Vec<String>> },
    /// SELECT table.* [AS (col1, col2, ...)] or SELECT alias.* [AS (col1, col2, ...)]
    /// SQL:1999 Feature E051-08: Correlation names in FROM clause with derived column lists
    QualifiedWildcard { qualifier: String, alias: Option<Vec<String>> },
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
        /// True for NATURAL JOIN (joins on common column names)
        natural: bool,
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

/// Set operators for combining SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperator {
    /// UNION - combines results from two queries, removing duplicates (unless ALL specified)
    Union,
    /// INTERSECT - returns only rows that appear in both queries
    Intersect,
    /// EXCEPT - returns rows from left query that don't appear in right query (SQL standard)
    Except,
}
