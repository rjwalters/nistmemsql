//! Abstract Syntax Tree (AST) for SQL:1999
//!
//! This crate defines the structure of SQL statements and expressions
//! as parsed from SQL text. The AST is a tree representation that
//! preserves the semantic structure of SQL queries.

mod ddl;
mod dml;
mod expression;
mod operators;
mod select;
mod statement;

pub use ddl::{ColumnDef, CreateTableStmt};
pub use dml::{Assignment, DeleteStmt, InsertStmt, UpdateStmt};
pub use expression::Expression;
pub use operators::{BinaryOperator, UnaryOperator};
pub use select::{FromClause, JoinType, OrderByItem, OrderDirection, SelectItem, SelectStmt};
pub use statement::Statement;

#[cfg(test)]
mod tests {
    use super::*;
    use types::SqlValue;

    // ============================================================================
    // Statement Tests - Top-level SQL statements
    // ============================================================================

    #[test]
    fn test_create_select_statement() {
        let stmt = Statement::Select(SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Wildcard],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        });

        match stmt {
            Statement::Select(_) => {} // Success
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_create_insert_statement() {
        let stmt = Statement::Insert(InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["name".to_string()],
            values: vec![vec![Expression::Literal(SqlValue::Varchar("Alice".to_string()))]],
        });

        match stmt {
            Statement::Insert(_) => {} // Success
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_create_update_statement() {
        let stmt = Statement::Update(UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Expression::Literal(SqlValue::Varchar("Bob".to_string())),
            }],
            where_clause: None,
        });

        match stmt {
            Statement::Update(_) => {} // Success
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_create_delete_statement() {
        let stmt =
            Statement::Delete(DeleteStmt { table_name: "users".to_string(), where_clause: None });

        match stmt {
            Statement::Delete(_) => {} // Success
            _ => panic!("Expected Delete statement"),
        }
    }

    // ============================================================================
    // Expression Tests - SQL expressions
    // ============================================================================

    #[test]
    fn test_literal_integer_expression() {
        let expr = Expression::Literal(SqlValue::Integer(42));
        match expr {
            Expression::Literal(SqlValue::Integer(42)) => {} // Success
            _ => panic!("Expected integer literal"),
        }
    }

    #[test]
    fn test_literal_string_expression() {
        let expr = Expression::Literal(SqlValue::Varchar("hello".to_string()));
        match expr {
            Expression::Literal(SqlValue::Varchar(s)) if s == "hello" => {} // Success
            _ => panic!("Expected string literal"),
        }
    }

    #[test]
    fn test_column_reference_expression() {
        let expr = Expression::ColumnRef { table: None, column: "id".to_string() };

        match expr {
            Expression::ColumnRef { table: None, column } if column == "id" => {} // Success
            _ => panic!("Expected column reference"),
        }
    }

    #[test]
    fn test_qualified_column_reference() {
        let expr =
            Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };

        match expr {
            Expression::ColumnRef { table: Some(t), column: c } if t == "users" && c == "id" => {} // Success
            _ => panic!("Expected qualified column reference"),
        }
    }

    #[test]
    fn test_binary_operation_addition() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };

        match expr {
            Expression::BinaryOp { op: BinaryOperator::Plus, .. } => {} // Success
            _ => panic!("Expected addition operation"),
        }
    }

    #[test]
    fn test_binary_operation_equality() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };

        match expr {
            Expression::BinaryOp { op: BinaryOperator::Equal, .. } => {} // Success
            _ => panic!("Expected equality operation"),
        }
    }

    #[test]
    fn test_function_call_count_star() {
        let expr =
            Expression::Function { name: "COUNT".to_string(), args: vec![Expression::Wildcard] };

        match expr {
            Expression::Function { name, .. } if name == "COUNT" => {} // Success
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_is_null_predicate() {
        let expr = Expression::IsNull {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            negated: false,
        };

        match expr {
            Expression::IsNull { negated: false, .. } => {} // Success
            _ => panic!("Expected IS NULL predicate"),
        }
    }

    #[test]
    fn test_is_not_null_predicate() {
        let expr = Expression::IsNull {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            negated: true,
        };

        match expr {
            Expression::IsNull { negated: true, .. } => {} // Success
            _ => panic!("Expected IS NOT NULL predicate"),
        }
    }

    // ============================================================================
    // SelectStmt Tests - SELECT statement structure
    // ============================================================================

    #[test]
    fn test_select_star() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Wildcard],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        assert_eq!(select.select_list.len(), 1);
        match &select.select_list[0] {
            SelectItem::Wildcard => {} // Success
            _ => panic!("Expected wildcard"),
        }
    }

    #[test]
    fn test_select_with_columns() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                    alias: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                },
            ],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        assert_eq!(select.select_list.len(), 2);
    }

    #[test]
    fn test_select_with_alias() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: Some("user_id".to_string()),
            }],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        match &select.select_list[0] {
            SelectItem::Expression { alias: Some(a), .. } if a == "user_id" => {} // Success
            _ => panic!("Expected aliased expression"),
        }
    }

    #[test]
    fn test_select_from_table() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Wildcard],
            from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        match &select.from {
            Some(FromClause::Table { name, .. }) if name == "users" => {} // Success
            _ => panic!("Expected table in FROM clause"),
        }
    }

    #[test]
    fn test_select_with_where() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Wildcard],
            from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        assert!(select.where_clause.is_some());
    }

    #[test]
    fn test_select_with_order_by() {
        let select = SelectStmt {
            distinct: false,
            select_list: vec![SelectItem::Wildcard],
            from: Some(FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![OrderByItem {
                expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                direction: OrderDirection::Asc,
            }]),
            limit: None,
            offset: None,
        };

        assert!(select.order_by.is_some());
        assert_eq!(select.order_by.as_ref().unwrap().len(), 1);
    }

    // ============================================================================
    // BinaryOperator Tests - All SQL operators
    // ============================================================================

    #[test]
    fn test_arithmetic_operators() {
        let _plus = BinaryOperator::Plus;
        let _minus = BinaryOperator::Minus;
        let _multiply = BinaryOperator::Multiply;
        let _divide = BinaryOperator::Divide;
        // If these compile, the operators exist
    }

    #[test]
    fn test_comparison_operators() {
        let _eq = BinaryOperator::Equal;
        let _ne = BinaryOperator::NotEqual;
        let _lt = BinaryOperator::LessThan;
        let _le = BinaryOperator::LessThanOrEqual;
        let _gt = BinaryOperator::GreaterThan;
        let _ge = BinaryOperator::GreaterThanOrEqual;
        // If these compile, the operators exist
    }

    #[test]
    fn test_logical_operators() {
        let _and = BinaryOperator::And;
        let _or = BinaryOperator::Or;
        // If these compile, the operators exist
    }
}
