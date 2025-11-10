//! Tests for query timeout enforcement

#[cfg(test)]
mod tests {
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use crate::{limits::MAX_QUERY_EXECUTION_SECONDS, SelectExecutor};

    #[test]
    fn test_executor_initialized_with_timeout() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Verify that executor has timeout configured
        assert_eq!(executor.timeout_seconds, MAX_QUERY_EXECUTION_SECONDS);
        assert!(executor.timeout_seconds > 0, "Timeout should be positive");
    }

    #[test]
    fn test_simple_query_succeeds_within_timeout() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Simple query that should execute quickly
        let sql = "SELECT 1";
        let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");

        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let result = executor.execute(&select_stmt);
                assert!(result.is_ok(), "Simple query should succeed within timeout: {:?}", result);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_timeout_check_method_works() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Timeout check should pass immediately after executor creation
        let result = executor.check_timeout();
        assert!(result.is_ok(), "Timeout check should pass immediately: {:?}", result);
    }

    #[test]
    fn test_executor_with_outer_context_has_timeout() {
        let db = Database::new();
        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
        let combined_schema =
            crate::schema::CombinedSchema::from_table("".to_string(), empty_schema);
        let empty_row = vibesql_storage::Row::new(vec![]);

        let executor = SelectExecutor::new_with_outer_context(&db, &empty_row, &combined_schema);

        // Verify that executor with outer context also has timeout configured
        assert_eq!(executor.timeout_seconds, MAX_QUERY_EXECUTION_SECONDS);
    }
}
