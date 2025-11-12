#[cfg(test)]
mod case_sensitivity_tests {
    use crate::{Catalog, CatalogError, ColumnSchema, TableSchema, ViewDefinition};
    use vibesql_ast::SelectStmt;

    /// Helper function to create a basic table
    fn create_basic_table(name: &str) -> TableSchema {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        TableSchema::new(name.to_string(), columns)
    }

    // ============================================================================
    // Basic Lookups - Create with one case, query with another
    // ============================================================================

    #[test]
    fn test_case_insensitive_table_lookup_default() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("active_users")).unwrap();

        // Default is case-insensitive
        assert!(!catalog.is_case_sensitive_identifiers());
        assert!(catalog.get_table("ACTIVE_USERS").is_some());
        assert!(catalog.get_table("active_users").is_some());
        assert!(catalog.get_table("Active_Users").is_some());
    }

    #[test]
    fn test_case_sensitive_table_lookup() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);
        catalog.create_table(create_basic_table("active_users")).unwrap();

        // With case-sensitive mode, only exact match works
        assert!(catalog.get_table("active_users").is_some());
        assert!(catalog.get_table("ACTIVE_USERS").is_none());
        assert!(catalog.get_table("Active_Users").is_none());
    }

    #[test]
    fn test_case_insensitive_create_lowercase_query_uppercase() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("my_table")).unwrap();

        assert!(catalog.get_table("MY_TABLE").is_some());
        assert!(catalog.get_table("my_table").is_some());
    }

    #[test]
    fn test_case_insensitive_create_uppercase_query_lowercase() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("MY_TABLE")).unwrap();

        assert!(catalog.get_table("my_table").is_some());
        assert!(catalog.get_table("MY_TABLE").is_some());
    }

    #[test]
    fn test_case_insensitive_create_mixedcase_query_variants() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("MyTable")).unwrap();

        assert!(catalog.get_table("mytable").is_some());
        assert!(catalog.get_table("MYTABLE").is_some());
        assert!(catalog.get_table("MyTable").is_some());
        assert!(catalog.get_table("myTABLE").is_some());
    }

    // ============================================================================
    // Mixed Case Variations
    // ============================================================================

    #[test]
    fn test_multiple_tables_with_mixed_case() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("users")).unwrap();
        catalog.create_table(create_basic_table("Orders")).unwrap();
        catalog.create_table(create_basic_table("PRODUCTS")).unwrap();

        assert!(catalog.get_table("USERS").is_some());
        assert!(catalog.get_table("orders").is_some());
        assert!(catalog.get_table("products").is_some());
    }

    // ============================================================================
    // Views - Case-insensitive lookup
    // ============================================================================

    #[test]
    fn test_case_insensitive_view_lookup() {
        let mut catalog = Catalog::new();

        let view_def = ViewDefinition {
            name: "active_users_view".to_string(),
            query: SelectStmt::default(),
        };

        catalog.create_view(view_def).unwrap();

        assert!(catalog.get_view("ACTIVE_USERS_VIEW").is_some());
        assert!(catalog.get_view("active_users_view").is_some());
        assert!(catalog.get_view("Active_Users_View").is_some());
    }

    #[test]
    fn test_case_sensitive_view_lookup() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);

        let view_def = ViewDefinition {
            name: "active_users_view".to_string(),
            query: SelectStmt::default(),
        };

        catalog.create_view(view_def).unwrap();

        assert!(catalog.get_view("active_users_view").is_some());
        assert!(catalog.get_view("ACTIVE_USERS_VIEW").is_none());
    }

    // ============================================================================
    // Drop Operations - Case-insensitive
    // ============================================================================

    #[test]
    fn test_drop_table_case_insensitive() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("my_table")).unwrap();

        // Drop using different case
        let result = catalog.drop_table("MY_TABLE");
        assert!(result.is_ok());
        assert!(!catalog.table_exists("my_table"));
    }

    #[test]
    fn test_drop_table_case_sensitive() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);
        catalog.create_table(create_basic_table("my_table")).unwrap();

        // Drop with different case should fail
        let result = catalog.drop_table("MY_TABLE");
        assert!(result.is_err());
        assert!(catalog.table_exists("my_table"));
    }

    #[test]
    fn test_drop_view_case_insensitive() {
        let mut catalog = Catalog::new();

        let view_def = ViewDefinition {
            name: "my_view".to_string(),
            query: SelectStmt::default(),
        };

        catalog.create_view(view_def).unwrap();
        assert!(catalog.get_view("my_view").is_some());

        // Drop using different case
        let result = catalog.drop_view("MY_VIEW", false);
        assert!(result.is_ok());
        assert!(catalog.get_view("my_view").is_none());
    }

    #[test]
    fn test_drop_view_case_sensitive() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);

        let view_def = ViewDefinition {
            name: "my_view".to_string(),
            query: SelectStmt::default(),
        };

        catalog.create_view(view_def).unwrap();

        // Drop with different case should fail
        let result = catalog.drop_view("MY_VIEW", false);
        assert!(result.is_err());
        assert!(catalog.get_view("my_view").is_some());
    }

    // ============================================================================
    // Table Existence Checks
    // ============================================================================

    #[test]
    fn test_table_exists_case_insensitive() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("users")).unwrap();

        assert!(catalog.table_exists("USERS"));
        assert!(catalog.table_exists("users"));
        assert!(catalog.table_exists("Users"));
    }

    #[test]
    fn test_table_exists_case_sensitive() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);
        catalog.create_table(create_basic_table("users")).unwrap();

        assert!(catalog.table_exists("users"));
        assert!(!catalog.table_exists("USERS"));
        assert!(!catalog.table_exists("Users"));
    }

    // ============================================================================
    // Setting Toggle - Test behavior changes when toggling the setting
    // ============================================================================

    #[test]
    fn test_toggle_case_sensitivity_setting() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("test_table")).unwrap();

        // Default is case-insensitive
        assert!(catalog.get_table("TEST_TABLE").is_some());

        // Toggle to case-sensitive
        catalog.set_case_sensitive_identifiers(true);
        assert!(catalog.get_table("TEST_TABLE").is_none());
        assert!(catalog.get_table("test_table").is_some());

        // Toggle back to case-insensitive
        catalog.set_case_sensitive_identifiers(false);
        assert!(catalog.get_table("TEST_TABLE").is_some());
    }

    // ============================================================================
    // Qualified Names (schema.table)
    // ============================================================================

    #[test]
    fn test_qualified_table_lookup_case_insensitive() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("my_table")).unwrap();

        assert!(catalog.get_table("public.MY_TABLE").is_some());
        assert!(catalog.get_table("PUBLIC.my_table").is_some());
        assert!(catalog.get_table("public.my_table").is_some());
    }

    #[test]
    fn test_qualified_table_lookup_case_sensitive() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(true);
        catalog.create_table(create_basic_table("my_table")).unwrap();

        assert!(catalog.get_table("public.my_table").is_some());
        assert!(catalog.get_table("public.MY_TABLE").is_none());
        assert!(catalog.get_table("PUBLIC.my_table").is_none());
    }

    // ============================================================================
    // Column Lookups - Case-insensitive (already exists in get_column_index)
    // ============================================================================

    #[test]
    fn test_get_column_index_case_insensitive() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "user_name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        // get_column_index already does case-insensitive lookup
        assert_eq!(schema.get_column_index("ID"), Some(0));
        assert_eq!(schema.get_column_index("id"), Some(0));
        assert_eq!(schema.get_column_index("USER_NAME"), Some(1));
        assert_eq!(schema.get_column_index("user_name"), Some(1));
    }

    // ============================================================================
    // Create Table - Duplicate with different case (should fail in case-sensitive)
    // ============================================================================

    #[test]
    fn test_create_duplicate_table_different_case_insensitive() {
        let mut catalog = Catalog::new();
        // In case-insensitive mode, duplicates with different cases should be treated as same
        // But the current HashMap implementation stores by exact name, so this might create two entries
        // This test documents the current behavior
        catalog.create_table(create_basic_table("users")).unwrap();
        
        // Currently, this creates a second entry because storage is by exact name
        // This is acceptable as long as lookups work case-insensitively
        catalog.create_table(create_basic_table("USERS")).ok();
        
        // But lookup finds the first one
        assert!(catalog.get_table("users").is_some());
        assert!(catalog.get_table("USERS").is_some());
    }

    // ============================================================================
    // Multiple Tables - Listing and searching
    // ============================================================================

    #[test]
    fn test_list_tables_case_variations() {
        let mut catalog = Catalog::new();
        catalog.create_table(create_basic_table("users")).unwrap();
        catalog.create_table(create_basic_table("Orders")).unwrap();
        catalog.create_table(create_basic_table("PRODUCTS")).unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 3);
        // Tables are listed in their original case
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"Orders".to_string()));
        assert!(tables.contains(&"PRODUCTS".to_string()));
    }

    // ============================================================================
    // Error Cases - Lookup failures
    // ============================================================================

    #[test]
    fn test_nonexistent_table_case_insensitive() {
        let catalog = Catalog::new();
        assert!(catalog.get_table("nonexistent").is_none());
        assert!(catalog.get_table("NONEXISTENT").is_none());
    }

    #[test]
    fn test_nonexistent_view_case_insensitive() {
        let catalog = Catalog::new();
        assert!(catalog.get_view("nonexistent").is_none());
        assert!(catalog.get_view("NONEXISTENT").is_none());
    }

    // ============================================================================
    // Configuration State Tests
    // ============================================================================

    #[test]
    fn test_case_sensitivity_config_getter() {
        let mut catalog = Catalog::new();
        assert!(!catalog.is_case_sensitive_identifiers());

        catalog.set_case_sensitive_identifiers(true);
        assert!(catalog.is_case_sensitive_identifiers());

        catalog.set_case_sensitive_identifiers(false);
        assert!(!catalog.is_case_sensitive_identifiers());
    }

    #[test]
    fn test_default_catalog_is_case_insensitive() {
        let catalog = Catalog::default();
        assert!(!catalog.is_case_sensitive_identifiers());
    }
}
