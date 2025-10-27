#[cfg(test)]
mod catalog_tests {
    use crate::{Catalog, CatalogError, ColumnSchema, TableSchema};

    #[test]
    fn test_column_schema_creation() {
        let col = ColumnSchema::new("id".to_string(), types::DataType::Integer, false);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, types::DataType::Integer);
        assert!(!col.nullable);
    }

    #[test]
    fn test_table_schema_creation() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);
        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 2);
    }

    #[test]
    fn test_table_schema_get_column() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        let id_col = schema.get_column("id");
        assert!(id_col.is_some());
        assert_eq!(id_col.unwrap().data_type, types::DataType::Integer);

        let missing_col = schema.get_column("missing");
        assert!(missing_col.is_none());
    }

    #[test]
    fn test_table_schema_get_column_index() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        assert_eq!(schema.get_column_index("id"), Some(0));
        assert_eq!(schema.get_column_index("name"), Some(1));
        assert_eq!(schema.get_column_index("missing"), None);
    }

    #[test]
    fn test_catalog_create_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        let result = catalog.create_table(schema);
        assert!(result.is_ok());
        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_create_duplicate_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema1 = TableSchema::new("users".to_string(), columns.clone());
        let schema2 = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema1).unwrap();
        let result = catalog.create_table(schema2);

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableAlreadyExists(name) => assert_eq!(name, "users"),
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_catalog_get_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();

        let retrieved = catalog.get_table("users");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "users");

        let missing = catalog.get_table("missing");
        assert!(missing.is_none());
    }

    #[test]
    fn test_catalog_drop_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();
        assert!(catalog.table_exists("users"));

        let result = catalog.drop_table("users");
        assert!(result.is_ok());
        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_drop_nonexistent_table() {
        let mut catalog = Catalog::new();
        let result = catalog.drop_table("missing");

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableNotFound(name) => assert_eq!(name, "missing"),
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_catalog_list_tables() {
        let mut catalog = Catalog::new();

        let schema1 = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        let schema2 = TableSchema::new(
            "orders".to_string(),
            vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );

        catalog.create_table(schema1).unwrap();
        catalog.create_table(schema2).unwrap();

        let mut tables = catalog.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["orders".to_string(), "users".to_string()]);
    }

    #[test]
    fn test_error_display_table_already_exists() {
        let error = CatalogError::TableAlreadyExists("customers".to_string());
        let error_msg = format!("{}", error);
        assert_eq!(error_msg, "Table 'customers' already exists");
    }

    #[test]
    fn test_error_display_table_not_found() {
        let error = CatalogError::TableNotFound("products".to_string());
        let error_msg = format!("{}", error);
        assert_eq!(error_msg, "Table 'products' not found");
    }

    #[test]
    fn test_catalog_default() {
        let catalog = Catalog::default();
        assert_eq!(catalog.list_tables().len(), 0);
        assert!(!catalog.table_exists("any_table"));
    }

    #[test]
    fn test_column_schema_clone() {
        let col1 = ColumnSchema::new("id".to_string(), types::DataType::Integer, false);
        let col2 = col1.clone();
        assert_eq!(col1, col2);
        assert_eq!(col1.name, col2.name);
        assert_eq!(col1.data_type, col2.data_type);
        assert_eq!(col1.nullable, col2.nullable);
    }

    #[test]
    fn test_table_schema_clone() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                types::DataType::Varchar { max_length: 255 },
                true,
            ),
        ];
        let schema1 = TableSchema::new("accounts".to_string(), columns);
        let schema2 = schema1.clone();
        assert_eq!(schema1, schema2);
        assert_eq!(schema1.name, schema2.name);
        assert_eq!(schema1.columns.len(), schema2.columns.len());
    }

    #[test]
    fn test_catalog_clone() {
        let mut catalog1 = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);
        catalog1.create_table(schema).unwrap();

        let catalog2 = catalog1.clone();
        assert!(catalog2.table_exists("users"));
        assert_eq!(catalog1.list_tables(), catalog2.list_tables());
    }
}
