#[cfg(test)]
mod tests {
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
}
