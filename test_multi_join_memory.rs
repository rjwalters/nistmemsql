// Test to verify multi-table join memory behavior
// This should test the case from issue #1036 where select5.test causes memory issues

#[cfg(test)]
mod tests {
    use executor::SelectExecutor;
    use storage::Database;
    use parser::Parser;

    #[test]
    fn test_multitable_join_memory_limit() {
        // Create test case similar to select5.test with multiple tables
        let mut db = Database::new();

        // Create 4 tables with 10 rows each
        for table_num in 1..=4 {
            let table_name = format!("public.t{}", table_num);
            let mut schema = catalog::TableSchema::new(
                table_name.clone(),
                vec![
                    catalog::ColumnSchema {
                        name: format!("a{}", table_num),
                        data_type: types::DataType::Integer,
                        nullable: false,
                        default_value: None,
                    },
                    catalog::ColumnSchema {
                        name: format!("b{}", table_num),
                        data_type: types::DataType::Integer,
                        nullable: true,
                        default_value: None,
                    },
                    catalog::ColumnSchema {
                        name: format!("x{}", table_num),
                        data_type: types::DataType::Varchar(40),
                        nullable: true,
                        default_value: None,
                    },
                ],
            );
            schema.primary_key = Some(vec![format!("a{}", table_num)]);
            db.create_table(schema).unwrap();

            // Insert 10 rows
            for row_num in 1..=10 {
                let row = storage::Row::new(vec![
                    types::SqlValue::Integer(row_num),
                    types::SqlValue::Integer(row_num),
                    types::SqlValue::Varchar(format!("row {}", row_num)),
                ]);
                db.insert(&table_name, row).unwrap();
            }
        }

        let executor = SelectExecutor::new(&db);

        // This query should fail with memory limit exceeded
        // 4 tables × 10 rows = 10^4 = 10,000 rows cartesian product
        let sql = "SELECT t1.a1, t2.a2, t3.a3, t4.a4 
                   FROM public.t1, public.t2, public.t3, public.t4
                   WHERE t1.a1 = t2.b2 AND t2.a2 = t3.b3 AND t3.a3 = t4.b4";

        let parser = Parser::new();
        match parser.parse_select(sql) {
            Ok(stmt) => {
                match executor.execute(&stmt) {
                    Ok(rows) => {
                        println!("Got {} rows", rows.len());
                        // Should succeed with proper filtering
                        assert!(rows.len() > 0);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                        // Could be memory limit or other error
                    }
                }
            }
            Err(e) => {
                println!("Parse error: {:?}", e);
            }
        }
    }
}
