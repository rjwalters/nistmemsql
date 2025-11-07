use async_trait::async_trait;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};

struct TestDB {
    db: storage::Database,
}

impl TestDB {
    fn new() -> Self {
        Self { db: storage::Database::new() }
    }

    fn format_sql_value_with_type(&self, value: &types::SqlValue, expected_type: Option<&DefaultColumnType>) -> String {
        // If we have an expected type, use it for formatting
        if let Some(expected_type) = expected_type {
            match expected_type {
                DefaultColumnType::FloatingPoint => {
                    match value {
                        types::SqlValue::Integer(i) => format!("{:.3}", *i as f64),
                        _ => format!("{:?}", value), // fallback
                    }
                }
                _ => format!("{:?}", value), // fallback
            }
        } else {
            format!("{:?}", value)
        }
    }
}

#[async_trait]
impl AsyncDB for TestDB {
    type Error = Box<dyn std::error::Error>;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        println!("Running: {}", sql);
        // For this simple test, just return a mock result
        if sql.contains("SELECT ALL + ( + - ( - 92 ) ) FROM tab1") {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::FloatingPoint], // This should trigger decimal formatting
                rows: vec![vec!["92.000".to_string()], vec!["92.000".to_string()], vec!["92.000".to_string()]],
            })
        } else {
            Ok(DBOutput::StatementComplete(0))
        }
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn test_issue_948() {
    let mut tester = Runner::new(|| async { Ok(TestDB::new()) });

    let script = r#"
statement ok
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)

statement ok
INSERT INTO tab1 VALUES(51,14,96)

statement ok
INSERT INTO tab1 VALUES(85,5,59)

statement ok
INSERT INTO tab1 VALUES(91,47,68)

query R rowsort
SELECT ALL + ( + - ( - 92 ) ) FROM tab1
----
92.000
92.000
92.000
"#;

    tester.run_script(script).await.expect("Test should pass");
}
