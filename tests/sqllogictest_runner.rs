//! SQLLogicTest integration for comprehensive SQL correctness testing.

use async_trait::async_trait;
use executor::SelectExecutor;
use md5::{Md5, Digest};
use parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use storage::Database;
use types::SqlValue;

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

struct NistMemSqlDB {
    db: Database,
}

impl NistMemSqlDB {
    fn new() -> Self {
        Self { db: Database::new() }
    }

    /// Format and optionally hash result rows
    /// Returns either the full result set or an MD5 hash for large results (>8 values)
    fn format_result_rows(
        &self,
        rows: &[storage::Row],
        types: Vec<DefaultColumnType>
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let mut formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(idx, val)| self.format_sql_value(val, types.get(idx)))
                    .collect()
            })
            .collect();

        // Sort rows for consistent ordering (required for hashing and rowsort)
        formatted_rows.sort_by(|a, b| a.join(" ").cmp(&b.join(" ")));

        let total_values: usize = formatted_rows.iter().map(|r| r.len()).sum();

        // If there are many values, return hash instead of rows
        if total_values > 8 {
            let mut hasher = Md5::new();
            for row in &formatted_rows {
                hasher.update(row.join(" "));
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            Ok(DBOutput::Rows { types, rows: formatted_rows })
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let stmt =
            Parser::parse_sql(sql).map_err(|e| TestError(format!("Parse error: {:?}", e)))?;

        match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Insert(insert_stmt) => {
                let rows_affected = executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Update(update_stmt) => {
                let rows_affected = executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Delete(delete_stmt) => {
                let rows_affected = executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::DropTable(drop_stmt) => {
                executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::AlterTable(alter_stmt) => {
                executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateSchema(create_schema_stmt) => {
                executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropSchema(drop_schema_stmt) => {
                executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetSchema(set_schema_stmt) => {
                executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetCatalog(set_stmt) => {
                executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateIndex(create_index_stmt) => {
                executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropIndex(drop_index_stmt) => {
                executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetNames(set_stmt) => {
                executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetTimeZone(set_stmt) => {
                executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Grant(grant_stmt) => {
                executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Revoke(revoke_stmt) => {
                executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateRole(create_role_stmt) => {
                executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropRole(drop_role_stmt) => {
                executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateDomain(create_domain_stmt) => {
                executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropDomain(drop_domain_stmt) => {
                executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateType(create_type_stmt) => {
                executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropType(drop_type_stmt) => {
                executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateAssertion(create_assertion_stmt) => {
                executor::advanced_objects::execute_create_assertion(
                    &create_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropAssertion(drop_assertion_stmt) => {
                executor::advanced_objects::execute_drop_assertion(
                    &drop_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::BeginTransaction(_)
            | ast::Statement::Commit(_)
            | ast::Statement::Rollback(_)
            | ast::Statement::Savepoint(_)
            | ast::Statement::RollbackToSavepoint(_)
            | ast::Statement::ReleaseSavepoint(_)
            | ast::Statement::SetTransaction(_)
            | ast::Statement::CreateSequence(_)
            | ast::Statement::DropSequence(_)
            | ast::Statement::AlterSequence(_)
            | ast::Statement::CreateCollation(_)
            | ast::Statement::DropCollation(_)
            | ast::Statement::CreateCharacterSet(_)
            | ast::Statement::DropCharacterSet(_)
            | ast::Statement::CreateTranslation(_)
            | ast::Statement::DropTranslation(_)
            | ast::Statement::CreateView(_)
            | ast::Statement::DropView(_)
            | ast::Statement::CreateTrigger(_)
            | ast::Statement::DropTrigger(_)
            | ast::Statement::DeclareCursor(_)
            | ast::Statement::OpenCursor(_)
            | ast::Statement::Fetch(_)
            | ast::Statement::CloseCursor(_) => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn format_query_result(
        &self,
        rows: Vec<storage::Row>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        if rows.is_empty() {
            return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
        }

        let types: Vec<DefaultColumnType> = rows[0]
            .values
            .iter()
            .map(|val| match val {
                SqlValue::Integer(_) | SqlValue::Smallint(_) | SqlValue::Bigint(_) | SqlValue::Unsigned(_) => {
                    DefaultColumnType::Integer
                }
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => DefaultColumnType::FloatingPoint,
                SqlValue::Varchar(_)
                | SqlValue::Character(_)
                | SqlValue::Date(_)
                | SqlValue::Time(_)
                | SqlValue::Timestamp(_)
                | SqlValue::Interval(_) => DefaultColumnType::Text,
                SqlValue::Boolean(_) => DefaultColumnType::Integer,
                SqlValue::Null => DefaultColumnType::Any,
            })
            .collect();

        self.format_result_rows(&rows, types)
    }

    fn format_sql_value(&self, value: &SqlValue, expected_type: Option<&DefaultColumnType>) -> String {
        match value {
            SqlValue::Integer(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Smallint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Bigint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Unsigned(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Numeric(f) => f.to_string(),
            SqlValue::Float(f) | SqlValue::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Double(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Date(d)
            | SqlValue::Time(d)
            | SqlValue::Timestamp(d)
            | SqlValue::Interval(d) => d.clone(),
        }
    }
}

#[async_trait]
impl AsyncDB for NistMemSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // No cleanup needed for in-memory database
    }
}

#[tokio::test]
async fn test_basic_select() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
statement ok
CREATE TABLE test (x INTEGER, y INTEGER)

statement ok
INSERT INTO test VALUES (1, 2)

statement ok
INSERT INTO test VALUES (3, 4)

query II rowsort
SELECT * FROM test
----
1 2
3 4

query I
SELECT x FROM test WHERE y = 4
----
3
"#;

    tester.run_script(script).expect("Basic SELECT test should pass");
}

#[tokio::test]
async fn test_arithmetic() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
query I
SELECT 1 + 1
----
2

query I
SELECT 10 - 3
----
7

query I
SELECT 4 * 5
----
20
"#;

    tester.run_script(script).expect("Arithmetic test should pass");
}

// Issue #919: Reproduction test for infinite loop in IN subquery evaluation
// This test contains the exact query from slt_good_32.test line 780 that causes a hang
#[tokio::test]
async fn test_issue_919_in_subquery_hang() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
hash-threshold 8

statement ok
CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)

statement ok
INSERT INTO tab0 VALUES(0,1058,996.42,'dpqjl',1029,993.33,'ixhua')

statement ok
INSERT INTO tab0 VALUES(1,1060,1001.62,'xlshf',1030,998.70,'raykp')

statement ok
INSERT INTO tab0 VALUES(2,1061,1002.38,'cffkv',1031,1000.64,'oiimp')

statement ok
INSERT INTO tab0 VALUES(3,1062,1003.98,'rqvjo',1033,1001.43,'ymmtc')

statement ok
INSERT INTO tab0 VALUES(4,1063,1004.20,'bapcy',1034,1002.90,'cizha')

statement ok
INSERT INTO tab0 VALUES(5,1064,1005.45,'vlixf',1035,1003.57,'gxput')

statement ok
INSERT INTO tab0 VALUES(6,1065,1008.91,'evlsa',1036,1004.80,'ctyjb')

statement ok
INSERT INTO tab0 VALUES(7,1066,1009.64,'uaiby',1037,1005.89,'nwyak')

statement ok
INSERT INTO tab0 VALUES(8,1067,1010.72,'dyeih',1038,1006.22,'fwrms')

statement ok
INSERT INTO tab0 VALUES(9,1068,1011.34,'luoso',1039,1007.34,'fgwoy')

query I rowsort
SELECT pk FROM tab0 WHERE col3 >= 94 OR (col1 IN (63.39,21.7,52.63,42.27,35.11,72.69)) OR col3 > 30 AND col0 IN (SELECT col3 FROM tab0 WHERE col1 < 71.54) OR (col3 > 35)
----
10 values hashing to e20b902b49a98b1a05ed62804c757f94
"#;

    tester.run_script(script).expect("IN subquery test should pass");
}
