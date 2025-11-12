//! Core execution logic for sqllogictest runner.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::executor::block_on;
use futures::{stream, Future, FutureExt, StreamExt};
use itertools::Itertools;

use crate::error_handling::{TestError, TestErrorKind, RecordKind, AnyError};
use crate::output::{RecordOutput, DBOutput, Normalizer, ColumnTypeValidator, Validator};
use crate::parser::*;
use crate::substitution::Substitution;
use crate::{ColumnType, Connections, MakeConnection};

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    async fn shutdown(&mut self);

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        std::thread::sleep(dur);
    }

    /// [`Runner`] calls this function to run a system command.
    ///
    /// The default implementation is `std::process::Command::output`, which is universal to any
    /// async runtime but would block the current thread. If you are running in tokio runtime, you
    /// should override this by `tokio::process::Command::output`.
    async fn run_command(mut command: Command) -> std::io::Result<std::process::Output> {
        command.output()
    }
}

/// The database to be tested.
pub trait DB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    fn shutdown(&mut self) {}

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }
}

/// Compat-layer for the new AsyncDB and DB trait
#[async_trait]
impl<D> AsyncDB for D
where
    D: DB + Send,
{
    type Error = D::Error;
    type ColumnType = D::ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        D::run(self, sql)
    }

    async fn shutdown(&mut self) {
        D::shutdown(self);
    }

    fn engine_name(&self) -> &str {
        D::engine_name(self)
    }
}

/// Decide whether a test file should be run. Useful for partitioning tests into multiple
/// parallel machines to speed up test runs.
pub trait Partitioner: Send + Sync + 'static {
    /// Returns true if the given file name matches the partition and should be run.
    fn matches(&self, file_name: &str) -> bool;
}

impl<F> Partitioner for F
where
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    fn matches(&self, file_name: &str) -> bool {
        self(file_name)
    }
}

/// The default partitioner matches all files.
pub fn default_partitioner(_file_name: &str) -> bool {
    true
}

#[derive(Default)]
pub(crate) struct RunnerLocals {
    /// The temporary directory. Test cases can use `__TEST_DIR__` to refer to this directory.
    /// Lazily initialized and cleaned up when dropped.
    test_dir: OnceLock<tempfile::TempDir>,
    /// Runtime variables for substitution.
    variables: BTreeMap<String, String>,
}

impl RunnerLocals {
    pub fn test_dir(&self) -> String {
        let test_dir = self
            .test_dir
            .get_or_init(|| tempfile::TempDir::new().expect("failed to create testdir"));
        test_dir.path().to_string_lossy().into_owned()
    }

    fn set_var(&mut self, key: String, value: String) {
        self.variables.insert(key, value);
    }

    pub fn get_var(&self, key: &str) -> Option<&String> {
        self.variables.get(key)
    }

    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.variables
    }
}

/// Sqllogictest runner.
pub struct Runner<D: AsyncDB, M: MakeConnection<Conn = D>> {
    conn: Connections<D, M>,
    // validator is used for validate if the result of query equals to expected.
    validator: Validator,
    // normalizer is used to normalize the result text
    normalizer: Normalizer,
    column_type_validator: ColumnTypeValidator<D::ColumnType>,
    partitioner: Arc<dyn Partitioner>,
    substitution_on: bool,
    sort_mode: Option<SortMode>,
    result_mode: Option<ResultMode>,
    /// 0 means never hashing
    hash_threshold: usize,
    /// Labels for condition `skipif` and `onlyif`.
    labels: HashSet<String>,
    /// Local variables/context for the runner.
    locals: RunnerLocals,
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Create a new test runner on the database, with the given connection maker.
    ///
    /// See [`MakeConnection`] for more details.
    pub fn new(make_conn: M) -> Self {
        Runner {
            validator: crate::output::default_validator,
            normalizer: crate::output::default_normalizer,
            column_type_validator: crate::output::default_column_validator,
            partitioner: Arc::new(default_partitioner),
            substitution_on: false,
            sort_mode: None,
            result_mode: None,
            hash_threshold: 0,
            labels: HashSet::new(),
            conn: Connections::new(make_conn),
            locals: RunnerLocals::default(),
        }
    }

    /// Add a label for condition `skipif` and `onlyif`.
    pub fn add_label(&mut self, label: &str) {
        self.labels.insert(label.to_string());
    }

    /// Set a local variable for substitution.
    pub fn set_var(&mut self, key: String, value: String) {
        self.locals.set_var(key, value);
    }

    pub fn with_normalizer(&mut self, normalizer: Normalizer) {
        self.normalizer = normalizer;
    }
    pub fn with_validator(&mut self, validator: Validator) {
        self.validator = validator;
    }

    pub fn with_column_validator(&mut self, validator: ColumnTypeValidator<D::ColumnType>) {
        self.column_type_validator = validator;
    }

    /// Set the partitioner for the runner. Only files that match the partitioner will be run.
    ///
    /// This only takes effect when running tests in parallel.
    pub fn with_partitioner(&mut self, partitioner: impl Partitioner + 'static) {
        self.partitioner = Arc::new(partitioner);
    }

    pub fn with_hash_threshold(&mut self, hash_threshold: usize) {
        self.hash_threshold = hash_threshold;
    }

    pub async fn apply_record(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> RecordOutput<D::ColumnType> {
        tracing::debug!(?record, "testing");
        /// Returns whether we should skip this record, according to given `conditions`.
        fn should_skip(
            labels: &HashSet<String>,
            engine_name: &str,
            conditions: &[Condition],
        ) -> bool {
            conditions.iter().any(|c| {
                c.should_skip(
                    labels
                        .iter()
                        .map(|l| l.as_str())
                        // attach the engine name to the labels
                        .chain(Some(engine_name).filter(|n| !n.is_empty())),
                )
            })
        }

        match record {
            Record::Statement {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected: _,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(error),
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(Arc::new(e)),
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let ret = conn.run(&sql).await;
                match ret {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => RecordOutput::Query {
                            types,
                            rows,
                            error: None,
                        },
                        DBOutput::StatementComplete(count) => {
                            RecordOutput::Statement { count, error: None }
                        }
                    },
                    Err(e) => RecordOutput::Statement {
                        count: 0,
                        error: Some(Arc::new(e)),
                    },
                }
            }
            Record::System {
                conditions,
                command,
                loc: _,
                stdout: expected_stdout,
                retry: _,
            } => {
                if should_skip(&self.labels, "", &conditions) {
                    return RecordOutput::Nothing;
                }

                let mut command = match self.may_substitute(command, false) {
                    Ok(command) => command,
                    Err(error) => {
                        return RecordOutput::System {
                            stdout: None,
                            error: Some(error),
                        }
                    }
                };

                let is_background = command.trim().ends_with('&');
                if is_background {
                    command = command.trim_end_matches('&').trim().to_string();
                }

                let mut cmd = if cfg!(target_os = "windows") {
                    let mut cmd = std::process::Command::new("cmd");
                    cmd.arg("/C").arg(&command);
                    cmd
                } else {
                    let mut cmd = std::process::Command::new("bash");
                    cmd.arg("-c").arg(&command);
                    cmd
                };

                if is_background {
                    // Spawn a new process, but don't wait for stdout, otherwise it will block until
                    // the process exits.
                    let error: Option<AnyError> = match cmd.spawn() {
                        Ok(_) => None,
                        Err(e) => Some(Arc::new(e)),
                    };
                    tracing::info!(target:"sqllogictest::system_command", command, "background system command spawned");
                    return RecordOutput::System {
                        error,
                        stdout: None,
                    };
                }

                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                let result = D::run_command(cmd).await;
                #[derive(thiserror::Error, Debug)]
                #[error(
                    "process exited unsuccessfully: {status}\nstdout: {stdout}\nstderr: {stderr}"
                )]
                struct SystemError {
                    status: std::process::ExitStatus,
                    stdout: String,
                    stderr: String,
                }

                let mut actual_stdout = None;
                let error: Option<AnyError> = match result {
                    Ok(std::process::Output {
                        status,
                        stdout,
                        stderr,
                    }) => {
                        let stdout = String::from_utf8_lossy(&stdout).to_string();
                        let stderr = String::from_utf8_lossy(&stderr).to_string();
                        tracing::info!(target:"sqllogictest::system_command", command, ?status, stdout, stderr, "system command executed");
                        if status.success() {
                            if expected_stdout.is_some() {
                                actual_stdout = Some(stdout);
                            }
                            None
                        } else {
                            Some(Arc::new(SystemError {
                                status,
                                stdout,
                                stderr,
                            }))
                        }
                    }
                    Err(error) => {
                        tracing::error!(target:"sqllogictest::system_command", command, ?error, "failed to run system command");
                        Some(Arc::new(error))
                    }
                };

                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                }
            }
            Record::Query {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Query {
                            error: Some(error),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let (types, mut rows) = match conn.run(&sql).await {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => (types, rows),
                        DBOutput::StatementComplete(count) => {
                            return RecordOutput::Statement { count, error: None };
                        }
                    },
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        };
                    }
                };

                let sort_mode = match expected {
                    QueryExpect::Results { sort_mode, .. } => sort_mode,
                    QueryExpect::Error(_) => None,
                }
                .or(self.sort_mode);

                let mut value_sort = false;
                match sort_mode {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        rows.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => {
                        rows = rows
                            .iter()
                            .flat_map(|row| row.iter())
                            .map(|s| vec![s.to_owned()])
                            .collect();
                        rows.sort_unstable();
                        value_sort = true;
                    }
                };

                let num_values = if value_sort {
                    rows.len()
                } else {
                    rows.len() * types.len()
                };

                if self.hash_threshold > 0 && num_values > self.hash_threshold {
                    let mut md5 = md5::Md5::new();
                    for line in &rows {
                        for value in line {
                            md5.update(value.as_bytes());
                            md5.update(b"\n");
                        }
                    }
                    let hash = format!("{:2x}", md5.finalize());
                    rows = vec![vec![format!(
                        "{} values hashing to {}",
                        rows.len() * rows[0].len(),
                        hash
                    )]];
                }

                RecordOutput::Query {
                    error: None,
                    types,
                    rows,
                }
            }
            Record::Sleep { duration, .. } => {
                D::sleep(duration).await;
                RecordOutput::Nothing
            }
            Record::Control(control) => {
                match control {
                    Control::SortMode(sort_mode) => {
                        self.sort_mode = Some(sort_mode);
                    }
                    Control::ResultMode(result_mode) => {
                        self.result_mode = Some(result_mode);
                    }
                    Control::Substitution(on_off) => self.substitution_on = on_off,
                }

                RecordOutput::Nothing
            }
            Record::HashThreshold { loc: _, threshold } => {
                self.hash_threshold = threshold as usize;
                RecordOutput::Nothing
            }
            Record::Halt { loc: _ } => {
                tracing::error!("halt record encountered. It's likely a bug of the runtime.");
                RecordOutput::Nothing
            }
            Record::Include { .. }
            | Record::Newline
            | Record::Comment(_)
            | Record::Subtest { .. }
            | Record::Injected(_)
            | Record::Condition(_)
            | Record::Connection(_) => RecordOutput::Nothing,
        }
    }

    /// Run a single record.
    pub async fn run_async(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let retry = match &record {
            Record::Statement { retry, .. } => retry.clone(),
            Record::Query { retry, .. } => retry.clone(),
            Record::System { retry, .. } => retry.clone(),
            _ => None,
        };
        if retry.is_none() {
            return self.run_async_no_retry(record).await;
        }

        // Retry for `retry.attempts` times. The parser ensures that `retry.attempts` must > 0.
        let retry = retry.unwrap();
        let mut last_error = None;
        for _ in 0..retry.attempts {
            let result = self.run_async_no_retry(record.clone()).await;
            if result.is_ok() {
                return result;
            }
            tracing::warn!(target:"sqllogictest::retry", backoff = ?retry.backoff, error = ?result, "retrying");
            D::sleep(retry.backoff).await;
            last_error = result.err();
        }

        Err(last_error.unwrap())
    }

    /// Run a single record without retry.
    async fn run_async_no_retry(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let result = self.apply_record(record.clone()).await;

        match (record, &result) {
            (_, RecordOutput::Nothing) => {}
            // Tolerate the mismatched return type...
            (
                Record::Statement {
                    sql, expected, loc, ..
                },
                RecordOutput::Query {
                    error: None, rows, ..
                },
            ) => {
                if let StatementExpect::Error(_) = expected {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc));
                }
                if let StatementExpect::Count(expected_count) = expected {
                    if expected_count != rows.len() as u64 {
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("returned {} rows", rows.len()),
                        }
                        .at(loc));
                    }
                }
            }
            (
                Record::Query {
                    loc, sql, expected, ..
                },
                RecordOutput::Statement { error: None, .. },
            ) => match expected {
                QueryExpect::Error(_) => {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc))
                }
                QueryExpect::Results { results, .. } if !results.is_empty() => {
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: results.join("\n"),
                        actual: "".to_string(),
                    }
                    .at(loc))
                }
                QueryExpect::Results { .. } => {}
            },
            (
                Record::Statement {
                    loc,
                    connection: _,
                    conditions: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Statement { count, error },
            ) => match (error, expected) {
                (None, StatementExpect::Error(_)) => {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Statement,
                    }
                    .at(loc))
                }
                (None, StatementExpect::Count(expected_count)) => {
                    if expected_count != *count {
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("affected {count} rows"),
                        }
                        .at(loc));
                    }
                }
                (None, StatementExpect::Ok) => {}
                (Some(e), StatementExpect::Error(expected_error)) => {
                    if !expected_error.is_match(&e.to_string()) {
                        return Err(TestErrorKind::ErrorMismatch {
                            sql,
                            err: Arc::clone(e),
                            expected_err: expected_error.to_string(),
                            kind: RecordKind::Statement,
                        }
                        .at(loc));
                    }
                }
                (Some(e), StatementExpect::Count(_) | StatementExpect::Ok) => {
                    return Err(TestErrorKind::Fail {
                        sql,
                        err: Arc::clone(e),
                        kind: RecordKind::Statement,
                    }
                    .at(loc));
                }
            },
            (
                Record::Query {
                    loc,
                    conditions: _,
                    connection: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Query { types, rows, error },
            ) => {
                match (error, expected) {
                    (None, QueryExpect::Error(_)) => {
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (Some(e), QueryExpect::Error(expected_error)) => {
                        if !expected_error.is_match(&e.to_string()) {
                            return Err(TestErrorKind::ErrorMismatch {
                                sql,
                                err: Arc::clone(e),
                                expected_err: expected_error.to_string(),
                                kind: RecordKind::Query,
                            }
                            .at(loc));
                        }
                    }
                    (Some(e), QueryExpect::Results { .. }) => {
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::clone(e),
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (
                        None,
                        QueryExpect::Results {
                            types: expected_types,
                            results: expected_results,
                            ..
                        },
                    ) => {
                        if !(self.column_type_validator)(types, &expected_types) {
                            return Err(TestErrorKind::QueryResultColumnsMismatch {
                                sql,
                                expected: expected_types.iter().map(|c| c.to_char()).join(""),
                                actual: types.iter().map(|c| c.to_char()).join(""),
                            }
                            .at(loc));
                        }

                        let actual_results = match self.result_mode {
                            Some(ResultMode::ValueWise) => rows
                                .iter()
                                .flat_map(|strs| strs.iter())
                                .map(|str| vec![str.to_string()])
                                .collect_vec(),
                            // default to rowwise
                            _ => rows.clone(),
                        };

                        if !(self.validator)(self.normalizer, &actual_results, &expected_results) {
                            let output_rows =
                                rows.iter().map(|strs| strs.iter().join(" ")).collect_vec();
                            return Err(TestErrorKind::QueryResultMismatch {
                                sql,
                                expected: expected_results.join("\n"),
                                actual: output_rows.join("\n"),
                            }
                            .at(loc));
                        }
                    }
                };
            }
            (
                Record::System {
                    loc,
                    conditions: _,
                    command,
                    stdout: expected_stdout,
                    retry: _,
                },
                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                },
            ) => {
                if let Some(err) = error {
                    return Err(TestErrorKind::SystemFail {
                        command,
                        err: Arc::clone(err),
                    }
                    .at(loc));
                }
                match (expected_stdout, actual_stdout) {
                    (None, _) => {}
                    (Some(expected_stdout), actual_stdout) => {
                        let actual_stdout = actual_stdout.clone().unwrap_or_default();
                        // TODO: support newlines contained in expected_stdout
                        if expected_stdout != actual_stdout.trim() {
                            return Err(TestErrorKind::SystemStdoutMismatch {
                                command,
                                expected_stdout,
                                actual_stdout,
                            }
                            .at(loc));
                        }
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(result)
    }

    /// Run a single record.
    ///
    /// Returns the output of the record if successful.
    pub fn run(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        block_on(self.run_async(record))
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run_async` for each record instead.
    pub async fn run_multi_async(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            self.run_async(record).await?;
        }
        Ok(())
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run` for each record instead.
    pub fn run_multi(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        block_on(self.run_multi_async(records))
    }

    /// Run a sqllogictest script.
    pub async fn run_script_async(&mut self, script: &str) -> Result<(), TestError> {
        let records = parse(script).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script with a given script name.
    pub async fn run_script_with_name_async(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        let records = parse_with_name(script, name).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest file.
    pub async fn run_file_async(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        let records = parse_file(filename)?;
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script.
    pub fn run_script(&mut self, script: &str) -> Result<(), TestError> {
        block_on(self.run_script_async(script))
    }

    /// Run a sqllogictest script with a given script name.
    pub fn run_script_with_name(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        block_on(self.run_script_with_name_async(script, name))
    }

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        block_on(self.run_file_async(filename))
    }

    /// accept the tasks, spawn jobs task to run slt test. the tasks are (AsyncDB, slt filename)
    /// pairs.
    // TODO: This is not a good interface, as the `make_conn` passed to `new` is unused but we
    // accept a new `conn_builder` here. May change `MakeConnection` to support specifying the
    // database name in the future.
    pub async fn run_parallel_async<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), crate::error_handling::ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        let files = glob::glob(glob).expect("failed to read glob pattern");
        let mut tasks = vec![];

        for (idx, file) in files.enumerate() {
            // for every slt file, we create a database against table conflict
            let file = file.unwrap();
            let filename = file.to_str().expect("not a UTF-8 filename");

            // Skip files that don't match the partitioner.
            if !self.partitioner.matches(filename) {
                continue;
            }

            let db_name = filename.replace([' ', '.', '-', '/'], "_");

            self.conn
                .run_default(&format!("CREATE DATABASE {db_name};"))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();

            let mut locals = RunnerLocals::default();
            locals.set_var("__DATABASE__".to_owned(), db_name.clone());

            let mut tester = Runner {
                conn: Connections::new(move || {
                    conn_builder(target.clone(), db_name.clone()).map(Ok)
                }),
                validator: self.validator,
                normalizer: self.normalizer,
                column_type_validator: self.column_type_validator,
                partitioner: self.partitioner.clone(),
                substitution_on: self.substitution_on,
                sort_mode: self.sort_mode,
                result_mode: self.result_mode,
                hash_threshold: self.hash_threshold,
                labels: self.labels.clone(),
                locals,
            };

            tasks.push(async move {
                let filename = file.to_string_lossy().to_string();
                tester.run_file_async(filename).await
            })
        }

        let tasks = stream::iter(tasks).buffer_unordered(jobs);
        let errors: Vec<_> = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error_handling::ParallelTestError { errors })
        }
    }

    /// sync version of `run_parallel_async`
    pub fn run_parallel<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), crate::error_handling::ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        block_on(self.run_parallel_async(glob, hosts, conn_builder, jobs))
    }

    /// Substitute the input SQL or command with [`Substitution`], if enabled by `control
    /// substitution`.
    ///
    /// If `subst_env_vars`, we will use the `subst` crate to support extensive substitutions, incl.
    /// `$NAME`, `${NAME}`, `${NAME:default}`. The cost is that we will have to use escape
    /// characters, e.g., `\$` & `\\`.
    ///
    /// Otherwise, we just do simple string substitution for `__TEST_DIR__` and `__NOW__`.
    /// This is useful for `system` commands: The shell can do the environment variables, and we can
    /// write strings like `\n` without escaping.
    fn may_substitute(&self, input: String, subst_env_vars: bool) -> Result<String, AnyError> {
        if self.substitution_on {
            Substitution::new(&self.locals, subst_env_vars)
                .substitute(&input)
                .map_err(|e| Arc::new(e) as AnyError)
        } else {
            Ok(input)
        }
    }
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Shutdown all connections in the runner.
    pub async fn shutdown_async(&mut self) {
        tracing::debug!("shutting down runner...");
        self.conn.shutdown_all().await;
    }

    /// Shutdown all connections in the runner.
    pub fn shutdown(&mut self) {
        block_on(self.shutdown_async());
    }
}
