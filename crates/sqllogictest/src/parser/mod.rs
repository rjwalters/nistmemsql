//! Sqllogictest parser.

use std::fmt;
use std::path::Path;

use itertools::Itertools;

pub mod location;
pub mod error_parser;
pub mod retry_parser;
pub mod directive_parser;
pub mod record_parser;

pub use self::location::Location;
pub use self::error_parser::ExpectedError;
pub use self::retry_parser::RetryConfig;
pub use self::directive_parser::{Control, Condition, Connection, SortMode, ResultMode, ControlItem};
pub use self::record_parser::{StatementExpect, QueryExpect};

use crate::ColumnType;
use self::record_parser::{parse_lines, parse_multiple_result, parse_multiline_error};
use self::retry_parser::parse_retry_config;

const RESULTS_DELIMITER: &str = "----";

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Record<T: ColumnType> {
    /// An include copies all records from another files.
    Include {
        loc: Location,
        /// A glob pattern
        filename: String,
    },
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: StatementExpect,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: QueryExpect<T>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A system command is an external command that is to be executed by the shell. Currently it
    /// must succeed and the output is ignored.
    #[non_exhaustive]
    System {
        loc: Location,
        conditions: Vec<Condition>,
        /// The external command.
        command: String,
        stdout: Option<String>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A sleep period.
    Sleep {
        loc: Location,
        duration: std::time::Duration,
    },
    /// Subtest.
    Subtest {
        loc: Location,
        name: String,
    },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt {
        loc: Location,
    },
    /// Control statements.
    Control(Control),
    /// Set the maximum number of result values that will be accepted
    /// for a query.  If the number of result values exceeds this number,
    /// then an MD5 hash is computed of all values, and the resulting hash
    /// is the only result.
    ///
    /// If the threshold is 0, then hashing is never used.
    HashThreshold {
        loc: Location,
        threshold: u64,
    },
    /// Condition statements, including `onlyif` and `skipif`.
    Condition(Condition),
    /// Connection statements to specify the connection to use for the following statement.
    Connection(Connection),
    Comment(Vec<String>),
    Newline,
    /// Internally injected record which should not occur in the test file.
    Injected(Injected),
}

impl<T: ColumnType> Record<T> {
    /// Unparses the record to its string representation in the test file.
    ///
    /// # Panics
    /// If the record is an internally injected record which should not occur in the test file.
    pub fn unparse(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(w, "{self}")
    }
}

/// As is the standard for Display, does not print any trailing
/// newline except for records that always end with a blank line such
/// as Query and Statement.
impl<T: ColumnType> std::fmt::Display for Record<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Record::Include { loc: _, filename } => {
                write!(f, "include {filename}")
            }
            Record::Statement {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                record_parser::fmt_statement(f, sql, expected, retry)
            }
            Record::Query {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                record_parser::fmt_query(f, sql, expected, retry)
            }
            Record::System {
                loc: _,
                conditions: _,
                command,
                stdout,
                retry,
            } => {
                record_parser::fmt_system(f, command, stdout, retry)
            }
            Record::Sleep { loc: _, duration } => {
                write!(f, "sleep {}", humantime::format_duration(*duration))
            }
            Record::Subtest { loc: _, name } => {
                write!(f, "subtest {name}")
            }
            Record::Halt { loc: _ } => {
                write!(f, "halt")
            }
            Record::Control(c) => match c {
                Control::SortMode(m) => write!(f, "control sortmode {}", m.as_str()),
                Control::ResultMode(m) => write!(f, "control resultmode {}", m.as_str()),
                Control::Substitution(s) => write!(f, "control substitution {}", s.as_str()),
            },
            Record::Condition(cond) => match cond {
                Condition::OnlyIf { label } => write!(f, "onlyif {label}"),
                Condition::SkipIf { label } => write!(f, "skipif {label}"),
            },
            Record::Connection(conn) => {
                if let Connection::Named(conn) = conn {
                    write!(f, "connection {}", conn)?;
                }
                Ok(())
            }
            Record::HashThreshold { loc: _, threshold } => {
                write!(f, "hash-threshold {threshold}")
            }
            Record::Comment(comment) => {
                let mut iter = comment.iter();
                write!(f, "#{}", iter.next().unwrap().trim_end())?;
                for line in iter {
                    write!(f, "\n#{}", line.trim_end())?;
                }
                Ok(())
            }
            Record::Newline => Ok(()), // Display doesn't end with newline
            Record::Injected(p) => panic!("unexpected injected record: {p:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Injected {
    /// Pseudo control command to indicate the begin of an include statement. Automatically
    /// injected by sqllogictest parser.
    BeginInclude(String),
    /// Pseudo control command to indicate the end of an include statement. Automatically injected
    /// by sqllogictest parser.
    EndInclude(String),
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
#[error("parse error at {loc}: {kind}")]
pub struct ParseError {
    kind: ParseErrorKind,
    loc: Location,
}

impl ParseError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum ParseErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type character: {0:?} in type string")]
    InvalidType(char),
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid error message: {0:?}")]
    InvalidErrorMessage(String),
    #[error("duplicated error messages after error` and under `----`")]
    DuplicatedErrorMessage,
    #[error("invalid retry config: {0:?}")]
    InvalidRetryConfig(String),
    #[error("statement should have no result, use `query` instead")]
    StatementHasResults,
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
    #[error("invalid include file pattern: {0}")]
    InvalidIncludeFile(String),
    #[error("no files found for include file pattern: {0:?}")]
    EmptyIncludeFile(String),
    #[error("no such file")]
    FileNotFound,
}

impl ParseErrorKind {
    pub(crate) fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse<T: ColumnType>(script: &str) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new("<unknown>", 0), script)
}

/// Parse a sqllogictest script into a list of records with a given script name.
pub fn parse_with_name<T: ColumnType>(
    script: &str,
    name: impl Into<std::sync::Arc<str>>,
) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new(name, 0), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner<T: ColumnType>(loc: &Location, script: &str) -> Result<Vec<Record<T>>, ParseError> {
    let mut lines = script.lines().enumerate().peekable();
    let mut records = vec![];
    let mut conditions = vec![];
    let mut connection = Connection::Default;
    let mut comments = vec![];

    while let Some((num, line)) = lines.next() {
        if let Some(text) = line.strip_prefix('#') {
            comments.push(text.to_string());
            if lines.peek().is_none() {
                // Special handling for the case where the last line is a comment.
                records.push(Record::Comment(comments));
                break;
            }
            continue;
        }
        if !comments.is_empty() {
            records.push(Record::Comment(comments));
            comments = vec![];
        }

        if line.is_empty() {
            records.push(Record::Newline);
            continue;
        }

        let mut loc = loc.clone();
        loc.line = num as u32 + 1;

        // Strip inline comments (lines starting with # are already handled above)
        let line_without_comment = if let Some(comment_pos) = line.find('#') {
            // Only treat # as a comment if it's preceded by whitespace
            // This prevents treating # inside other contexts as a comment marker
            if line[..comment_pos].ends_with(|c: char| c.is_whitespace()) {
                &line[..comment_pos]
            } else {
                line
            }
        } else {
            line
        };

        let tokens: Vec<&str> = line_without_comment.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    loc,
                    name: name.to_string(),
                });
            }
            ["sleep", dur] => {
                records.push(Record::Sleep {
                    duration: humantime::parse_duration(dur).map_err(|_| {
                        ParseErrorKind::InvalidDuration(dur.to_string()).at(loc.clone())
                    })?,
                    loc,
                });
            }
            ["skipif", label] => {
                let cond = Condition::SkipIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["onlyif", label] => {
                let cond = Condition::OnlyIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["connection", name] => {
                let conn = Connection::new(name);
                connection = conn.clone();
                records.push(Record::Connection(conn));
            }
            ["statement", res @ ..] => {
                let (mut expected, res) = match res {
                    ["ok", retry @ ..] => (StatementExpect::Ok, retry),
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `statement error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (StatementExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (StatementExpect::Error(error), &[][..])
                        }
                    }
                    ["count", count_str, retry @ ..] => {
                        let count = count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?;
                        (StatementExpect::Count(count), retry)
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                let (sql, has_results) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;

                if has_results {
                    if let StatementExpect::Error(e) = &mut expected {
                        // If no inline error message is specified, it might be a multiline error.
                        if e.is_empty() {
                            *e = parse_multiline_error(&mut lines);
                        } else {
                            return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                        }
                    } else {
                        return Err(ParseErrorKind::StatementHasResults.at(loc.clone()));
                    }
                }

                records.push(Record::Statement {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["query", res @ ..] => {
                let (mut expected, res) = match res {
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `query error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (QueryExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (QueryExpect::Error(error), &[][..])
                        }
                    }
                    [type_str, res @ ..] => {
                        // query <type-string> [<sort-mode>] [<label>] [retry <attempts> backoff <backoff>]
                        let types = type_str
                            .chars()
                            .map(|ch| {
                                T::from_char(ch)
                                    .ok_or_else(|| ParseErrorKind::InvalidType(ch).at(loc.clone()))
                            })
                            .try_collect()?;
                        let sort_mode = res.first().and_then(|&s| SortMode::try_from_str(s).ok()); // Could be `retry` or label

                        // To support `retry`, we assume the label must *not* be "retry"
                        let label_start = if sort_mode.is_some() { 1 } else { 0 };
                        let res = &res[label_start..];
                        let label = res.first().and_then(|&s| {
                            if s != "retry" {
                                Some(s.to_owned())
                            } else {
                                None // `retry` is not a valid label
                            }
                        });

                        let retry_start = if label.is_some() { 1 } else { 0 };
                        let res = &res[retry_start..];
                        (
                            QueryExpect::Results {
                                types,
                                sort_mode,
                                result_mode: None,
                                label,
                                results: Vec::new(),
                            },
                            res,
                        )
                    }
                    [] => (QueryExpect::empty_results(), &[][..]),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // The SQL for the query is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (sql, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                if has_result {
                    match &mut expected {
                        // Lines following the "----" are expected results of the query, one value
                        // per line.
                        QueryExpect::Results { results, .. } => {
                            for (_, line) in &mut lines {
                                if line.is_empty() {
                                    break;
                                }
                                results.push(line.to_string());
                            }
                        }
                        // If no inline error message is specified, it might be a multiline error.
                        QueryExpect::Error(e) => {
                            if e.is_empty() {
                                *e = parse_multiline_error(&mut lines);
                            } else {
                                return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                            }
                        }
                    }
                }
                records.push(Record::Query {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["system", "ok", res @ ..] => {
                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // TODO: we don't support asserting error message for system command
                // The command is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (command, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                let stdout = if has_result {
                    Some(parse_multiple_result(&mut lines))
                } else {
                    None
                };
                records.push(Record::System {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    command,
                    stdout,
                    retry,
                });
            }
            ["control", res @ ..] => match res {
                ["resultmode", result_mode] => match ResultMode::try_from_str(result_mode) {
                    Ok(result_mode) => {
                        records.push(Record::Control(Control::ResultMode(result_mode)))
                    }
                    Err(k) => return Err(k.at(loc)),
                },
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                ["substitution", on_off] => match bool::try_from_str(on_off) {
                    Ok(on_off) => records.push(Record::Control(Control::Substitution(on_off))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            ["hash-threshold", threshold] => {
                records.push(Record::HashThreshold {
                    loc: loc.clone(),
                    threshold: threshold.parse::<u64>().map_err(|_| {
                        ParseErrorKind::InvalidNumber((*threshold).into()).at(loc.clone())
                    })?,
                });
            }
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file. The included scripts are inserted after the `include` record.
pub fn parse_file<T: ColumnType>(filename: impl AsRef<Path>) -> Result<Vec<Record<T>>, ParseError> {
    let filename = filename.as_ref().to_str().unwrap();
    parse_file_inner(Location::new(filename, 0))
}

fn parse_file_inner<T: ColumnType>(loc: Location) -> Result<Vec<Record<T>>, ParseError> {
    let path = Path::new(loc.file());
    if !path.exists() {
        return Err(ParseErrorKind::FileNotFound.at(loc.clone()));
    }
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(&loc, &script)? {
        records.push(rec.clone());

        if let Record::Include { filename, loc } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            let mut iter = glob::glob(&complete_filename)
                .map_err(|e| ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone()))?
                .peekable();
            if iter.peek().is_none() {
                return Err(ParseErrorKind::EmptyIncludeFile(filename).at(loc.clone()));
            }
            for included_file in iter {
                let included_file = included_file.map_err(|e| {
                    ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone())
                })?;
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file_inner(loc.include(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::DefaultColumnType;

    #[test]
    fn test_trailing_comment() {
        let script = "\
# comment 1
#  comment 2
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(
            records,
            vec![Record::Comment(vec![
                " comment 1".to_string(),
                "  comment 2".to_string(),
            ]),]
        );
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_include_glob() {
        let records =
            parse_file::<DefaultColumnType>("../tests/slt/include/include_1.slt").unwrap();
        assert_eq!(15, records.len());
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_basic() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/basic.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_condition() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/condition.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_file_level_sort_mode() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/file_level_sort_mode.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_rowsort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/rowsort.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_valuesort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/valuesort.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_substitution() {
        parse_roundtrip::<DefaultColumnType>("../tests/substitution/basic.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_test_dir_escape() {
        parse_roundtrip::<DefaultColumnType>("../tests/test_dir_escape/test_dir_escape.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_validator() {
        parse_roundtrip::<DefaultColumnType>("../tests/validator/validator.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_custom_type() {
        parse_roundtrip::<CustomColumnType>("../tests/custom_type/custom_type.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_system_command() {
        parse_roundtrip::<DefaultColumnType>("../tests/system_command/system_command.slt")
    }

    #[test]
    fn test_fail_unknown_type() {
        let script = "\
query IA
select * from unknown_type
----
";

        let error_kind = parse::<CustomColumnType>(script).unwrap_err().kind;

        assert_eq!(error_kind, ParseErrorKind::InvalidType('A'));
    }

    #[test]
    fn test_parse_no_types() {
        let script = "\
query
select * from foo;
----
";
        let records = parse::<DefaultColumnType>(script).unwrap();

        assert_eq!(
            records,
            vec![Record::Query {
                loc: Location::new("<unknown>", 1),
                conditions: vec![],
                connection: Connection::Default,
                sql: "select * from foo;".to_string(),
                expected: QueryExpect::empty_results(),
                retry: None,
            }]
        );
    }

    /// Verifies Display impl is consistent with parsing by ensuring
    /// roundtrip parse(unparse(parse())) is consistent
    #[track_caller]
    fn parse_roundtrip<T: ColumnType>(filename: impl AsRef<Path>) {
        let filename = filename.as_ref();
        let records = parse_file::<T>(filename).expect("parsing to complete");

        let unparsed = records
            .iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>();

        let output_contents = unparsed.join("\n");

        // The original and parsed records should be logically equivalent
        let mut output_file = tempfile::NamedTempFile::new().expect("Error creating tempfile");
        output_file
            .write_all(output_contents.as_bytes())
            .expect("Unable to write file");
        output_file.flush().unwrap();

        let output_path = output_file.into_temp_path();
        let reparsed_records =
            parse_file(&output_path).expect("reparsing to complete successfully");

        let records = normalize_filename(records);
        let reparsed_records = normalize_filename(reparsed_records);

        pretty_assertions::assert_eq!(records, reparsed_records, "Mismatch in reparsed records");
    }

    /// Replaces the actual filename in all Records with
    /// "__FILENAME__" so different files with the same contents can
    /// compare equal
    fn normalize_filename<T: ColumnType>(records: Vec<Record<T>>) -> Vec<Record<T>> {
        records
            .into_iter()
            .map(|mut record| {
                match &mut record {
                    Record::Include { loc, .. } => normalize_loc(loc),
                    Record::Statement { loc, .. } => normalize_loc(loc),
                    Record::System { loc, .. } => normalize_loc(loc),
                    Record::Query { loc, .. } => normalize_loc(loc),
                    Record::Sleep { loc, .. } => normalize_loc(loc),
                    Record::Subtest { loc, .. } => normalize_loc(loc),
                    Record::Halt { loc, .. } => normalize_loc(loc),
                    Record::HashThreshold { loc, .. } => normalize_loc(loc),
                    // even though these variants don't include a
                    // location include them in this match statement
                    // so if new variants are added, this match
                    // statement must be too.
                    Record::Condition(_)
                    | Record::Connection(_)
                    | Record::Comment(_)
                    | Record::Control(_)
                    | Record::Newline
                    | Record::Injected(_) => {}
                };
                record
            })
            .collect()
    }

    // Normalize a location
    fn normalize_loc(loc: &mut Location) {
        loc.file = std::sync::Arc::from("__FILENAME__");
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum CustomColumnType {
        Integer,
        Boolean,
    }

    impl ColumnType for CustomColumnType {
        fn from_char(value: char) -> Option<Self> {
            match value {
                'I' => Some(Self::Integer),
                'B' => Some(Self::Boolean),
                _ => None,
            }
        }

        fn to_char(&self) -> char {
            match self {
                Self::Integer => 'I',
                Self::Boolean => 'B',
            }
        }
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_statement_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/statement_retry.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_query_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/query_retry.slt")
    }
}
