//! Parsing of individual SQL record types (statement, query, system).

use std::fmt;
use std::iter::Peekable;

use itertools::Itertools;

use crate::{ColumnType, Location, ParseError, ParseErrorKind};
use crate::error_parser::ExpectedError;
use crate::retry_parser::RetryConfig;
use crate::directive_parser::{SortMode, ResultMode, ControlItem};

const RESULTS_DELIMITER: &str = "----";

/// Expectation for a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementExpect {
    /// Statement should succeed.
    Ok,
    /// Statement should succeed and affect the given number of rows.
    Count(u64),
    /// Statement should fail with the given error message.
    Error(ExpectedError),
}

/// Expectation for a query.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpect<T: ColumnType> {
    /// Query should succeed and return the given results.
    Results {
        types: Vec<T>,
        sort_mode: Option<SortMode>,
        result_mode: Option<ResultMode>,
        label: Option<String>,
        results: Vec<String>,
    },
    /// Query should fail with the given error message.
    Error(ExpectedError),
}

impl<T: ColumnType> QueryExpect<T> {
    /// Creates a new [`QueryExpect`] with empty results.
    pub(crate) fn empty_results() -> Self {
        Self::Results {
            types: Vec::new(),
            sort_mode: None,
            result_mode: None,
            label: None,
            results: Vec::new(),
        }
    }
}

/// Parse one or more lines until empty line or a delimiter.
pub(crate) fn parse_lines<'a>(
    lines: &mut impl Iterator<Item = (usize, &'a str)>,
    loc: &Location,
    delimiter: Option<&str>,
) -> Result<(String, bool), ParseError> {
    let mut found_delimiter = false;
    let mut out = match lines.next() {
        Some((_, line)) => Ok(line.into()),
        None => Err(ParseErrorKind::UnexpectedEOF.at(loc.clone().next_line())),
    }?;

    for (_, line) in lines {
        if line.is_empty() {
            break;
        }
        if let Some(delimiter) = delimiter {
            if line == delimiter {
                found_delimiter = true;
                break;
            }
        }
        out += "\n";
        out += line;
    }

    Ok((out, found_delimiter))
}

/// Parse multiline output under `----`.
pub(crate) fn parse_multiple_result<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> String {
    let mut results = String::new();

    while let Some((_, line)) = lines.next() {
        // 2 consecutive empty lines
        if line.is_empty() && lines.peek().map(|(_, l)| l.is_empty()).unwrap_or(true) {
            lines.next();
            break;
        }
        results += line;
        results.push('\n');
    }

    results.trim().to_string()
}

/// Parse multiline error message under `----`.
pub(crate) fn parse_multiline_error<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> ExpectedError {
    ExpectedError::Multiline(parse_multiple_result(lines))
}

/// Format functions for unparsing records

/// Unparse a statement record.
pub(crate) fn fmt_statement(
    f: &mut fmt::Formatter<'_>,
    sql: &str,
    expected: &StatementExpect,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    write!(f, "statement ")?;
    match expected {
        StatementExpect::Ok => write!(f, "ok")?,
        StatementExpect::Count(cnt) => write!(f, "count {cnt}")?,
        StatementExpect::Error(err) => err.fmt_inline(f)?,
    }
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    writeln!(f)?;
    // statement always end with a blank line
    writeln!(f, "{sql}")?;

    if let StatementExpect::Error(err) = expected {
        err.fmt_multiline(f, RESULTS_DELIMITER)?;
    }
    Ok(())
}

/// Unparse a query record.
pub(crate) fn fmt_query<T: ColumnType>(
    f: &mut fmt::Formatter<'_>,
    sql: &str,
    expected: &QueryExpect<T>,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    write!(f, "query ")?;
    match expected {
        QueryExpect::Results {
            types,
            sort_mode,
            label,
            ..
        } => {
            write!(f, "{}", types.iter().map(|c| c.to_char()).join(""))?;
            if let Some(sort_mode) = sort_mode {
                write!(f, " {}", sort_mode.as_str())?;
            }
            if let Some(label) = label {
                write!(f, " {label}")?;
            }
        }
        QueryExpect::Error(err) => err.fmt_inline(f)?,
    }
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    writeln!(f)?;
    writeln!(f, "{sql}")?;

    match expected {
        QueryExpect::Results { results, .. } => {
            write!(f, "{}", RESULTS_DELIMITER)?;
            for result in results {
                write!(f, "\n{result}")?;
            }

            // query always ends with a blank line
            writeln!(f)?
        }
        QueryExpect::Error(err) => err.fmt_multiline(f, RESULTS_DELIMITER)?,
    }
    Ok(())
}

/// Unparse a system record.
pub(crate) fn fmt_system(
    f: &mut fmt::Formatter<'_>,
    command: &str,
    stdout: &Option<String>,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    writeln!(f, "system ok\n{command}")?;
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    if let Some(stdout) = stdout {
        writeln!(f, "----\n{}\n", stdout.trim())?;
    }
    Ok(())
}
