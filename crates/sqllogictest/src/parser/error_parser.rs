//! Parsing and handling of expected error messages.

use std::fmt;

use itertools::Itertools;
use regex::Regex;

use crate::ParseErrorKind;

/// Expected error message after `error` or under `----`.
#[derive(Debug, Clone)]
pub enum ExpectedError {
    /// No expected error message.
    ///
    /// Any error message is considered as a match.
    Empty,
    /// An inline regular expression after `error`.
    ///
    /// The actual error message that matches the regex is considered as a match.
    Inline(Regex),
    /// A multiline error message under `----`, ends with 2 consecutive empty lines.
    ///
    /// The actual error message that's exactly the same as the expected one is considered as a
    /// match.
    Multiline(String),
}

impl ExpectedError {
    /// Parses an inline regex variant from tokens.
    pub(crate) fn parse_inline_tokens(tokens: &[&str]) -> Result<Self, ParseErrorKind> {
        Self::new_inline(tokens.join(" "))
    }

    /// Creates an inline expected error message from a regex string.
    ///
    /// If the regex is empty, it's considered as [`ExpectedError::Empty`].
    fn new_inline(regex: String) -> Result<Self, ParseErrorKind> {
        if regex.is_empty() {
            Ok(Self::Empty)
        } else {
            let regex =
                Regex::new(&regex).map_err(|_| ParseErrorKind::InvalidErrorMessage(regex))?;
            Ok(Self::Inline(regex))
        }
    }

    /// Returns whether it's an empty match.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Unparses the expected message after `statement`.
    pub(crate) fn fmt_inline(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error")?;
        if let Self::Inline(regex) = self {
            write!(f, " {regex}")?;
        }
        Ok(())
    }

    /// Unparses the expected message with `----`, if it's multiline.
    pub(crate) fn fmt_multiline(
        &self,
        f: &mut fmt::Formatter<'_>,
        results_delimiter: &str,
    ) -> fmt::Result {
        if let Self::Multiline(results) = self {
            writeln!(f, "{}", results_delimiter)?;
            writeln!(f, "{}", results.trim())?;
            writeln!(f)?; // another empty line to indicate the end of multiline message
        }
        Ok(())
    }

    /// Returns whether the given error message matches the expected one.
    pub fn is_match(&self, err: &str) -> bool {
        match self {
            Self::Empty => true,
            Self::Inline(regex) => regex.is_match(err),
            Self::Multiline(results) => results.trim() == err.trim(),
        }
    }

    /// Creates an expected error message from the actual error message. Used by the runner
    /// to update the test cases with `--override`.
    ///
    /// A reference might be provided to help decide whether to use inline or multiline.
    pub fn from_actual_error(reference: Option<&Self>, actual_err: &str) -> Self {
        let trimmed_err = actual_err.trim();
        let err_is_multiline = trimmed_err.lines().next_tuple::<(_, _)>().is_some();

        let multiline = match reference {
            Some(Self::Multiline(_)) => true, // always multiline if the ref is multiline
            _ => err_is_multiline,            // prefer inline as long as it fits
        };

        if multiline {
            // Even if the actual error is empty, we still use `Multiline` to indicate that
            // an exact empty error is expected, instead of any error by `Empty`.
            Self::Multiline(trimmed_err.to_string())
        } else {
            Self::new_inline(regex::escape(actual_err)).expect("escaped regex should be valid")
        }
    }
}

impl std::fmt::Display for ExpectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedError::Empty => write!(f, "(any)"),
            ExpectedError::Inline(regex) => write!(f, "(regex) {}", regex),
            ExpectedError::Multiline(results) => write!(f, "(multiline) {}", results.trim()),
        }
    }
}

impl PartialEq for ExpectedError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Inline(l0), Self::Inline(r0)) => l0.as_str() == r0.as_str(),
            (Self::Multiline(l0), Self::Multiline(r0)) => l0 == r0,
            _ => false,
        }
    }
}
