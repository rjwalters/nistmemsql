//! Date and time format string parsing utilities
//!
//! Converts between SQL format strings (YYYY-MM-DD, Mon DD YYYY, etc.)
//! and chrono format strings (%Y-%m-%d, %b %d %Y, etc.)

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::errors::ExecutorError;

/// Convert SQL format string to chrono format string
///
/// Supports common SQL format codes:
/// - Date: YYYY (4-digit year), YY (2-digit year), MM (month), DD (day), Mon (abbreviated month
///   name)
/// - Time: HH24 (24-hour), HH12 (12-hour), MI (minute), SS (second), AM/PM
///
/// # Examples
/// ```
/// use vibesql_executor::evaluator::date_format::sql_to_chrono_format;
/// assert_eq!(sql_to_chrono_format("YYYY-MM-DD"), Ok("%Y-%m-%d".to_string()));
/// assert_eq!(sql_to_chrono_format("Mon DD, YYYY"), Ok("%b %d, %Y".to_string()));
/// ```
pub fn sql_to_chrono_format(sql_format: &str) -> Result<String, ExecutorError> {
    let mut result = sql_format.to_string();

    // Date components (order matters - replace longer patterns first)
    result = result.replace("YYYY", "%Y"); // 4-digit year
    result = result.replace("YY", "%y"); // 2-digit year
    result = result.replace("Month", "%B"); // Full month name (January, February, etc.) - must come before "Mon"
    result = result.replace("Mon", "%b"); // Abbreviated month name (Jan, Feb, etc.)
    result = result.replace("MM", "%m"); // Month as number (01-12)
    result = result.replace("DD", "%d"); // Day of month (01-31)
    result = result.replace("Day", "%A"); // Full day name (Monday, Tuesday, etc.)
    result = result.replace("Dy", "%a"); // Abbreviated day name (Mon, Tue, etc.)

    // Time components
    result = result.replace("HH24", "%H"); // 24-hour (00-23)
    result = result.replace("HH12", "%I"); // 12-hour (01-12)
    result = result.replace("HH", "%H"); // Default to 24-hour
    result = result.replace("MI", "%M"); // Minute (00-59)
    result = result.replace("SS", "%S"); // Second (00-59)
    result = result.replace("AM", "%p"); // AM/PM
    result = result.replace("PM", "%p"); // AM/PM
    result = result.replace("am", "%P"); // am/pm (lowercase)
    result = result.replace("pm", "%P"); // am/pm (lowercase)

    Ok(result)
}

/// Format a date using SQL format string
///
/// # Examples
/// ```
/// use chrono::NaiveDate;
/// use vibesql_executor::evaluator::date_format::format_date;
/// let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
/// assert_eq!(format_date(&date, "YYYY-MM-DD"), Ok("2024-03-15".to_string()));
/// assert_eq!(format_date(&date, "Mon DD, YYYY"), Ok("Mar 15, 2024".to_string()));
/// ```
pub fn format_date(date: &NaiveDate, sql_format: &str) -> Result<String, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    Ok(date.format(&chrono_format).to_string())
}

/// Format a timestamp using SQL format string
///
/// # Examples
/// ```
/// use chrono::{DateTime, NaiveDateTime};
/// use vibesql_executor::evaluator::date_format::format_timestamp;
/// let timestamp = DateTime::from_timestamp(1700000000, 0).unwrap().naive_utc();
/// assert_eq!(
///     format_timestamp(&timestamp, "YYYY-MM-DD HH24:MI:SS"),
///     Ok("2023-11-14 22:13:20".to_string())
/// );
/// ```
pub fn format_timestamp(
    timestamp: &NaiveDateTime,
    sql_format: &str,
) -> Result<String, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    Ok(timestamp.format(&chrono_format).to_string())
}

/// Format a time using SQL format string
pub fn format_time(time: &NaiveTime, sql_format: &str) -> Result<String, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    Ok(time.format(&chrono_format).to_string())
}

/// Parse a date string using SQL format string
///
/// # Examples
/// ```
/// use chrono::NaiveDate;
/// use vibesql_executor::evaluator::date_format::parse_date;
/// assert_eq!(
///     parse_date("2024-03-15", "YYYY-MM-DD"),
///     Ok(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
/// );
/// assert_eq!(
///     parse_date("15/03/2024", "DD/MM/YYYY"),
///     Ok(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
/// );
/// ```
pub fn parse_date(input: &str, sql_format: &str) -> Result<NaiveDate, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    NaiveDate::parse_from_str(input, &chrono_format).map_err(|e| {
        ExecutorError::UnsupportedFeature(format!(
            "Failed to parse date '{}' with format '{}': {}",
            input, sql_format, e
        ))
    })
}

/// Parse a timestamp string using SQL format string
///
/// # Examples
/// ```
/// use chrono::{DateTime, NaiveDateTime};
/// use vibesql_executor::evaluator::date_format::parse_timestamp;
/// let expected = DateTime::from_timestamp(1710513045, 0).unwrap().naive_utc();
/// assert_eq!(parse_timestamp("2024-03-15 14:30:45", "YYYY-MM-DD HH24:MI:SS"), Ok(expected));
/// ```
pub fn parse_timestamp(input: &str, sql_format: &str) -> Result<NaiveDateTime, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    NaiveDateTime::parse_from_str(input, &chrono_format).map_err(|e| {
        ExecutorError::UnsupportedFeature(format!(
            "Failed to parse timestamp '{}' with format '{}': {}",
            input, sql_format, e
        ))
    })
}

/// Parse a time string using SQL format string
pub fn parse_time(input: &str, sql_format: &str) -> Result<NaiveTime, ExecutorError> {
    let chrono_format = sql_to_chrono_format(sql_format)?;
    NaiveTime::parse_from_str(input, &chrono_format).map_err(|e| {
        ExecutorError::UnsupportedFeature(format!(
            "Failed to parse time '{}' with format '{}': {}",
            input, sql_format, e
        ))
    })
}

/// Format a number using SQL number format string
///
/// Supports format codes:
/// - 9: digit position (no leading zeros)
/// - 0: digit position (with leading zeros)
/// - .: decimal point
/// - ,: thousand separator
/// - $: dollar sign prefix
/// - %: percentage suffix (multiplies by 100)
///
/// # Examples
/// ```
/// use vibesql_executor::evaluator::date_format::format_number;
/// assert_eq!(format_number(1234.5, "9999.99"), Ok("1234.50".to_string()));
/// assert_eq!(format_number(1234.5, "$9,999.99"), Ok("$1,234.50".to_string()));
/// assert_eq!(format_number(0.75, "99.99%"), Ok("75.00%".to_string()));
/// ```
pub fn format_number(number: f64, sql_format: &str) -> Result<String, ExecutorError> {
    // Check for special prefixes/suffixes
    let has_dollar = sql_format.starts_with('$');
    let has_percent = sql_format.ends_with('%');

    // Apply percentage conversion
    let mut value = if has_percent { number * 100.0 } else { number };

    // Extract format pattern (remove $ and %)
    let pattern = sql_format.trim_start_matches('$').trim_end_matches('%').trim();

    // Determine if we need thousand separators
    let has_comma = pattern.contains(',');

    // Find decimal point position
    let decimal_pos = pattern.rfind('.');
    let decimal_places = if let Some(pos) = decimal_pos { pattern.len() - pos - 1 } else { 0 };

    // Round to decimal places
    let multiplier = 10_f64.powi(decimal_places as i32);
    value = (value * multiplier).round() / multiplier;

    // Split into integer and decimal parts
    let value_str = value.abs().to_string();
    let parts: Vec<&str> = value_str.split('.').collect();
    let int_part = parts[0].parse::<i64>().unwrap_or(0);
    let dec_part = if decimal_places > 0 {
        if parts.len() > 1 {
            // Pad or truncate decimal part to required decimal places
            let mut dec = parts[1].to_string();
            while dec.len() < decimal_places {
                dec.push('0');
            }
            dec.truncate(decimal_places);
            format!(".{}", dec)
        } else {
            format!(".{:0width$}", 0, width = decimal_places)
        }
    } else {
        String::new()
    };

    // Format integer part with thousand separators if needed
    let formatted_int = if has_comma { format_with_commas(int_part) } else { int_part.to_string() };

    // Combine parts
    let mut result = String::new();
    if value < 0.0 {
        result.push('-');
    }
    if has_dollar {
        result.push('$');
    }
    result.push_str(&formatted_int);
    result.push_str(&dec_part);
    if has_percent {
        result.push('%');
    }

    Ok(result)
}

/// Helper: Format integer with thousand separators
fn format_with_commas(num: i64) -> String {
    let s = num.abs().to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, ch);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_to_chrono_format_basic() {
        assert_eq!(sql_to_chrono_format("YYYY-MM-DD").unwrap(), "%Y-%m-%d");
        assert_eq!(sql_to_chrono_format("DD/MM/YYYY").unwrap(), "%d/%m/%Y");
    }

    #[test]
    fn test_sql_to_chrono_format_with_names() {
        assert_eq!(sql_to_chrono_format("Mon DD, YYYY").unwrap(), "%b %d, %Y");
        assert_eq!(sql_to_chrono_format("Month DD, YYYY").unwrap(), "%B %d, %Y");
    }

    #[test]
    fn test_sql_to_chrono_format_timestamp() {
        assert_eq!(sql_to_chrono_format("YYYY-MM-DD HH24:MI:SS").unwrap(), "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            sql_to_chrono_format("DD/MM/YYYY HH12:MI:SS AM").unwrap(),
            "%d/%m/%Y %I:%M:%S %p"
        );
    }

    #[test]
    fn test_format_date() {
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert_eq!(format_date(&date, "YYYY-MM-DD").unwrap(), "2024-03-15");
        assert_eq!(format_date(&date, "DD/MM/YYYY").unwrap(), "15/03/2024");
        assert_eq!(format_date(&date, "Mon DD, YYYY").unwrap(), "Mar 15, 2024");
    }

    #[test]
    fn test_parse_date() {
        assert_eq!(
            parse_date("2024-03-15", "YYYY-MM-DD").unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
        );
        assert_eq!(
            parse_date("15/03/2024", "DD/MM/YYYY").unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
        );
    }

    #[test]
    fn test_format_number_basic() {
        assert_eq!(format_number(1234.5, "9999.99").unwrap(), "1234.50");
        assert_eq!(format_number(123.456, "999.99").unwrap(), "123.46"); // Rounds
    }

    #[test]
    fn test_format_number_with_comma() {
        assert_eq!(format_number(1234.5, "9,999.99").unwrap(), "1,234.50");
        assert_eq!(format_number(1234567.89, "9,999,999.99").unwrap(), "1,234,567.89");
    }

    #[test]
    fn test_format_number_with_dollar() {
        assert_eq!(format_number(1234.5, "$9,999.99").unwrap(), "$1,234.50");
    }

    #[test]
    fn test_format_number_with_percent() {
        assert_eq!(format_number(0.75, "99.99%").unwrap(), "75.00%");
        assert_eq!(format_number(1.5, "999%").unwrap(), "150%");
    }

    #[test]
    fn test_format_number_negative() {
        assert_eq!(format_number(-1234.5, "9999.99").unwrap(), "-1234.50");
        assert_eq!(format_number(-1234.5, "$9,999.99").unwrap(), "-$1,234.50");
    }
}
