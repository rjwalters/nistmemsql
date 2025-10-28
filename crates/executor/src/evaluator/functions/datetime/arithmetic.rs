//! Date/time arithmetic functions
//!
//! Implements DATEDIFF, DATE_ADD, DATE_SUB, AGE, and supporting helpers.

use crate::errors::ExecutorError;
use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime};
use types::SqlValue;

use super::extract::{day, hour, minute, month, second, year};

/// DATEDIFF(date1, date2) - Calculate day difference between two dates
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
/// Returns: date1 - date2 in days
pub fn datediff(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATEDIFF requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => {
            Ok(SqlValue::Null)
        }
        (SqlValue::Date(date1_str), SqlValue::Date(date2_str)) |
        (SqlValue::Timestamp(date1_str), SqlValue::Date(date2_str)) |
        (SqlValue::Date(date1_str), SqlValue::Timestamp(date2_str)) |
        (SqlValue::Timestamp(date1_str), SqlValue::Timestamp(date2_str)) => {
            // Extract date part from timestamps if needed
            let date1_part = date1_str.split(' ').next().unwrap_or(date1_str);
            let date2_part = date2_str.split(' ').next().unwrap_or(date2_str);

            // Parse dates
            let date1 = NaiveDate::parse_from_str(date1_part, "%Y-%m-%d")
                .map_err(|_| ExecutorError::UnsupportedFeature(
                    format!("Invalid date format for DATEDIFF: {}", date1_part)
                ))?;
            let date2 = NaiveDate::parse_from_str(date2_part, "%Y-%m-%d")
                .map_err(|_| ExecutorError::UnsupportedFeature(
                    format!("Invalid date format for DATEDIFF: {}", date2_part)
                ))?;

            // Calculate difference in days
            let diff = date1.signed_duration_since(date2).num_days();
            Ok(SqlValue::Integer(diff))
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(
            format!("DATEDIFF requires date or timestamp arguments, got {:?} and {:?}", a, b),
        )),
    }
}

/// DATE_ADD(date, amount, unit) - Add interval to date
/// Alias: ADDDATE
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
/// Units: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
pub fn date_add(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires exactly 3 arguments (date, amount, unit), got {}", args.len()),
        ));
    }

    // Handle NULL inputs
    if matches!(&args[0], SqlValue::Null) ||
       matches!(&args[1], SqlValue::Null) ||
       matches!(&args[2], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    // Extract date string
    let date_str = match &args[0] {
        SqlValue::Date(s) | SqlValue::Timestamp(s) => s,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires date/timestamp as first argument, got {:?}", val)
        )),
    };

    // Extract amount
    let amount = match &args[1] {
        SqlValue::Integer(n) => *n,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires integer amount, got {:?}", val)
        )),
    };

    // Extract unit
    let unit = match &args[2] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires string unit, got {:?}", val)
        )),
    };

    // Perform date arithmetic based on unit
    date_add_subtract(date_str, amount, &unit, true)
}

/// DATE_SUB(date, amount, unit) - Subtract interval from date
/// Alias: SUBDATE
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
pub fn date_sub(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires exactly 3 arguments (date, amount, unit), got {}", args.len()),
        ));
    }

    // Handle NULL inputs
    if matches!(&args[0], SqlValue::Null) ||
       matches!(&args[1], SqlValue::Null) ||
       matches!(&args[2], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    // Extract date string
    let date_str = match &args[0] {
        SqlValue::Date(s) | SqlValue::Timestamp(s) => s,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires date/timestamp as first argument, got {:?}", val)
        )),
    };

    // Extract amount
    let amount = match &args[1] {
        SqlValue::Integer(n) => *n,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires integer amount, got {:?}", val)
        )),
    };

    // Extract unit
    let unit = match &args[2] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires string unit, got {:?}", val)
        )),
    };

    // Perform date arithmetic (negate amount for subtraction)
    date_add_subtract(date_str, -amount, &unit, true)
}

/// AGE(date1, date2) - Calculate age as interval between two dates
/// AGE(date) - Calculate age from date to current date
/// Returns VARCHAR representation of interval (e.g., "2 years 3 months 5 days")
pub fn age(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("AGE requires 1 or 2 arguments, got {}", args.len()),
        ));
    }

    // Get the two dates to compare
    let (date1_str, date2_str) = if args.len() == 1 {
        // AGE(date) - compare with current date
        match &args[0] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Date(s) | SqlValue::Timestamp(s) => {
                let now = Local::now();
                let current_date = now.format("%Y-%m-%d").to_string();
                (current_date, s.clone())
            }
            val => return Err(ExecutorError::UnsupportedFeature(
                format!("AGE requires date/timestamp argument, got {:?}", val)
            )),
        }
    } else {
        // AGE(date1, date2) - calculate date1 - date2
        match (&args[0], &args[1]) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => {
                return Ok(SqlValue::Null);
            }
            (
                SqlValue::Date(d1) | SqlValue::Timestamp(d1),
                SqlValue::Date(d2) | SqlValue::Timestamp(d2),
            ) => (d1.clone(), d2.clone()),
            (a, b) => return Err(ExecutorError::UnsupportedFeature(
                format!("AGE requires date/timestamp arguments, got {:?} and {:?}", a, b)
            )),
        }
    };

    // Extract date parts
    let date1_part = date1_str.split(' ').next().unwrap_or(&date1_str);
    let date2_part = date2_str.split(' ').next().unwrap_or(&date2_str);

    // Parse dates
    let date1 = NaiveDate::parse_from_str(date1_part, "%Y-%m-%d")
        .map_err(|_| ExecutorError::UnsupportedFeature(
            format!("Invalid date format for AGE: {}", date1_part)
        ))?;
    let date2 = NaiveDate::parse_from_str(date2_part, "%Y-%m-%d")
        .map_err(|_| ExecutorError::UnsupportedFeature(
            format!("Invalid date format for AGE: {}", date2_part)
        ))?;

    // Calculate age components
    let (years, months, days) = calculate_age_components(date1, date2);

    // Format as interval string
    let mut parts = Vec::new();
    if years != 0 {
        parts.push(format!("{} year{}", years.abs(), if years.abs() == 1 { "" } else { "s" }));
    }
    if months != 0 {
        parts.push(format!("{} month{}", months.abs(), if months.abs() == 1 { "" } else { "s" }));
    }
    if days != 0 || parts.is_empty() {
        parts.push(format!("{} day{}", days.abs(), if days.abs() == 1 { "" } else { "s" }));
    }

    let result = if date1 < date2 {
        format!("-{}", parts.join(" "))
    } else {
        parts.join(" ")
    };

    Ok(SqlValue::Varchar(result))
}

/// EXTRACT(unit, date) - Extract date/time component
/// SQL:1999 Section 6.32: Datetime field extraction
/// Simplified syntax: EXTRACT('YEAR', date) instead of EXTRACT(YEAR FROM date)
pub fn extract(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("EXTRACT requires exactly 2 arguments (unit, date), got {}", args.len()),
        ));
    }

    // Extract unit
    let unit = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("EXTRACT requires string unit as first argument, got {:?}", val)
        )),
    };

    // Delegate to existing extraction functions based on unit
    match unit.as_ref() {
        "YEAR" => year(&[args[1].clone()]),
        "MONTH" => month(&[args[1].clone()]),
        "DAY" => day(&[args[1].clone()]),
        "HOUR" => hour(&[args[1].clone()]),
        "MINUTE" => minute(&[args[1].clone()]),
        "SECOND" => second(&[args[1].clone()]),
        _ => Err(ExecutorError::UnsupportedFeature(
            format!("Unsupported EXTRACT unit: {}", unit)
        )),
    }
}

// ==================== HELPER FUNCTIONS ====================

/// Helper for DATE_ADD and DATE_SUB functions
/// Adds or subtracts an interval from a date/timestamp
/// preserve_time: if true and input is timestamp, preserve time component
fn date_add_subtract(
    date_str: &str,
    amount: i64,
    unit: &str,
    preserve_time: bool,
) -> Result<SqlValue, ExecutorError> {
    // Check if input is a timestamp (has time component)
    let is_timestamp = date_str.contains(' ');

    if is_timestamp && preserve_time {
        // Parse as timestamp and preserve time
        let timestamp = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S")
            .map_err(|_| ExecutorError::UnsupportedFeature(
                format!("Invalid timestamp format: {}", date_str)
            ))?;

        let new_timestamp = match unit {
            "YEAR" | "YEARS" => {
                let year = timestamp.year() + amount as i32;
                timestamp.with_year(year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", year)
                ))?
            }
            "MONTH" | "MONTHS" => {
                let months = timestamp.month() as i64 + amount;
                let years_offset = (months - 1) / 12;
                let new_month = ((months - 1) % 12 + 1) as u32;
                let new_month = if new_month == 0 { 12 } else { new_month };
                let new_year = timestamp.year() + years_offset as i32;

                let mut new_ts = timestamp.with_year(new_year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", new_year)
                ))?;
                new_ts = new_ts.with_month(new_month).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid month: {}", new_month)
                ))?;
                new_ts
            }
            "DAY" | "DAYS" => timestamp + Duration::days(amount),
            "HOUR" | "HOURS" => timestamp + Duration::hours(amount),
            "MINUTE" | "MINUTES" => timestamp + Duration::minutes(amount),
            "SECOND" | "SECONDS" => timestamp + Duration::seconds(amount),
            _ => return Err(ExecutorError::UnsupportedFeature(
                format!("Unsupported date unit: {}", unit)
            )),
        };

        Ok(SqlValue::Timestamp(new_timestamp.format("%Y-%m-%d %H:%M:%S").to_string()))
    } else {
        // Parse as date (extract date part if timestamp)
        let date_part = date_str.split(' ').next().unwrap_or(date_str);
        let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
            .map_err(|_| ExecutorError::UnsupportedFeature(
                format!("Invalid date format: {}", date_part)
            ))?;

        let new_date = match unit {
            "YEAR" | "YEARS" => {
                let year = date.year() + amount as i32;
                date.with_year(year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", year)
                ))?
            }
            "MONTH" | "MONTHS" => {
                let months = date.month() as i64 + amount;
                let years_offset = (months - 1) / 12;
                let new_month = ((months - 1) % 12 + 1) as u32;
                let new_month = if new_month == 0 { 12 } else { new_month };
                let new_year = date.year() + years_offset as i32;

                let mut new_date = date.with_year(new_year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", new_year)
                ))?;
                new_date = new_date.with_month(new_month).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid month: {}", new_month)
                ))?;
                new_date
            }
            "DAY" | "DAYS" => date + Duration::days(amount),
            "HOUR" | "HOURS" | "MINUTE" | "MINUTES" | "SECOND" | "SECONDS" => {
                // Time units on dates don't change the date
                return Ok(SqlValue::Date(date.format("%Y-%m-%d").to_string()));
            }
            _ => return Err(ExecutorError::UnsupportedFeature(
                format!("Unsupported date unit: {}", unit)
            )),
        };

        Ok(SqlValue::Date(new_date.format("%Y-%m-%d").to_string()))
    }
}

/// Helper for AGE function - calculate years, months, and days between two dates
/// Returns (years, months, days) where all values have the same sign
fn calculate_age_components(date1: NaiveDate, date2: NaiveDate) -> (i32, i32, i32) {
    // Determine if result should be negative
    let is_negative = date1 < date2;
    let (later, earlier) = if is_negative {
        (date2, date1)
    } else {
        (date1, date2)
    };

    let mut years = later.year() - earlier.year();
    let mut months = later.month() as i32 - earlier.month() as i32;
    let mut days = later.day() as i32 - earlier.day() as i32;

    // Adjust if days are negative
    if days < 0 {
        months -= 1;
        // Add days in the previous month
        let prev_month = if later.month() == 1 { 12 } else { later.month() - 1 };
        let prev_year = if later.month() == 1 { later.year() - 1 } else { later.year() };

        // Get days in previous month
        let days_in_prev_month = if let Some(date) = NaiveDate::from_ymd_opt(prev_year, prev_month, 1) {
            let next_month_date = if prev_month == 12 {
                NaiveDate::from_ymd_opt(prev_year + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(prev_year, prev_month + 1, 1).unwrap()
            };
            (next_month_date - date).num_days() as i32
        } else {
            30 // Fallback to 30 days
        };

        days += days_in_prev_month;
    }

    // Adjust if months are negative
    if months < 0 {
        years -= 1;
        months += 12;
    }

    // Apply sign
    if is_negative {
        (-years, -months, -days)
    } else {
        (years, months, days)
    }
}
