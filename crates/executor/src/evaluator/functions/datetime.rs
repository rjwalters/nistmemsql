//! Date and time function implementations for SQL scalar functions
//!
//! This module contains all date/time manipulation and extraction functions including:
//! - Current date/time functions (CURRENT_DATE/CURDATE, CURRENT_TIME/CURTIME, CURRENT_TIMESTAMP/NOW)
//! - Date/time extraction (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EXTRACT)
//! - Date arithmetic (DATE_ADD/ADDDATE, DATE_SUB/SUBDATE, DATEDIFF)
//! - Age calculation (AGE)
//!
//! These functions implement SQL:1999 standard date/time operations.

use crate::errors::ExecutorError;
use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime};

/// CURRENT_DATE / CURDATE - Returns current date
/// Alias: CURDATE
/// SQL:1999 Section 6.31: Datetime value functions
pub(super) fn current_date(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_DATE takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    Ok(types::SqlValue::Date(date_str))
}

/// CURRENT_TIME / CURTIME - Returns current time
/// Alias: CURTIME
/// SQL:1999 Section 6.31: Datetime value functions
pub(super) fn current_time(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIME takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let time_str = now.format("%H:%M:%S").to_string();
    Ok(types::SqlValue::Time(time_str))
}

/// CURRENT_TIMESTAMP / NOW - Returns current timestamp
/// Alias: NOW
/// SQL:1999 Section 6.31: Datetime value functions
pub(super) fn current_timestamp(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIMESTAMP takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let timestamp_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
    Ok(types::SqlValue::Timestamp(timestamp_str))
}

/// YEAR(date) - Extract year from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn year(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "YEAR requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let parts: Vec<&str> = s.split(&['-', ' '][..]).collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for YEAR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(year) => Ok(types::SqlValue::Integer(year)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid year value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "YEAR requires date or timestamp argument".to_string(),
        )),
    }
}

/// MONTH(date) - Extract month from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn month(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "MONTH requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let date_part = s.split(' ').next().unwrap_or(s);
            let parts: Vec<&str> = date_part.split('-').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for MONTH".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(month) => Ok(types::SqlValue::Integer(month)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid month value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "MONTH requires date or timestamp argument".to_string(),
        )),
    }
}

/// DAY(date) - Extract day from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn day(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "DAY requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let date_part = s.split(' ').next().unwrap_or(s);
            let parts: Vec<&str> = date_part.split('-').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for DAY".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(day) => Ok(types::SqlValue::Integer(day)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid day value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "DAY requires date or timestamp argument".to_string(),
        )),
    }
}

/// HOUR(time) - Extract hour from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn hour(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "HOUR requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for HOUR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(hour) => Ok(types::SqlValue::Integer(hour)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid hour value".to_string(),
                )),
            }
        }
        types::SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for HOUR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(hour) => Ok(types::SqlValue::Integer(hour)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid hour value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "HOUR requires time or timestamp argument".to_string(),
        )),
    }
}

/// MINUTE(time) - Extract minute from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn minute(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "MINUTE requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for MINUTE".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(minute) => Ok(types::SqlValue::Integer(minute)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid minute value".to_string(),
                )),
            }
        }
        types::SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for MINUTE".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(minute) => Ok(types::SqlValue::Integer(minute)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid minute value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "MINUTE requires time or timestamp argument".to_string(),
        )),
    }
}

/// SECOND(time) - Extract second from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub(super) fn second(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "SECOND requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for SECOND".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(second) => Ok(types::SqlValue::Integer(second)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid second value".to_string(),
                )),
            }
        }
        types::SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for SECOND".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(second) => Ok(types::SqlValue::Integer(second)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid second value".to_string(),
                )),
            }
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "SECOND requires time or timestamp argument".to_string(),
        )),
    }
}

/// DATEDIFF(date1, date2) - Calculate day difference between two dates
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
/// Returns: date1 - date2 in days
pub(super) fn datediff(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATEDIFF requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
            Ok(types::SqlValue::Null)
        }
        (types::SqlValue::Date(date1_str), types::SqlValue::Date(date2_str)) |
        (types::SqlValue::Timestamp(date1_str), types::SqlValue::Date(date2_str)) |
        (types::SqlValue::Date(date1_str), types::SqlValue::Timestamp(date2_str)) |
        (types::SqlValue::Timestamp(date1_str), types::SqlValue::Timestamp(date2_str)) => {
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
            Ok(types::SqlValue::Integer(diff))
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
pub(super) fn date_add(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires exactly 3 arguments (date, amount, unit), got {}", args.len()),
        ));
    }

    // Handle NULL inputs
    if matches!(&args[0], types::SqlValue::Null) ||
       matches!(&args[1], types::SqlValue::Null) ||
       matches!(&args[2], types::SqlValue::Null) {
        return Ok(types::SqlValue::Null);
    }

    // Extract date string
    let date_str = match &args[0] {
        types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => s,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires date/timestamp as first argument, got {:?}", val)
        )),
    };

    // Extract amount
    let amount = match &args[1] {
        types::SqlValue::Integer(n) => *n,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_ADD requires integer amount, got {:?}", val)
        )),
    };

    // Extract unit
    let unit = match &args[2] {
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
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
pub(super) fn date_sub(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires exactly 3 arguments (date, amount, unit), got {}", args.len()),
        ));
    }

    // Handle NULL inputs
    if matches!(&args[0], types::SqlValue::Null) ||
       matches!(&args[1], types::SqlValue::Null) ||
       matches!(&args[2], types::SqlValue::Null) {
        return Ok(types::SqlValue::Null);
    }

    // Extract date string
    let date_str = match &args[0] {
        types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => s,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires date/timestamp as first argument, got {:?}", val)
        )),
    };

    // Extract amount
    let amount = match &args[1] {
        types::SqlValue::Integer(n) => *n,
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires integer amount, got {:?}", val)
        )),
    };

    // Extract unit
    let unit = match &args[2] {
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
        val => return Err(ExecutorError::UnsupportedFeature(
            format!("DATE_SUB requires string unit, got {:?}", val)
        )),
    };

    // Perform date arithmetic (negate amount for subtraction)
    date_add_subtract(date_str, -amount, &unit, true)
}

/// EXTRACT(unit, date) - Extract date/time component
/// SQL:1999 Section 6.32: Datetime field extraction
/// Simplified syntax: EXTRACT('YEAR', date) instead of EXTRACT(YEAR FROM date)
pub(super) fn extract(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("EXTRACT requires exactly 2 arguments (unit, date), got {}", args.len()),
        ));
    }

    // Extract unit
    let unit = match &args[0] {
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
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

/// AGE(date1, date2) - Calculate age as interval between two dates
/// AGE(date) - Calculate age from date to current date
/// Returns VARCHAR representation of interval (e.g., "2 years 3 months 5 days")
pub(super) fn age(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("AGE requires 1 or 2 arguments, got {}", args.len()),
        ));
    }

    // Get the two dates to compare
    let (date1_str, date2_str) = if args.len() == 1 {
        // AGE(date) - compare with current date
        match &args[0] {
            types::SqlValue::Null => return Ok(types::SqlValue::Null),
            types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
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
            (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                return Ok(types::SqlValue::Null);
            }
            (
                types::SqlValue::Date(d1) | types::SqlValue::Timestamp(d1),
                types::SqlValue::Date(d2) | types::SqlValue::Timestamp(d2),
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

    Ok(types::SqlValue::Varchar(result))
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
) -> Result<types::SqlValue, ExecutorError> {
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

        Ok(types::SqlValue::Timestamp(new_timestamp.format("%Y-%m-%d %H:%M:%S").to_string()))
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
                return Ok(types::SqlValue::Date(date.format("%Y-%m-%d").to_string()));
            }
            _ => return Err(ExecutorError::UnsupportedFeature(
                format!("Unsupported date unit: {}", unit)
            )),
        };

        Ok(types::SqlValue::Date(new_date.format("%Y-%m-%d").to_string()))
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
