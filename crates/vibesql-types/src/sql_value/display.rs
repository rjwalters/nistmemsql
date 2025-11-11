//! Display implementation for SqlValue

use crate::sql_value::SqlValue;
use std::fmt;

/// Display implementation for SqlValue (how values are shown to users)
impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Smallint(i) => write!(f, "{}", i),
            SqlValue::Bigint(i) => write!(f, "{}", i),
            SqlValue::Unsigned(u) => write!(f, "{}", u),
            // Format Numeric - match MySQL formatting
            // MySQL displays whole numbers from integer arithmetic without decimal places
            SqlValue::Numeric(n) => {
                if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    if *n > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else if n.fract() == 0.0 && n.abs() < 1e15 {
                    // Whole number - display as integer (MySQL behavior for integer arithmetic)
                    // Only for reasonable range to avoid scientific notation
                    write!(f, "{:.0}", n)
                } else {
                    // Has fractional part - use default formatting (no trailing zeros)
                    write!(f, "{}", n)
                }
            }
            SqlValue::Float(n) => write!(f, "{}", n),
            SqlValue::Real(n) => write!(f, "{}", n),
            SqlValue::Double(n) => write!(f, "{}", n),
            SqlValue::Character(s) => write!(f, "{}", s),
            SqlValue::Varchar(s) => write!(f, "{}", s),
            SqlValue::Boolean(true) => write!(f, "TRUE"),
            SqlValue::Boolean(false) => write!(f, "FALSE"),
            SqlValue::Date(s) => write!(f, "{}", s),
            SqlValue::Time(s) => write!(f, "{}", s),
            SqlValue::Timestamp(s) => write!(f, "{}", s),
            SqlValue::Interval(s) => write!(f, "{}", s),
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_display_whole_numbers() {
        // MySQL compatibility: whole numbers display without decimals
        assert_eq!(format!("{}", SqlValue::Numeric(32.0)), "32");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.0)), "-4373");
        assert_eq!(format!("{}", SqlValue::Numeric(0.0)), "0");
        assert_eq!(format!("{}", SqlValue::Numeric(164.0)), "164");
    }

    #[test]
    fn test_numeric_display_fractional() {
        // Fractional values display with decimals
        assert_eq!(format!("{}", SqlValue::Numeric(32.5)), "32.5");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.123)), "-4373.123");
        assert_eq!(format!("{}", SqlValue::Numeric(0.5)), "0.5");
    }

    #[test]
    fn test_numeric_display_special_values() {
        // Special values
        assert_eq!(format!("{}", SqlValue::Numeric(f64::NAN)), "NaN");
        assert_eq!(format!("{}", SqlValue::Numeric(f64::INFINITY)), "Infinity");
        assert_eq!(format!("{}", SqlValue::Numeric(f64::NEG_INFINITY)), "-Infinity");
    }
}
