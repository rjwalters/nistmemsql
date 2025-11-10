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
            // Format Numeric - match SQLite formatting with .000 for whole numbers
            // This ensures aggregate functions (SUM, AVG) match SQLite output exactly
            SqlValue::Numeric(n) => {
                if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    if *n > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else if n.fract() == 0.0 {
                    // Whole number - display with .000 to match SQLite format
                    write!(f, "{:.3}", n)
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
