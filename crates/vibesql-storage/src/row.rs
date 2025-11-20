use vibesql_types::{Date, SqlValue};

/// A single row of data - vector of SqlValues
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<SqlValue>,
}

impl Row {
    /// Create a new row from values
    pub fn new(values: Vec<SqlValue>) -> Self {
        Row { values }
    }

    /// Get value at column index
    pub fn get(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }

    /// Get number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Set value at column index
    pub fn set(&mut self, index: usize, value: SqlValue) -> Result<(), crate::StorageError> {
        if index >= self.values.len() {
            return Err(crate::StorageError::ColumnIndexOutOfBounds { index });
        }
        self.values[index] = value;
        Ok(())
    }

    /// Add a value to the end of the row
    pub fn add_value(&mut self, value: SqlValue) {
        self.values.push(value);
    }

    /// Remove a value at the specified index
    pub fn remove_value(&mut self, index: usize) -> Result<SqlValue, crate::StorageError> {
        if index >= self.values.len() {
            return Err(crate::StorageError::ColumnIndexOutOfBounds { index });
        }
        Ok(self.values.remove(index))
    }

    // ========================================================================
    // Type-specialized unchecked accessors for monomorphic execution paths
    //
    // SAFETY: These methods bypass enum tag checks for performance.
    // Caller MUST guarantee the column type matches the accessor type.
    // Debug builds include assertions to catch type mismatches.
    // ========================================================================

    /// Get f64 value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Double or Float variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_f64_unchecked(&self, idx: usize) -> f64 {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Double(_) | SqlValue::Float(_)),
            "get_f64_unchecked called on non-float value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Double(d) => *d,
            SqlValue::Float(f) => *f as f64,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get i64 value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is an Integer, Bigint, or Smallint variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_i64_unchecked(&self, idx: usize) -> i64 {
        debug_assert!(
            matches!(
                self.values[idx],
                SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_)
            ),
            "get_i64_unchecked called on non-integer value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Integer(i) | SqlValue::Bigint(i) => *i,
            SqlValue::Smallint(s) => *s as i64,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get Date value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Date variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_date_unchecked(&self, idx: usize) -> Date {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Date(_)),
            "get_date_unchecked called on non-date value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Date(d) => *d,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get bool value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Boolean variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_bool_unchecked(&self, idx: usize) -> bool {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Boolean(_)),
            "get_bool_unchecked called on non-boolean value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Boolean(b) => *b,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get string value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Varchar or Character variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_string_unchecked(&self, idx: usize) -> &str {
        debug_assert!(
            matches!(
                self.values[idx],
                SqlValue::Varchar(_) | SqlValue::Character(_)
            ),
            "get_string_unchecked called on non-string value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s,
            _ => std::hint::unreachable_unchecked(),
        }
    }
}
