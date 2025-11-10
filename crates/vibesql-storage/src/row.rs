use vibesql_types::SqlValue;

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
}
