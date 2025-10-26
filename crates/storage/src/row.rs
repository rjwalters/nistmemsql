use types::SqlValue;

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
}
