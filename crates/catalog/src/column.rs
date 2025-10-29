/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: types::DataType,
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: types::DataType, nullable: bool) -> Self {
        ColumnSchema { name, data_type, nullable }
    }

    /// Set the nullable property
    pub fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable;
    }
}
