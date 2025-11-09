//! Parameterized plan support for literal extraction and binding
//!
//! Allows queries with different literal values to share the same execution plan
//! by extracting literals into parameters and binding them at execution time.

/// Type-safe literal value representation
#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Integer(i64),
    String(String),
    Boolean(bool),
    Null,
}

impl LiteralValue {
    /// Convert to SQL string representation
    pub fn to_sql(&self) -> String {
        match self {
            LiteralValue::Integer(n) => n.to_string(),
            LiteralValue::String(s) => format!("'{}'", s.replace("'", "''")),
            LiteralValue::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
            LiteralValue::Null => "NULL".to_string(),
        }
    }
}

/// Position of a parameter in a query
#[derive(Clone, Debug)]
pub struct ParameterPosition {
    pub position: usize,
    pub context: String,
}

/// Query plan with parameter placeholders
#[derive(Clone, Debug)]
pub struct ParameterizedPlan {
    pub normalized_query: String,
    pub param_positions: Vec<ParameterPosition>,
    pub literal_values: Vec<LiteralValue>,
}

impl ParameterizedPlan {
    /// Create a new parameterized plan
    pub fn new(
        normalized_query: String,
        param_positions: Vec<ParameterPosition>,
        literal_values: Vec<LiteralValue>,
    ) -> Self {
        Self {
            normalized_query,
            param_positions,
            literal_values,
        }
    }

    /// Bind literal values to create executable query
    pub fn bind(&self, values: &[LiteralValue]) -> Result<String, String> {
        if values.len() != self.param_positions.len() {
            return Err(format!(
                "Expected {} parameters, got {}",
                self.param_positions.len(),
                values.len()
            ));
        }

        let mut result = self.normalized_query.clone();
        let mut offset = 0;

        for (i, value) in values.iter().enumerate() {
            if let Some(_pos) = self.param_positions.get(i) {
                let sql_value = value.to_sql();
                let placeholder = "?";

                if let Some(idx) = result[offset..].find(placeholder) {
                    let insert_pos = offset + idx;
                    result.replace_range(insert_pos..insert_pos + 1, &sql_value);
                    offset = insert_pos + sql_value.len();
                }
            }
        }

        Ok(result)
    }

    /// Get a comparison key for the plan
    pub fn comparison_key(&self) -> String {
        self.normalized_query.clone()
    }
}

/// Utility for extracting literals from queries
pub struct LiteralExtractor;

impl LiteralExtractor {
    /// Extract literals from a query (placeholder implementation)
    pub fn extract(query: &str) -> (String, Vec<LiteralValue>) {
        // Simple implementation: just return the query as-is
        // Full implementation would parse and extract actual literals
        (query.to_string(), Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_value_to_string() {
        assert_eq!(LiteralValue::Integer(42).to_sql(), "42");
        assert_eq!(LiteralValue::String("hello".to_string()).to_sql(), "'hello'");
        assert_eq!(LiteralValue::Boolean(true).to_sql(), "true");
        assert_eq!(LiteralValue::Null.to_sql(), "NULL");
    }

    #[test]
    fn test_literal_value_string_escape() {
        assert_eq!(
            LiteralValue::String("it's".to_string()).to_sql(),
            "'it''s'"
        );
    }

    #[test]
    fn test_parameterized_plan_bind() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE age > ?".to_string(),
            vec![ParameterPosition {
                position: 40,
                context: "age".to_string(),
            }],
            vec![LiteralValue::Integer(25)],
        );

        let result = plan.bind(&[LiteralValue::Integer(30)]).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE age > 30");
    }

    #[test]
    fn test_parameterized_plan_bind_string() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE name = ?".to_string(),
            vec![ParameterPosition {
                position: 40,
                context: "name".to_string(),
            }],
            vec![LiteralValue::String("John".to_string())],
        );

        let result = plan.bind(&[LiteralValue::String("Jane".to_string())]).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE name = 'Jane'");
    }

    #[test]
    fn test_parameterized_plan_bind_error() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE age > ?".to_string(),
            vec![ParameterPosition {
                position: 40,
                context: "age".to_string(),
            }],
            vec![LiteralValue::Integer(25)],
        );

        let result = plan.bind(&[LiteralValue::Integer(30), LiteralValue::Integer(40)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_comparison_key() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users".to_string(),
            vec![],
            vec![],
        );

        assert_eq!(plan.comparison_key(), "SELECT * FROM users");
    }
}
