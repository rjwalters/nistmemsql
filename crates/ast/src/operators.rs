//! Operator enums for SQL expressions

/// Binary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Concat, // ||

            // TODO: Add more (LIKE, IN, etc.)
}

/// Unary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,       // NOT
    Minus,     // - (negation)
    Plus,      // + (unary plus)
    IsNull,    // IS NULL
    IsNotNull, // IS NOT NULL
}
