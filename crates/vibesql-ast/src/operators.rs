//! Operator enums for SQL expressions

/// Binary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    IntegerDivide, // DIV (MySQL-specific integer division)
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
    Concat, /* || */
            /* Note: LIKE and IN are not simple binary operators. They are implemented
             * as Expression variants in expression.rs due to their complex structure:
             * - LIKE: Pattern matching with wildcards (%, _)
             * - IN: Subquery or value list support */
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
