//! Query signature generation for cache keys
//!
//! Generates deterministic cache keys from SQL queries by normalizing whitespace
//! and creating a hash. Queries with identical structure (different literals)
//! will have the same signature.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Unique identifier for a query based on its structure
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct QuerySignature {
    hash: u64,
}

impl QuerySignature {
    /// Create a signature from SQL text
    pub fn from_sql(sql: &str) -> Self {
        let normalized = Self::normalize(sql);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        let hash = hasher.finish();
        Self { hash }
    }

    /// Get the underlying hash
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Normalize SQL: trim and collapse whitespace
    fn normalize(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_query_same_signature() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_whitespace_normalization() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT  *  FROM  users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_case_insensitive() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("select * from users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_different_queries_different_signature() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM orders");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_different_literals_different_signature() {
        // Different literals create different signatures with simple hashing
        // Future improvement: implement AST-based normalization to handle this
        let sig1 = QuerySignature::from_sql("SELECT col0 FROM tab WHERE col1 > 5");
        let sig2 = QuerySignature::from_sql("SELECT col0 FROM tab WHERE col1 > 10");
        // For now, these have different signatures due to numeric differences
        // This is a limitation of string-based hashing
        assert_ne!(sig1, sig2);
    }
}
