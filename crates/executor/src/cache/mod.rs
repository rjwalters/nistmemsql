//! Query plan caching module
//!
//! Provides query plan caching infrastructure to optimize repeated query patterns.
//! Queries with identical structure (different literals) reuse cached plans.

mod query_signature;
mod query_plan_cache;
pub mod parameterized;
pub mod integration;
pub mod table_extractor;

pub use query_signature::QuerySignature;
pub use query_plan_cache::{QueryPlanCache, CacheStats};
pub use parameterized::{ParameterizedPlan, LiteralValue, ParameterPosition, LiteralExtractor};
pub use integration::{CacheManager, CachedQueryContext};
pub use table_extractor::{extract_tables_from_select, extract_tables_from_statement};
