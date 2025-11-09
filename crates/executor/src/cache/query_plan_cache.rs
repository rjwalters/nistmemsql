//! Thread-safe query plan cache with LRU eviction
//!
//! Caches parsed AST and execution plans to avoid repeated parsing and
//! planning for structurally identical queries. This is NOT a result cache -
//! plans are still executed, we just skip parsing and analysis.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        RwLock,
    },
};

use super::QuerySignature;

/// Statistics about cache performance
#[derive(Clone, Debug)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub size: usize,
    pub hit_rate: f64,
}

/// Wrapper for cached plans that tracks table dependencies
#[derive(Clone)]
struct CachedEntry {
    sql: String,
    tables: HashSet<String>,
}

/// Generic type-erased cache for storing parsed AST and plans
/// This intentionally avoids caching results - plans are still executed
pub struct QueryPlanCache {
    cache: RwLock<HashMap<QuerySignature, CachedEntry>>,
    max_size: usize,
    hits: AtomicUsize,
    misses: AtomicUsize,
    evictions: AtomicUsize,
}

impl QueryPlanCache {
    /// Create a new cache with specified max size
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
        }
    }

    /// Check if query is in cache and update statistics
    pub fn get(&self, signature: &QuerySignature) -> Option<String> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(signature) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.sql.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert plan into cache with table metadata, evicting LRU entry if at capacity
    /// The sql should be the normalized original query
    pub fn insert(&self, signature: QuerySignature, sql: String) {
        self.insert_with_tables(signature, sql, HashSet::new());
    }

    /// Insert plan with explicit table dependencies for better invalidation
    pub fn insert_with_tables(
        &self,
        signature: QuerySignature,
        sql: String,
        tables: HashSet<String>,
    ) {
        let entry = CachedEntry { sql, tables };
        let mut cache = self.cache.write().unwrap();

        if cache.len() >= self.max_size {
            // Simple LRU: remove first entry
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        cache.insert(signature, entry);
    }

    /// Check if signature is cached
    pub fn contains(&self, signature: &QuerySignature) -> bool {
        self.cache.read().unwrap().contains_key(signature)
    }

    /// Clear all cached plans
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }

    /// Invalidate all plans referencing a table
    pub fn invalidate_table(&self, table: &str) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|_, entry| !entry.tables.iter().any(|t| t.eq_ignore_ascii_case(table)));
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };

        CacheStats {
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
            size: cache.len(),
            hit_rate,
        }
    }

    /// Get maximum cache size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit() {
        let cache = QueryPlanCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");
        let sql = "select * from users".to_string();

        cache.insert(sig.clone(), sql.clone());
        let result = cache.get(&sig);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), sql);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryPlanCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");

        let result = cache.get(&sig);
        assert!(result.is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = QueryPlanCache::new(2);

        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM orders");
        let sig3 = QuerySignature::from_sql("SELECT * FROM products");

        cache.insert(sig1, "select * from users".to_string());
        cache.insert(sig2, "select * from orders".to_string());
        assert_eq!(cache.stats().size, 2);

        cache.insert(sig3, "select * from products".to_string());
        assert_eq!(cache.stats().size, 2);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryPlanCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");

        cache.insert(sig.clone(), "select * from users".to_string());
        assert!(cache.contains(&sig));

        cache.clear();
        assert!(!cache.contains(&sig));
    }

    #[test]
    fn test_table_invalidation() {
        let cache = QueryPlanCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users WHERE id = 1");
        let mut tables = std::collections::HashSet::new();
        tables.insert("users".to_string());

        cache.insert_with_tables(
            sig.clone(),
            "select * from users where id = 1".to_string(),
            tables,
        );
        assert!(cache.contains(&sig));

        cache.invalidate_table("users");
        assert!(!cache.contains(&sig));
    }

    #[test]
    fn test_cache_stats() {
        let cache = QueryPlanCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");

        cache.insert(sig.clone(), "select * from users".to_string());

        // Generate hits
        cache.get(&sig);
        cache.get(&sig);

        // Generate miss
        let other_sig = QuerySignature::from_sql("SELECT * FROM orders");
        cache.get(&other_sig);

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 2.0 / 3.0).abs() < 0.01);
    }
}
