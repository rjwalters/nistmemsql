//! Index Module - Index implementations for vibesql storage
//!
//! This module provides various index implementations:
//! - Spatial indexes using R-trees for spatial queries

pub mod spatial;

pub use spatial::{SpatialIndex, SpatialIndexEntry, extract_mbr_from_sql_value};
