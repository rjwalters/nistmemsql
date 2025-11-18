//! TPC-H Data Generator
//!
//! This module provides data generation utilities for TPC-H benchmark tables.
//! It includes constants for reference data (nations, regions) and a data
//! generator that produces deterministic pseudo-random data based on scale factor.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

pub const NATIONS: &[(&str, usize)] = &[
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
];

pub const REGIONS: &[&str] = &["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
pub const SEGMENTS: &[&str] = &["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"];
pub const PRIORITIES: &[&str] = &["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
pub const SHIP_MODES: &[&str] = &["AIR", "AIR REG", "MAIL", "RAIL", "SHIP", "TRUCK", "FOB"];

pub struct TPCHData {
    pub scale_factor: f64,
    pub customer_count: usize,
    pub orders_count: usize,
    pub lineitem_count: usize,
    pub supplier_count: usize,
    rng: ChaCha8Rng,
}

impl TPCHData {
    pub fn new(scale_factor: f64) -> Self {
        let customer_count = ((150_000.0 * scale_factor) as usize).max(100);
        let orders_count = ((1_500_000.0 * scale_factor) as usize).max(1000);
        let lineitem_count = ((6_000_000.0 * scale_factor) as usize).max(4000);
        let supplier_count = ((10_000.0 * scale_factor) as usize).max(10);

        Self {
            scale_factor,
            customer_count,
            orders_count,
            lineitem_count,
            supplier_count,
            rng: ChaCha8Rng::seed_from_u64(42), // Deterministic
        }
    }

    pub fn random_varchar(&mut self, max_len: usize) -> String {
        let len = self.rng.gen_range(10..max_len);
        (0..len)
            .map(|_| self.rng.sample(rand::distributions::Alphanumeric) as char)
            .collect()
    }

    pub fn random_phone(&mut self, nation_key: usize) -> String {
        format!(
            "{:02}-{:03}-{:03}-{:04}",
            10 + nation_key,
            self.rng.gen_range(100..1000),
            self.rng.gen_range(100..1000),
            self.rng.gen_range(1000..10000)
        )
    }

    pub fn random_date(&mut self, _start: &str, _end: &str) -> String {
        // Simple date generation between start and end
        let year = self.rng.gen_range(1992..1999);
        let month = self.rng.gen_range(1..13);
        let day = self.rng.gen_range(1..29); // Simplified
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}
