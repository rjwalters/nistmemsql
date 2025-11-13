
//! TPC-H Benchmark Suite - Native Rust Implementation
//!
//! This benchmark compares pure SQL engine performance across:
//! - VibeSQL (native Rust API)
//! - SQLite (via rusqlite)
//! - DuckDB (via duckdb-rs)
//!
//! All measurements are done in-memory with no Python/FFI overhead.
//!
//! Usage:
//!   cargo bench --bench tpch_benchmark
//!   cargo bench --bench tpch_benchmark -- --baseline=main
//!   cargo bench --bench tpch_benchmark -- q1  # Run only Q1
//!
//! Scale factors:
//!   SF 0.01 (10MB) - Fast iteration
//!   SF 0.1 (100MB) - Realistic testing
//!   SF 1.0 (1GB) - Full benchmark

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;
use vibesql_types::Date;
use rusqlite::Connection as SqliteConn;
use duckdb::Connection as DuckDBConn;
use rand::Rng;
use std::str::FromStr;
use std::time::Duration;

// =============================================================================
// TPC-H Data Generator
// =============================================================================

mod tpch_data {
    use super::*;
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
}

// =============================================================================
// Database Loaders
// =============================================================================

fn load_vibesql(scale_factor: f64) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = tpch_data::TPCHData::new(scale_factor);

    // Create schema
    create_tpch_schema_vibesql(&mut db);

    // Load data
    load_region_vibesql(&mut db);
    load_nation_vibesql(&mut db);
    load_customer_vibesql(&mut db, &mut data);
    load_supplier_vibesql(&mut db, &mut data);
    load_orders_vibesql(&mut db, &mut data);
    load_lineitem_vibesql(&mut db, &mut data);

    db
}

fn load_sqlite(scale_factor: f64) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = tpch_data::TPCHData::new(scale_factor);

    // Create schema
    create_tpch_schema_sqlite(&conn);

    // Load data
    load_region_sqlite(&conn);
    load_nation_sqlite(&conn);
    load_customer_sqlite(&conn, &mut data);
    load_supplier_sqlite(&conn, &mut data);
    load_orders_sqlite(&conn, &mut data);
    load_lineitem_sqlite(&conn, &mut data);

    conn
}

fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = tpch_data::TPCHData::new(scale_factor);

    // Create schema
    create_tpch_schema_duckdb(&conn);

    // Load data
    load_region_duckdb(&conn);
    load_nation_duckdb(&conn);
    load_customer_duckdb(&conn, &mut data);
    load_supplier_duckdb(&conn, &mut data);
    load_orders_duckdb(&conn, &mut data);
    load_lineitem_duckdb(&conn, &mut data);

    conn
}

// =============================================================================
// Schema Creation
// =============================================================================

fn create_tpch_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{TableSchema, ColumnSchema};
    use vibesql_types::DataType;

    // REGION table
    db.create_table(TableSchema::new(
        "REGION".to_string(),
        vec![
            ColumnSchema {
                name: "R_REGIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "R_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "R_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(152) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();

    // NATION table
    db.create_table(TableSchema::new(
        "NATION".to_string(),
        vec![
            ColumnSchema {
                name: "N_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_REGIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(152) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::new(
        "CUSTOMER".to_string(),
        vec![
            ColumnSchema {
                name: "C_CUSTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_ADDRESS".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_PHONE".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_ACCTBAL".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_MKTSEGMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(117) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();

    // ORDERS table
    db.create_table(TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema {
                name: "O_ORDERKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_CUSTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERSTATUS".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_TOTALPRICE".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERPRIORITY".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_CLERK".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_SHIPPRIORITY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(79) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();

    // LINEITEM table
    db.create_table(TableSchema::new(
        "LINEITEM".to_string(),
        vec![
            ColumnSchema {
                name: "L_ORDERKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_PARTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SUPPKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_LINENUMBER".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_QUANTITY".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_EXTENDEDPRICE".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_DISCOUNT".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_TAX".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_RETURNFLAG".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_LINESTATUS".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_COMMITDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_RECEIPTDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPINSTRUCT".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPMODE".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(44) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();

    // SUPPLIER table
    db.create_table(TableSchema::new(
        "SUPPLIER".to_string(),
        vec![
            ColumnSchema {
                name: "S_SUPPKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_ADDRESS".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_PHONE".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_ACCTBAL".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(101) },
                nullable: true,
                default_value: None,
            },
        ],
    )).unwrap();
}

fn create_tpch_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(r#"
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT NOT NULL,
            r_comment TEXT
        );

        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment TEXT
        );

        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name TEXT NOT NULL,
            c_address TEXT NOT NULL,
            c_nationkey INTEGER NOT NULL,
            c_phone TEXT NOT NULL,
            c_acctbal REAL NOT NULL,
            c_mktsegment TEXT NOT NULL,
            c_comment TEXT
        );

        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER NOT NULL,
            o_orderstatus TEXT NOT NULL,
            o_totalprice REAL NOT NULL,
            o_orderdate TEXT NOT NULL,
            o_orderpriority TEXT NOT NULL,
            o_clerk TEXT NOT NULL,
            o_shippriority INTEGER NOT NULL,
            o_comment TEXT
        );

        CREATE TABLE lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity REAL NOT NULL,
            l_extendedprice REAL NOT NULL,
            l_discount REAL NOT NULL,
            l_tax REAL NOT NULL,
            l_returnflag TEXT NOT NULL,
            l_linestatus TEXT NOT NULL,
            l_shipdate TEXT NOT NULL,
            l_commitdate TEXT NOT NULL,
            l_receiptdate TEXT NOT NULL,
            l_shipinstruct TEXT NOT NULL,
            l_shipmode TEXT NOT NULL,
            l_comment TEXT,
            PRIMARY KEY (l_orderkey, l_linenumber)
        );

        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name TEXT NOT NULL,
            s_address TEXT NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone TEXT NOT NULL,
            s_acctbal REAL NOT NULL,
            s_comment TEXT
        );
    "#).unwrap();
}

fn create_tpch_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(r#"
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name VARCHAR(25) NOT NULL,
            r_comment VARCHAR(152)
        );

        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name VARCHAR(25) NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment VARCHAR(152)
        );

        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25) NOT NULL,
            c_address VARCHAR(40) NOT NULL,
            c_nationkey INTEGER NOT NULL,
            c_phone VARCHAR(15) NOT NULL,
            c_acctbal DECIMAL(15,2) NOT NULL,
            c_mktsegment VARCHAR(10) NOT NULL,
            c_comment VARCHAR(117)
        );

        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER NOT NULL,
            o_orderstatus VARCHAR(1) NOT NULL,
            o_totalprice DECIMAL(15,2) NOT NULL,
            o_orderdate DATE NOT NULL,
            o_orderpriority VARCHAR(15) NOT NULL,
            o_clerk VARCHAR(15) NOT NULL,
            o_shippriority INTEGER NOT NULL,
            o_comment VARCHAR(79)
        );

        CREATE TABLE lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity DECIMAL(15,2) NOT NULL,
            l_extendedprice DECIMAL(15,2) NOT NULL,
            l_discount DECIMAL(15,2) NOT NULL,
            l_tax DECIMAL(15,2) NOT NULL,
            l_returnflag VARCHAR(1) NOT NULL,
            l_linestatus VARCHAR(1) NOT NULL,
            l_shipdate DATE NOT NULL,
            l_commitdate DATE NOT NULL,
            l_receiptdate DATE NOT NULL,
            l_shipinstruct VARCHAR(25) NOT NULL,
            l_shipmode VARCHAR(10) NOT NULL,
            l_comment VARCHAR(44),
            PRIMARY KEY (l_orderkey, l_linenumber)
        );

        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25) NOT NULL,
            s_address VARCHAR(40) NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone VARCHAR(15) NOT NULL,
            s_acctbal DECIMAL(15,2) NOT NULL,
            s_comment VARCHAR(101)
        );
    "#).unwrap();
}

// =============================================================================
// Data Loading (REGION - simple reference data)
// =============================================================================

fn load_region_vibesql(db: &mut VibeDB) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for (i, &name) in tpch_data::REGIONS.iter().enumerate() {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(name.to_string()),
            SqlValue::Varchar("comment".to_string()),
        ]);
        db.insert_row("REGION", row).unwrap();
    }
}

fn load_region_sqlite(conn: &SqliteConn) {
    for (i, &name) in tpch_data::REGIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO region VALUES (?, ?, ?)",
            rusqlite::params![i as i64, name, "comment"],
        ).unwrap();
    }
}

fn load_region_duckdb(conn: &DuckDBConn) {
    for (i, &name) in tpch_data::REGIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO region VALUES (?, ?, ?)",
            duckdb::params![i as i64, name, "comment"],
        ).unwrap();
    }
}

// =============================================================================
// Data Loading (NATION - simple reference data)
// =============================================================================

fn load_nation_vibesql(db: &mut VibeDB) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for (i, &(name, region_key)) in tpch_data::NATIONS.iter().enumerate() {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(name.to_string()),
            SqlValue::Integer(region_key as i64),
            SqlValue::Varchar("comment".to_string()),
        ]);
        db.insert_row("NATION", row).unwrap();
    }
}

fn load_nation_sqlite(conn: &SqliteConn) {
    for (i, &(name, region_key)) in tpch_data::NATIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO nation VALUES (?, ?, ?, ?)",
            rusqlite::params![i as i64, name, region_key as i64, "comment"],
        ).unwrap();
    }
}

fn load_nation_duckdb(conn: &DuckDBConn) {
    for (i, &(name, region_key)) in tpch_data::NATIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO nation VALUES (?, ?, ?, ?)",
            duckdb::params![i as i64, name, region_key as i64, "comment"],
        ).unwrap();
    }
}

// =============================================================================
// Data Loading (CUSTOMER - generated data)
// =============================================================================

fn load_customer_vibesql(db: &mut VibeDB, data: &mut tpch_data::TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;
    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Varchar(format!("Customer#{:09}", i + 1)),
            SqlValue::Varchar(data.random_varchar(40)),
            SqlValue::Integer(nation_key as i64),
            SqlValue::Varchar(data.random_phone(nation_key)),
            SqlValue::Numeric(acctbal as f64),
            SqlValue::Varchar(tpch_data::SEGMENTS[i % tpch_data::SEGMENTS.len()].to_string()),
            SqlValue::Varchar(data.random_varchar(117)),
        ]);
        db.insert_row("CUSTOMER", row).unwrap();
    }
}

fn load_customer_sqlite(conn: &SqliteConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        stmt.execute(rusqlite::params![
            i as i64 + 1,
            format!("Customer#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            tpch_data::SEGMENTS[i % tpch_data::SEGMENTS.len()],
            data.random_varchar(117),
        ]).unwrap();
    }
}

fn load_customer_duckdb(conn: &DuckDBConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        stmt.execute(duckdb::params![
            i as i64 + 1,
            format!("Customer#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            tpch_data::SEGMENTS[i % tpch_data::SEGMENTS.len()],
            data.random_varchar(117),
        ]).unwrap();
    }
}

// =============================================================================
// Data Loading (SUPPLIER - generated data)
// =============================================================================

fn load_supplier_vibesql(db: &mut VibeDB, data: &mut tpch_data::TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;
    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Varchar(format!("Supplier#{:09}", i + 1)),
            SqlValue::Varchar(data.random_varchar(40)),
            SqlValue::Integer(nation_key as i64),
            SqlValue::Varchar(data.random_phone(nation_key)),
            SqlValue::Numeric(acctbal as f64),
            SqlValue::Varchar(data.random_varchar(101)),
        ]);
        db.insert_row("SUPPLIER", row).unwrap();
    }
}

fn load_supplier_sqlite(conn: &SqliteConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        stmt.execute(rusqlite::params![
            i as i64 + 1,
            format!("Supplier#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            data.random_varchar(101),
        ]).unwrap();
    }
}

fn load_supplier_duckdb(conn: &DuckDBConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        stmt.execute(duckdb::params![
            i as i64 + 1,
            format!("Supplier#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            data.random_varchar(101),
        ]).unwrap();
    }
}

// =============================================================================
// Data Loading (ORDERS - generated data)
// =============================================================================

fn load_orders_vibesql(db: &mut VibeDB, data: &mut tpch_data::TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;
    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Integer(cust_key as i64),
            SqlValue::Varchar(["O", "F", "P"][i % 3].to_string()),
            SqlValue::Numeric(totalprice as f64),
            SqlValue::Date(Date::from_str(&order_date).unwrap()),
            SqlValue::Varchar(tpch_data::PRIORITIES[i % tpch_data::PRIORITIES.len()].to_string()),
            SqlValue::Varchar(format!("Clerk#{:09}", (i * 7) % 1000 + 1)),
            SqlValue::Integer(0),
            SqlValue::Varchar(data.random_varchar(79)),
        ]);
        db.insert_row("ORDERS", row).unwrap();
    }
}

fn load_orders_sqlite(conn: &SqliteConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        stmt.execute(rusqlite::params![
            i as i64 + 1,
            cust_key as i64,
            ["O", "F", "P"][i % 3],
            totalprice,
            order_date,
            tpch_data::PRIORITIES[i % tpch_data::PRIORITIES.len()],
            format!("Clerk#{:09}", (i * 7) % 1000 + 1),
            0,
            data.random_varchar(79),
        ]).unwrap();
    }
}

fn load_orders_duckdb(conn: &DuckDBConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        stmt.execute(duckdb::params![
            i as i64 + 1,
            cust_key as i64,
            ["O", "F", "P"][i % 3],
            totalprice,
            order_date,
            tpch_data::PRIORITIES[i % tpch_data::PRIORITIES.len()],
            format!("Clerk#{:09}", (i * 7) % 1000 + 1),
            0,
            data.random_varchar(79),
        ]).unwrap();
    }
}

// =============================================================================
// Data Loading (LINEITEM - generated data, largest table)
// =============================================================================

fn load_lineitem_vibesql(db: &mut VibeDB, data: &mut tpch_data::TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;
    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1; // 1-7 lines per order

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            let row = Row::new(vec![
                SqlValue::Integer(order_num as i64),
                SqlValue::Integer(((line_id * 13) % 200000 + 1) as i64),
                SqlValue::Integer(((line_id * 17) % data.supplier_count + 1) as i64),
                SqlValue::Integer(line_num as i64),
                SqlValue::Numeric(quantity as f64),
                SqlValue::Numeric(extendedprice as f64),
                SqlValue::Numeric(discount as f64),
                SqlValue::Numeric(tax as f64),
                SqlValue::Varchar(["N", "R", "A"][line_id % 3].to_string()),
                SqlValue::Varchar(["O", "F"][line_id % 2].to_string()),
                SqlValue::Date(Date::from_str(&ship_date).unwrap()),
                SqlValue::Date(Date::from_str(&commit_date).unwrap()),
                SqlValue::Date(Date::from_str(&receipt_date).unwrap()),
                SqlValue::Varchar("DELIVER IN PERSON".to_string()),
                SqlValue::Varchar(tpch_data::SHIP_MODES[line_id % tpch_data::SHIP_MODES.len()].to_string()),
                SqlValue::Varchar(data.random_varchar(44)),
            ]);
            db.insert_row("LINEITEM", row).unwrap();

            line_id += 1;
        }
    }
}

fn load_lineitem_sqlite(conn: &SqliteConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1;

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            stmt.execute(rusqlite::params![
                order_num as i64,
                ((line_id * 13) % 200000 + 1) as i64,
                ((line_id * 17) % data.supplier_count + 1) as i64,
                line_num as i64,
                quantity,
                extendedprice,
                discount,
                tax,
                ["N", "R", "A"][line_id % 3],
                ["O", "F"][line_id % 2],
                ship_date,
                commit_date,
                receipt_date,
                "DELIVER IN PERSON",
                tpch_data::SHIP_MODES[line_id % tpch_data::SHIP_MODES.len()],
                data.random_varchar(44),
            ]).unwrap();

            line_id += 1;
        }
    }
}

fn load_lineitem_duckdb(conn: &DuckDBConn, data: &mut tpch_data::TPCHData) {
    let mut stmt = conn.prepare(
        "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1;

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            stmt.execute(duckdb::params![
                order_num as i64,
                ((line_id * 13) % 200000 + 1) as i64,
                ((line_id * 17) % data.supplier_count + 1) as i64,
                line_num as i64,
                quantity,
                extendedprice,
                discount,
                tax,
                ["N", "R", "A"][line_id % 3],
                ["O", "F"][line_id % 2],
                ship_date,
                commit_date,
                receipt_date,
                "DELIVER IN PERSON",
                tpch_data::SHIP_MODES[line_id % tpch_data::SHIP_MODES.len()],
                data.random_varchar(44),
            ]).unwrap();

            line_id += 1;
        }
    }
}

// =============================================================================
// TPC-H Query Benchmarks
// =============================================================================

// TPC-H Q1: Pricing Summary Report
const TPCH_Q1: &str = r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"#;


const TPCH_Q1: &str = r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"#;

const TPCH_Q6: &str = r#"
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
"#;

const TPCH_Q2: &str = r#"
SELECT
    s_acctbal,
    s_name,
    n_name,
    s_address,
    s_phone,
    s_comment
FROM supplier, nation, region
WHERE s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC
LIMIT 100
"#;

const TPCH_Q3: &str = r#"
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"#;

const TPCH_Q4: &str = r#"
SELECT
    o_orderpriority,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
    AND o_orderdate < '1993-10-01'
    AND EXISTS (
        SELECT *
        FROM lineitem
        WHERE l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"#;

const TPCH_Q5: &str = r#"
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
"#;

const TPCH_Q7: &str = r#"
SELECT
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    SUBSTR(l_shipdate, 1, 4) as l_year,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
         OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate >= '1995-01-01'
    AND l_shipdate <= '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"#;

const TPCH_Q8: &str = r#"
SELECT
    SUBSTR(o_orderdate, 1, 4) as o_year,
    SUM(CASE WHEN n2.n_name = 'BRAZIL'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendprice * (1 - l_discount)) as mkt_share
FROM lineitem, orders, customer, nation n1, nation n2, region, supplier
WHERE l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND l_suppkey = s_suppkey
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate >= '1995-01-01'
    AND o_orderdate <= '1996-12-31'
GROUP BY o_year
ORDER BY o_year
"#;

const TPCH_Q9: &str = r#"
SELECT
    n_name as nation,
    SUBSTR(o_orderdate, 1, 4) as o_year,
    SUM(l_extendedprice * (1 - l_discount)) as sum_profit
FROM lineitem, orders, nation, supplier
WHERE l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"#;

const TPCH_Q10: &str = r#"
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"#;

const TPCH_Q11: &str = r#"
SELECT
    s_suppkey,
    SUM(s_acctbal) as total_value
FROM supplier, nation
WHERE s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY s_suppkey
HAVING SUM(s_acctbal) > (
    SELECT SUM(s_acctbal) * 0.0001
    FROM supplier, nation
    WHERE s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
)
ORDER BY total_value DESC
"#;

const TPCH_Q12: &str = r#"
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        THEN 1 ELSE 0 END) as high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        THEN 1 ELSE 0 END) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= '1994-01-01'
    AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
"#;

const TPCH_Q13: &str = r#"
SELECT
    c_count,
    COUNT(*) as custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) as c_count
    FROM customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"#;

const TPCH_Q14: &str = r#"
SELECT
    100.00 * SUM(CASE WHEN l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem
WHERE l_shipdate >= '1995-09-01'
    AND l_shipdate < '1995-10-01'
"#;

const TPCH_Q15: &str = r#"
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= '1996-01-01'
        AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey
) revenue
WHERE s_suppkey = supplier_no
    AND total_revenue = (
        SELECT MAX(total_revenue)
        FROM (
            SELECT SUM(l_extendedprice * (1 - l_discount)) as total_revenue
            FROM lineitem
            WHERE l_shipdate >= '1996-01-01'
                AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        ) max_revenue
    )
ORDER BY s_suppkey
"#;

const TPCH_Q16: &str = r#"
SELECT
    COUNT(DISTINCT s_suppkey) as supplier_cnt
FROM supplier
WHERE s_suppkey NOT IN (
    SELECT s_suppkey
    FROM supplier
    WHERE s_comment LIKE '%Customer%Complaints%'
)
GROUP BY s_nationkey
ORDER BY supplier_cnt DESC, s_nationkey
LIMIT 1
"#;

const TPCH_Q17: &str = r#"
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem
WHERE l_quantity < (
    SELECT 0.2 * AVG(l_quantity)
    FROM lineitem
)
"#;

const TPCH_Q18: &str = r#"
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) as total_qty
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
HAVING SUM(l_quantity) > 300
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"#;

const TPCH_Q19: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem
WHERE l_quantity >= 1
    AND l_quantity <= 30
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
"#;

const TPCH_Q20: &str = r#"
SELECT
    s_name,
    s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT l_suppkey
    FROM lineitem
    WHERE l_shipdate >= '1994-01-01'
        AND l_shipdate < '1995-01-01'
    GROUP BY l_suppkey
    HAVING SUM(l_quantity) > 50
)
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
"#;

const TPCH_Q21: &str = r#"
SELECT
    s_name,
    COUNT(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"#;

const TPCH_Q22: &str = r#"
SELECT
    SUBSTR(c_phone, 1, 2) as cntrycode,
    COUNT(*) as numcust,
    SUM(c_acctbal) as totacctbal
FROM customer
WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > (
        SELECT AVG(c_acctbal)
        FROM customer
        WHERE c_acctbal > 0.00
            AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    )
    AND NOT EXISTS (
        SELECT *
        FROM orders
        WHERE o_custkey = c_custkey
    )
GROUP BY cntrycode
ORDER BY cntrycode
"#;

// =============================================================================
// Benchmark Helper Functions
// =============================================================================

/// Helper function to benchmark a query on VibeSQL
fn benchmark_vibesql_query(c: &mut Criterion, name: &str, sql: &str) {
    let db = load_vibesql(0.01);
    
    c.bench_function(name, |b| {
        b.iter(|| {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::Select(select) = stmt {
                let executor = SelectExecutor::new(&db);
                let result = executor.execute(&select).unwrap();
                let count = result.len();
                black_box(count);
            }
        });
    });
}

/// Helper function to benchmark a query on VibeSQL with benchmark groups (for Q1)
fn benchmark_vibesql_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let db = load_vibesql(sf);

        group.bench_with_input(BenchmarkId::new("vibesql", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let stmt = Parser::parse_sql(sql).unwrap();
                if let vibesql_ast::Statement::Select(select) = stmt {
                    let executor = SelectExecutor::new(&db);
                    let result = executor.execute(&select).unwrap();
                    black_box(result.len());
                }
            });
        });
    }

    group.finish();
}

/// Helper function to benchmark a query on SQLite
fn benchmark_sqlite_query(c: &mut Criterion, name: &str, sql: &str) {
    let conn = load_sqlite(0.01);
    
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while rows.next().unwrap().is_some() {
                count += 1;
            }
            black_box(count);
        });
    });
}

/// Helper function to benchmark a query on SQLite with benchmark groups (for Q1)
fn benchmark_sqlite_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let conn = load_sqlite(sf);

        group.bench_with_input(BenchmarkId::new("sqlite", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let mut stmt = conn.prepare(sql).unwrap();
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                }).unwrap();

                let mut count = 0;
                for _ in rows {
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

/// Helper function to benchmark a query on DuckDB
fn benchmark_duckdb_query(c: &mut Criterion, name: &str, sql: &str) {
    let conn = load_duckdb(0.01);
    
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while rows.next().unwrap().is_some() {
                count += 1;
            }
            black_box(count);
        });
    });
}

/// Helper function to benchmark a query on DuckDB with benchmark groups (for Q1)
fn benchmark_duckdb_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let conn = load_duckdb(sf);

        group.bench_with_input(BenchmarkId::new("duckdb", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let mut stmt = conn.prepare(sql).unwrap();
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                }).unwrap();

                let mut count = 0;
                for _ in rows {
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

// =============================================================================
// Macro to Generate Benchmark Functions
// =============================================================================

/// Macro to generate three benchmark functions (vibesql, sqlite, duckdb) for a given query
macro_rules! tpch_benchmark {
    ($query_num:expr, $sql:expr) => {
        paste::paste! {
            fn [<benchmark_q $query_num _vibesql>](c: &mut Criterion) {
                benchmark_vibesql_query(c, concat!("tpch_q", stringify!($query_num), "_vibesql"), $sql);
            }
            
            fn [<benchmark_q $query_num _sqlite>](c: &mut Criterion) {
                benchmark_sqlite_query(c, concat!("tpch_q", stringify!($query_num), "_sqlite"), $sql);
            }
            
            fn [<benchmark_q $query_num _duckdb>](c: &mut Criterion) {
                benchmark_duckdb_query(c, concat!("tpch_q", stringify!($query_num), "_duckdb"), $sql);
            }
        }
    };
}

/// Special macro for Q1 which uses grouped benchmarks
macro_rules! tpch_benchmark_grouped {
    ($query_num:expr, $sql:expr) => {
        paste::paste! {
            fn [<benchmark_q $query_num _vibesql>](c: &mut Criterion) {
                benchmark_vibesql_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }
            
            fn [<benchmark_q $query_num _sqlite>](c: &mut Criterion) {
                benchmark_sqlite_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }
            
            fn [<benchmark_q $query_num _duckdb>](c: &mut Criterion) {
                benchmark_duckdb_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }
        }
    };
}

// =============================================================================
// Generate All Benchmark Functions
// =============================================================================

// Q1 uses grouped benchmarks
tpch_benchmark_grouped!(1, TPCH_Q1);

// Q2-Q22 use simple benchmarks
tpch_benchmark!(2, TPCH_Q2);
tpch_benchmark!(3, TPCH_Q3);
tpch_benchmark!(4, TPCH_Q4);
tpch_benchmark!(5, TPCH_Q5);
tpch_benchmark!(6, TPCH_Q6);
tpch_benchmark!(7, TPCH_Q7);
tpch_benchmark!(8, TPCH_Q8);
tpch_benchmark!(9, TPCH_Q9);
tpch_benchmark!(10, TPCH_Q10);
tpch_benchmark!(11, TPCH_Q11);
tpch_benchmark!(12, TPCH_Q12);
tpch_benchmark!(13, TPCH_Q13);
tpch_benchmark!(14, TPCH_Q14);
tpch_benchmark!(15, TPCH_Q15);
tpch_benchmark!(16, TPCH_Q16);
tpch_benchmark!(17, TPCH_Q17);
tpch_benchmark!(18, TPCH_Q18);
tpch_benchmark!(19, TPCH_Q19);
tpch_benchmark!(20, TPCH_Q20);
tpch_benchmark!(21, TPCH_Q21);
tpch_benchmark!(22, TPCH_Q22);

// =============================================================================
// Criterion Benchmark Group
// =============================================================================

criterion_group!(
    benches,
    benchmark_q1_vibesql,
    benchmark_q1_sqlite,
    benchmark_q1_duckdb,
    benchmark_q2_vibesql,
    benchmark_q2_sqlite,
    benchmark_q2_duckdb,
    benchmark_q3_vibesql,
    benchmark_q3_sqlite,
    benchmark_q3_duckdb,
    benchmark_q4_vibesql,
    benchmark_q4_sqlite,
    benchmark_q4_duckdb,
    benchmark_q5_vibesql,
    benchmark_q5_sqlite,
    benchmark_q5_duckdb,
    benchmark_q6_vibesql,
    benchmark_q6_sqlite,
    benchmark_q6_duckdb,
    benchmark_q7_vibesql,
    benchmark_q7_sqlite,
    benchmark_q7_duckdb,
    benchmark_q8_vibesql,
    benchmark_q8_sqlite,
    benchmark_q8_duckdb,
    benchmark_q9_vibesql,
    benchmark_q9_sqlite,
    benchmark_q9_duckdb,
    benchmark_q10_vibesql,
    benchmark_q10_sqlite,
    benchmark_q10_duckdb,
    benchmark_q11_vibesql,
    benchmark_q11_sqlite,
    benchmark_q11_duckdb,
    benchmark_q12_vibesql,
    benchmark_q12_sqlite,
    benchmark_q12_duckdb,
    benchmark_q13_vibesql,
    benchmark_q13_sqlite,
    benchmark_q13_duckdb,
    benchmark_q14_vibesql,
    benchmark_q14_sqlite,
    benchmark_q14_duckdb,
    benchmark_q15_vibesql,
    benchmark_q15_sqlite,
    benchmark_q15_duckdb,
    benchmark_q16_vibesql,
    benchmark_q16_sqlite,
    benchmark_q16_duckdb,
    benchmark_q17_vibesql,
    benchmark_q17_sqlite,
    benchmark_q17_duckdb,
    benchmark_q18_vibesql,
    benchmark_q18_sqlite,
    benchmark_q18_duckdb,
    benchmark_q19_vibesql,
    benchmark_q19_sqlite,
    benchmark_q19_duckdb,
    benchmark_q20_vibesql,
    benchmark_q20_sqlite,
    benchmark_q20_duckdb,
    benchmark_q21_vibesql,
    benchmark_q21_sqlite,
    benchmark_q21_duckdb,
    benchmark_q22_vibesql,
    benchmark_q22_sqlite,
    benchmark_q22_duckdb
);

criterion_main!(benches);
