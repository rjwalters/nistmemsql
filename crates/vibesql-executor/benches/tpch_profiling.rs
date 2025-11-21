//! Comprehensive profiling for all TPC-H queries
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpch_profiling --features benchmark-comparison --no-run && ./target/release/deps/tpch_profiling-*
//!
//! Set QUERY_TIMEOUT_SECS env var to limit per-query time (default: 30s)

mod tpch;

use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

fn run_query_detailed(db: &vibesql_storage::Database, name: &str, sql: &str, timeout: Duration) {
    eprintln!("\n=== {} ===", name);
    eprintln!("SQL: {}", sql.trim().lines().take(3).collect::<Vec<_>>().join(" ").chars().take(80).collect::<String>());

    // Parse
    let parse_start = Instant::now();
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => { eprintln!("ERROR: Not a SELECT"); return; }
        Err(e) => { eprintln!("ERROR: Parse error: {}", e); return; }
    };
    let parse_time = parse_start.elapsed();
    eprintln!("  Parse:    {:>10.2?}", parse_time);

    // Create executor with timeout
    let exec_create_start = Instant::now();
    let executor = SelectExecutor::new(db).with_timeout(timeout.as_secs());
    let exec_create_time = exec_create_start.elapsed();
    eprintln!("  Executor: {:>10.2?} (timeout: {:?})", exec_create_time, timeout);

    // Execute
    let execute_start = Instant::now();
    let result = executor.execute(&stmt);
    let execute_time = execute_start.elapsed();

    match result {
        Ok(rows) => {
            eprintln!("  Execute:  {:>10.2?} ({} rows)", execute_time, rows.len());
            let total = parse_time + exec_create_time + execute_time;
            eprintln!("  TOTAL:    {:>10.2?}", total);
        }
        Err(e) => {
            eprintln!("  Execute:  {:>10.2?} ERROR: {}", execute_time, e);
        }
    }
}

fn main() {
    eprintln!("=== TPC-H Query Profiling ===");

    // Get timeout from env (default 30s)
    let timeout_secs: u64 = std::env::var("QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let timeout = Duration::from_secs(timeout_secs);
    eprintln!("Per-query timeout: {}s (set QUERY_TIMEOUT_SECS to change)", timeout_secs);

    // Load database
    eprintln!("\nLoading TPC-H database (SF 0.01)...");
    let load_start = Instant::now();
    let db = load_vibesql(0.01);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Run all 22 TPC-H queries to get baseline timings
    let queries: Vec<(&str, &str)> = vec![
        ("Q1", TPCH_Q1),
        ("Q2", TPCH_Q2),
        ("Q3", TPCH_Q3),
        ("Q4", TPCH_Q4),
        ("Q5", TPCH_Q5),
        ("Q6", TPCH_Q6),
        ("Q7", TPCH_Q7),
        ("Q8", TPCH_Q8),
        ("Q9", TPCH_Q9),
        ("Q10", TPCH_Q10),
        ("Q11", TPCH_Q11),
        ("Q12", TPCH_Q12),
        ("Q13", TPCH_Q13),
        ("Q14", TPCH_Q14),
        ("Q15", TPCH_Q15),
        ("Q16", TPCH_Q16),
        ("Q17", TPCH_Q17),
        ("Q18", TPCH_Q18),
        ("Q19", TPCH_Q19),
        ("Q20", TPCH_Q20),
        ("Q21", TPCH_Q21),
        ("Q22", TPCH_Q22),
    ];

    for (name, sql) in &queries {
        run_query_detailed(&db, name, sql, timeout);
    }

    eprintln!("\n=== Done - All 22 TPC-H Queries ===");
}
