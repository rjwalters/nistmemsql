//! Detailed profiling benchmark for TPC-H Q6
//!
//! This benchmark uses the profile-q6 feature to add timing instrumentation
//! to understand where time is spent:
//! 1. Loading rows from storage (disk I/O + deserialization)
//! 2. Query processing (predicate evaluation + aggregation)
//!
//! Run with:
//!   cargo run --release --features "benchmark-comparison,profile-q6" --bench q6_profiling

mod tpch;

use std::time::Instant;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

const Q6_SQL: &str = r#"
    SELECT SUM(l_extendedprice * l_discount) as revenue
    FROM lineitem
    WHERE
        l_shipdate >= '1994-01-01'
        AND l_shipdate < '1995-01-01'
        AND l_discount BETWEEN 0.05 AND 0.07
        AND l_quantity < 24
"#;

fn main() {
    eprintln!("=== TPC-H Q6 Profiling ===\n");

    // Load database
    eprintln!("Loading TPC-H database (SF 0.01)...");
    let load_start = Instant::now();
    let db = load_vibesql(0.01);
    eprintln!("Database loaded in {:?}\n", load_start.elapsed());

    // Parse query
    let stmt = Parser::parse_sql(Q6_SQL).unwrap();
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    // Warm-up run
    eprintln!("Warm-up run...");
    let executor = SelectExecutor::new(&db);
    let _ = executor.execute(&select_stmt).unwrap();
    eprintln!("Warm-up complete\n");

    // Profile runs
    const NUM_RUNS: usize = 10;
    eprintln!("Running {} iterations...\n", NUM_RUNS);

    let mut total_times = Vec::with_capacity(NUM_RUNS);

    for i in 0..NUM_RUNS {
        eprintln!("--- Run {} ---", i + 1);

        let total_start = Instant::now();

        let executor_create_start = Instant::now();
        let executor = SelectExecutor::new(&db);
        let executor_create_time = executor_create_start.elapsed();

        let execute_start = Instant::now();
        let result = executor.execute(&select_stmt).unwrap();
        let execute_time = execute_start.elapsed();

        let total_elapsed = total_start.elapsed();

        eprintln!("[Q6 PROFILE] Executor creation: {:?}", executor_create_time);
        eprintln!("[Q6 PROFILE] Execute call: {:?}", execute_time);
        eprintln!("[Q6 PROFILE] Total query time: {:?}\n", total_elapsed);
        total_times.push(total_elapsed);

        if i == 0 {
            eprintln!("Result: {:?}\n", result[0]);
        }
    }

    // Calculate statistics
    total_times.sort();
    let median_total = total_times[NUM_RUNS / 2];

    eprintln!("\n=== Summary ===");
    eprintln!("Median total: {:?}", median_total);
    eprintln!("\nWith profile-q6 feature enabled, see detailed breakdown above.");
}
