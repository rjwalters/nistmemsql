//! Detailed profiling benchmark for TPC-H Q3
//!
//! Run with:
//!   cargo run --release --features "benchmark-comparison" --bench q3_profiling

mod tpch;

use std::time::Instant;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

const Q3_SQL: &str = r#"
    SELECT l_orderkey,
           SUM(l_extendedprice * (1 - l_discount)) as revenue,
           o_orderdate, o_shippriority
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

fn main() {
    eprintln!("=== TPC-H Q3 Profiling ===\n");

    // Load database
    eprintln!("Loading TPC-H database (SF 0.01)...");
    let load_start = Instant::now();
    let db = load_vibesql(0.01);
    eprintln!("Database loaded in {:?}\n", load_start.elapsed());

    // Parse query
    let stmt = Parser::parse_sql(Q3_SQL).unwrap();
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    // Warm-up run
    eprintln!("Warm-up run...");
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt);
    match &result {
        Ok(r) => eprintln!("Warm-up complete ({} rows)\n", r.len()),
        Err(e) => eprintln!("Warm-up error: {:?}\n", e),
    }

    // Profile runs
    const NUM_RUNS: usize = 5;
    eprintln!("Running {} iterations...\n", NUM_RUNS);

    let mut total_times = Vec::with_capacity(NUM_RUNS);

    for i in 0..NUM_RUNS {
        eprintln!("--- Run {} ---", i + 1);

        let total_start = Instant::now();
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();
        let total_elapsed = total_start.elapsed();

        eprintln!("[Q3 PROFILE] Total query time: {:?}", total_elapsed);
        eprintln!("[Q3 PROFILE] Result rows: {}\n", result.len());
        total_times.push(total_elapsed);
    }

    // Calculate statistics
    total_times.sort();
    let median_total = total_times[NUM_RUNS / 2];

    eprintln!("\n=== Summary ===");
    eprintln!("Median total: {:?}", median_total);
}
