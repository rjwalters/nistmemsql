-- Benchmark results database schema
-- Stores performance data from SQLLogicTest suite benchmarks

-- Benchmark runs (metadata about each benchmark execution)
CREATE TABLE benchmark_runs (
    run_id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    total_files INTEGER NOT NULL,
    notes TEXT
);

-- Individual test file results
CREATE TABLE benchmark_results (
    result_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    category TEXT NOT NULL,

    -- VibeSQL results
    vibesql_success INTEGER NOT NULL,  -- 1 for success, 0 for failure
    vibesql_run1_secs REAL,
    vibesql_run2_secs REAL,
    vibesql_run3_secs REAL,
    vibesql_min_secs REAL,
    vibesql_max_secs REAL,
    vibesql_avg_secs REAL,

    -- SQLite3 results (optional, for head-to-head benchmarks)
    sqlite_success INTEGER,
    sqlite_run1_secs REAL,
    sqlite_run2_secs REAL,
    sqlite_run3_secs REAL,
    sqlite_min_secs REAL,
    sqlite_max_secs REAL,
    sqlite_avg_secs REAL,

    -- Computed comparison
    speed_ratio REAL,  -- vibesql_avg / sqlite_avg (< 1.0 means VibeSQL faster)

    FOREIGN KEY (run_id) REFERENCES benchmark_runs(run_id)
);

-- Indexes for common queries
CREATE INDEX idx_results_run_id ON benchmark_results(run_id);
CREATE INDEX idx_results_category ON benchmark_results(category);
CREATE INDEX idx_results_vibesql_success ON benchmark_results(vibesql_success);

-- Useful views for analysis

-- Summary by category
CREATE VIEW benchmark_summary_by_category AS
SELECT
    r.run_id,
    br.timestamp,
    res.category,
    COUNT(*) as total_tests,
    SUM(CASE WHEN res.vibesql_success = 1 THEN 1 ELSE 0 END) as vibesql_passed,
    SUM(CASE WHEN res.sqlite_success = 1 THEN 1 ELSE 0 END) as sqlite_passed,
    ROUND(AVG(res.vibesql_avg_secs), 6) as avg_vibesql_time,
    ROUND(AVG(res.sqlite_avg_secs), 6) as avg_sqlite_time,
    ROUND(AVG(res.speed_ratio), 3) as avg_speed_ratio,
    SUM(CASE WHEN res.speed_ratio < 1.0 THEN 1 ELSE 0 END) as vibesql_faster_count,
    SUM(CASE WHEN res.speed_ratio > 1.0 THEN 1 ELSE 0 END) as sqlite_faster_count
FROM benchmark_results res
JOIN benchmark_runs r ON res.run_id = r.run_id
JOIN benchmark_runs br ON r.run_id = br.run_id
GROUP BY r.run_id, res.category
ORDER BY r.run_id DESC, res.category;

-- Top 20 fastest VibeSQL vs SQLite comparisons
CREATE VIEW top_vibesql_wins AS
SELECT
    file_path,
    category,
    vibesql_avg_secs,
    sqlite_avg_secs,
    speed_ratio,
    ROUND((1.0 - speed_ratio) * 100, 1) as percent_faster
FROM benchmark_results
WHERE speed_ratio IS NOT NULL
  AND speed_ratio < 1.0
ORDER BY speed_ratio ASC
LIMIT 20;

-- Top 20 tests where SQLite is faster
CREATE VIEW top_sqlite_wins AS
SELECT
    file_path,
    category,
    vibesql_avg_secs,
    sqlite_avg_secs,
    speed_ratio,
    ROUND((speed_ratio - 1.0) * 100, 1) as percent_slower
FROM benchmark_results
WHERE speed_ratio IS NOT NULL
  AND speed_ratio > 1.0
ORDER BY speed_ratio DESC
LIMIT 20;

-- Overall statistics
CREATE VIEW benchmark_overall_stats AS
SELECT
    r.run_id,
    r.timestamp,
    r.total_files,
    COUNT(*) as results_count,
    SUM(CASE WHEN vibesql_success = 1 THEN 1 ELSE 0 END) as vibesql_passed,
    SUM(CASE WHEN sqlite_success = 1 THEN 1 ELSE 0 END) as sqlite_passed,
    ROUND(SUM(vibesql_avg_secs), 3) as total_vibesql_time,
    ROUND(SUM(sqlite_avg_secs), 3) as total_sqlite_time,
    ROUND(AVG(speed_ratio), 3) as avg_speed_ratio,
    ROUND(MIN(speed_ratio), 3) as best_ratio,
    ROUND(MAX(speed_ratio), 3) as worst_ratio
FROM benchmark_results res
JOIN benchmark_runs r ON res.run_id = r.run_id
GROUP BY r.run_id
ORDER BY r.run_id DESC;
