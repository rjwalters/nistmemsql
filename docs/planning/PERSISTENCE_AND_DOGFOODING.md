# Persistence and Dogfooding Workplan

## Overview

Add SQL dump persistence to VibeSQL and use the database to store its own SQLLogicTest conformance results. This demonstrates real-world usage and provides a live, queryable dataset on the demo website.

## Goals

1. **SQL Dump Persistence** - Export/import database state as human-readable SQL
2. **Self-Hosting Test Results** - Store SQLLogicTest results in VibeSQL itself
3. **Live Query Interface** - Make test results explorable on the demo website
4. **Dogfooding** - Use VibeSQL for a real, production use case

## Architecture

### 1. SQL Dump Format (Rust)

**Location**: `crates/storage/src/persistence.rs`

**Functionality**:
- `Database::save_sql_dump(path)` - Export database as SQL statements
- Format: Standard SQL CREATE TABLE + INSERT statements
- Includes: schemas, tables, data, indexes, roles
- Human-readable, version-controllable, portable

**Test Coverage**:
- Unit tests for SQL generation
- Integration tests for round-trip (export → import)
- Edge cases: NULL values, special characters, quotes

### 2. Test Results Schema

**Tables**:
```sql
-- Main test file tracking
CREATE TABLE test_files (
    file_path VARCHAR(500) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    status VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'UNTESTED'
    last_tested TIMESTAMP,
    last_passed TIMESTAMP
);

-- Test run metadata
CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit VARCHAR(40),
    ci_run_id VARCHAR(100)
);

-- Individual test results
CREATE TABLE test_results (
    result_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    status VARCHAR(20) NOT NULL,
    tested_at TIMESTAMP NOT NULL,
    duration_ms INTEGER,
    error_message VARCHAR(2000),
    FOREIGN KEY (run_id) REFERENCES test_runs(run_id),
    FOREIGN KEY (file_path) REFERENCES test_files(file_path)
);
```

**Useful Queries**:
```sql
-- Current status summary
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested
FROM test_files
GROUP BY category
ORDER BY category;

-- Progress over time
SELECT
    DATE(completed_at) as date,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT 30;

-- Most problematic files
SELECT
    file_path,
    COUNT(*) as failure_count
FROM test_results
WHERE status = 'FAIL'
GROUP BY file_path
HAVING COUNT(*) > 5
ORDER BY failure_count DESC
LIMIT 20;

-- Recent failures
SELECT
    tf.file_path,
    tf.category,
    tr.error_message,
    tr.tested_at
FROM test_results tr
JOIN test_files tf ON tr.file_path = tf.file_path
WHERE tr.status = 'FAIL'
ORDER BY tr.tested_at DESC
LIMIT 50;
```

### 3. Python Integration

**File**: `scripts/generate_punchlist.py`

**Changes**:
1. Import VibeSQL Python bindings
2. Create/load test results database
3. Insert test file statuses from cumulative results
4. Run summary queries to generate punchlist
5. Export SQL dump to `target/sqllogictest_results.sql`

**Example Code**:
```python
import vibesql

# Load or create database
try:
    db = vibesql.connect()
    # Load previous state if exists
    with open('target/test_results.sql', 'r') as f:
        for statement in parse_sql_dump(f):
            db.execute(statement)
except FileNotFoundError:
    # First run, create schema
    db.execute(open('scripts/schema/test_results.sql').read())

# Insert current test results
cursor = db.cursor()
for file_path, status in test_results.items():
    cursor.execute("""
        INSERT OR REPLACE INTO test_files (file_path, category, status, last_tested)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (file_path, category, status))

# Export for web demo
with open('target/sqllogictest_results.sql', 'w') as f:
    f.write(db.dump_sql())
```

### 4. Web Demo Integration

**Location**: `web-demo/src/App.tsx`

**Changes**:
1. Add "SQLLogicTest Results" sample database
2. Pre-load SQL dump on page load
3. Add example queries for exploring test data
4. Show summary statistics in UI

**Example Queries for Demo**:
```sql
-- Status by category
SELECT category, COUNT(*) as total,
       SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed
FROM test_files GROUP BY category;

-- Recent progress
SELECT DATE(completed_at) as date, passed, failed
FROM test_runs ORDER BY date DESC LIMIT 10;

-- Find all index tests that are failing
SELECT file_path FROM test_files
WHERE category='index' AND status='FAIL'
ORDER BY file_path LIMIT 20;
```

## Implementation Plan (TDD)

### Phase 1: SQL Dump Persistence ✅ (Partially Complete)

**Tests** (in `crates/storage/src/persistence.rs`):
- [x] `test_save_sql_dump` - Basic export functionality
- [ ] `test_sql_dump_round_trip` - Export then re-import
- [ ] `test_sql_dump_with_nulls` - NULL value handling
- [ ] `test_sql_dump_with_quotes` - String escaping
- [ ] `test_sql_dump_empty_database` - Edge case
- [ ] `test_sql_dump_with_indexes` - Index export
- [ ] `test_sql_dump_with_roles` - Role/privilege export

**Implementation**:
1. Write failing tests
2. Implement `save_sql_dump()` method
3. Fix edge cases until all tests pass
4. Add to public API

### Phase 2: Test Results Schema

**Tests** (in `tests/test_results_schema.rs`):
- [ ] `test_create_test_files_table`
- [ ] `test_create_test_runs_table`
- [ ] `test_create_test_results_table`
- [ ] `test_foreign_key_constraints`
- [ ] `test_insert_test_file`
- [ ] `test_insert_test_run`
- [ ] `test_query_summary_by_category`
- [ ] `test_query_progress_over_time`

**Implementation**:
1. Create `scripts/schema/test_results.sql`
2. Write tests that create tables and query data
3. Verify schema works with real data
4. Document schema in this file

### Phase 3: Python Integration

**Tests** (in `scripts/test_generate_punchlist.py`):
- [ ] `test_create_database_from_schema`
- [ ] `test_insert_test_results`
- [ ] `test_export_sql_dump`
- [ ] `test_load_existing_dump`
- [ ] `test_summary_queries_match_old_format`

**Implementation**:
1. Write Python tests using vibesql bindings
2. Update `generate_punchlist.py` incrementally
3. Keep backward compatibility with JSON output
4. Validate SQL dump is valid

### Phase 4: Web Demo Integration

**Tests** (in `web-demo/src/App.test.tsx`):
- [ ] `test_load_sqllogictest_database`
- [ ] `test_example_queries_execute`
- [ ] `test_summary_statistics_display`

**Implementation**:
1. Add SQL dump to web demo assets
2. Load on "SQLLogicTest Results" tab
3. Add example queries
4. Test in browser

### Phase 5: CI Integration

**Tests** (in `.github/workflows/ci-and-deploy.yml`):
- [ ] Verify SQL dump is generated in CI
- [ ] Upload SQL dump as artifact
- [ ] Deploy to GitHub Pages

**Implementation**:
1. Run `generate_punchlist.py` in CI
2. Copy SQL dump to `web-demo/public/data/`
3. Deploy with web demo

## Success Criteria

- [ ] `cargo test --package storage` - All persistence tests pass
- [ ] `python3 scripts/test_generate_punchlist.py` - All Python tests pass
- [ ] `npm test` (in web-demo) - All web tests pass
- [ ] Demo website shows live SQLLogicTest results
- [ ] Users can query test data with custom SQL
- [ ] Badge generation sources from VibeSQL queries
- [ ] Documentation complete

## Benefits

1. **Dogfooding** - We use our own database for real work
2. **Transparency** - Test results are queryable by users
3. **Debugging** - Easy to explore failure patterns
4. **Progress Tracking** - Historical data over time
5. **Demo Value** - Real dataset, not toy examples
6. **SQL Showcase** - Complex queries demonstrate capabilities

## Timeline

- **Phase 1**: 1-2 hours (SQL dump persistence)
- **Phase 2**: 1 hour (Schema design and tests)
- **Phase 3**: 2-3 hours (Python integration)
- **Phase 4**: 1-2 hours (Web demo)
- **Phase 5**: 1 hour (CI integration)

**Total**: 6-9 hours of development time

## Open Questions

1. Should we store historical test runs or only current status?
   - **Decision**: Store both for progress tracking

2. How to handle schema migrations as we add features?
   - **Decision**: For now, recreate from scratch each time (acceptable for test data)

3. Should Python bindings support executing SQL dump files directly?
   - **Decision**: Add `db.execute_script(sql_dump)` helper method

4. What about binary persistence for performance?
   - **Decision**: Deferred - SQL dump is sufficient for now, can add later with serde

## Future Enhancements

- Binary persistence with serde (faster load/save)
- Incremental updates (don't recreate entire database)
- Schema migrations support
- Performance metrics in test_results
- Failure pattern analysis queries
- Automated issue creation from failure patterns
