# Migration Plan: Scripts → vibesql-sqllogictest Crate

## Overview
Currently we have ~3,400 lines of Python code in `scripts/` for SQLLogicTest orchestration. This document outlines what functionality should be moved into the `vibesql-sqllogictest` Rust crate.

## Current Script Structure

### Core Scripts
1. **`sqllogictest`** (bash, 613 lines) - Unified CLI wrapper
2. **`run_parallel_tests.py`** (1,273 lines) - Parallel test execution
3. **`process_test_results.py`** (445 lines) - Store results in VibeSQL database
4. **`query_test_results.py`** (424 lines) - Query results database
5. **`detect_test_regressions.py`** (312 lines) - Find regressions
6. **Other utilities** (~1,000 lines) - Analysis, reporting, backup

### Total: ~3,400 lines Python + 613 lines Bash

## Recommended Migration Strategy

### Phase 1: Core Functionality (High Priority)
Move into `crates/vibesql-sqllogictest/src/`:

#### 1. Test Result Storage (`src/results.rs`)
**From**: `process_test_results.py`
**Lines**: ~445
**Rationale**:
- Results are JSON - native Rust serde handling
- VibeSQL database integration shows dogfooding
- Remove Python dependency

**API**:
```rust
pub struct ResultStorage {
    db: Database,
}

impl ResultStorage {
    pub fn store_run(&mut self, results: Vec<TestResult>) -> Result<RunId>;
    pub fn query(&self, filter: ResultFilter) -> Result<Vec<TestResult>>;
}
```

#### 2. Parallel Test Runner (`src/parallel.rs`)
**From**: `run_parallel_tests.py`
**Lines**: ~1,273
**Rationale**:
- The library already has `executor/parallel.rs` scaffolding
- Native async/tokio better than Python multiprocessing
- Better error handling and resource management

**Status**: Partially implemented in `src/executor/parallel.rs`
**Needed**:
- File-based work queue with locking
- Per-file timeout handling
- Worker result merging

#### 3. Regression Detection (`src/regression.rs`)
**From**: `detect_test_regressions.py`
**Lines**: ~312
**Rationale**:
- Simple logic comparing two result sets
- Better integrated with result storage

### Phase 2: CLI Interface (Medium Priority)

#### 4. Unified CLI (`src/bin/vibesql-sqllogictest.rs`)
**From**: `scripts/sqllogictest` (bash)
**Lines**: ~613
**Rationale**:
- Replace bash with `clap` for robust CLI
- Single binary distribution
- Better error messages and help

**Commands**:
- `vibesql-sqllogictest test <file>` - Single file
- `vibesql-sqllogictest run [--parallel]` - Full suite
- `vibesql-sqllogictest query <sql>` - Query results
- `vibesql-sqllogictest analyze` - Show patterns
- `vibesql-sqllogictest status` - Quick summary

### Phase 3: Analysis & Reporting (Lower Priority)

#### 5. Keep as Python (for now)
- `query_test_results.py` - Complex SQL queries, prototyping
- `test_generate_punchlist.py` - Markdown generation
- `backup_test_results.sh` - Simple file operations

**Rationale**:
- These are less performance-critical
- Python better for rapid iteration on queries/reports
- Can migrate later if needed

## Directory Structure (Proposed)

```
crates/vibesql-sqllogictest/
├── src/
│   ├── lib.rs                 # Re-exports from submodules
│   ├── executor/
│   │   ├── core.rs           # Existing
│   │   ├── parallel.rs       # ENHANCE: full parallel runner
│   │   └── record_processor.rs
│   ├── parser/
│   │   └── ...               # Existing
│   ├── results.rs            # NEW: result storage
│   ├── regression.rs         # NEW: regression detection
│   └── bin/
│       └── vibesql-sqllogictest.rs  # NEW: CLI
├── MIGRATION_PLAN.md         # This file
└── Cargo.toml
```

## Implementation Order

1. ✅ **Rename crate** (DONE)
   - Directory: `crates/vibesql-sqllogictest/`
   - Package name: `sqllogictest` (for patch compatibility)

2. **Results storage** (`results.rs`)
   - 2-3 days
   - Replaces `process_test_results.py`
   - Uses vibesql-executor for database operations

3. **Enhanced parallel runner** (`executor/parallel.rs`)
   - 3-5 days
   - Replaces `run_parallel_tests.py`
   - File queue, timeouts, worker coordination

4. **CLI binary** (`bin/vibesql-sqllogictest.rs`)
   - 2-3 days
   - Replaces `scripts/sqllogictest`
   - Unified interface to all functionality

5. **Regression detection** (`regression.rs`)
   - 1-2 days
   - Replaces `detect_test_regressions.py`

## Benefits

1. **Performance**: Rust faster than Python for parallel coordination
2. **Reliability**: Strong typing catches bugs at compile time
3. **Distribution**: Single binary, no Python dependency
4. **Integration**: Native access to vibesql-executor
5. **Clarity**: Clear ownership - this is our code

## Risks & Mitigation

**Risk**: Breaking existing workflows
**Mitigation**: Keep Python scripts during transition, add deprecation warnings

**Risk**: Longer implementation time
**Mitigation**: Incremental migration, keep what works

**Risk**: Feature parity with Python
**Mitigation**: Focus on core features first, keep advanced analysis in Python

## Next Steps

1. **Immediate**: Document current script usage patterns
2. **This week**: Implement `results.rs` module
3. **Next week**: Enhance `parallel.rs` with full worker coordination
4. **Following week**: Create CLI binary

## Questions for Discussion

1. Should we keep Python for complex SQL queries and analysis?
2. Do we need full feature parity or can we simplify?
3. Timeline for deprecating Python scripts?
4. Should CLI be a separate binary or integrated into vibesql-cli?
