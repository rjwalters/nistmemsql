# Agent Task: Fix SQLLogicTest Parallel Execution Time Budget Issue

## Your Mission

Fix the remote SQLLogicTest parallel execution so that workers utilize their full time budget instead of exiting early after completing small file partitions.

## The Problem

**Current Behavior**:
- 64 Python workers spawn correctly ✓
- Each worker gets ~10 test files from their partition
- Workers complete their 10 files in ~30 seconds
- Workers exit instead of continuing for the full 3600 second (1 hour) time budget
- Result: Only 2-4 active test processes at any time, ~3% CPU utilization

**Expected Behavior**:
- 64 workers should run for the FULL time budget (3600 seconds)
- Workers should loop/cycle through their files until time expires
- CPU utilization should be >70% across all 64 cores
- Each worker should test hundreds of files over the hour

## Root Cause

The test suite in `tests/sqllogictest_suite.rs` iterates through the prioritized file list ONCE and then exits:

```rust
for (files_tested, test_file) in prioritized_files.into_iter().enumerate() {
    if start_time.elapsed() >= time_budget {
        break;  // Only checked between files
    }
    // Test the file...
}
// Loop exits - worker terminates even if time remains!
```

When a worker's partition only has 10 files that test in seconds, it exits in ~30 seconds despite having a 3600 second time budget.

## Your Task

### Primary Objective

Modify `tests/sqllogictest_suite.rs` to make workers continue testing until their time budget is exhausted.

### Recommended Approach

1. **Wrap the file iteration in an outer loop** that continues until time budget expires:

```rust
let mut iteration = 0;
loop {
    for (files_tested, test_file) in prioritized_files.iter().enumerate() {
        // Check time budget frequently
        if start_time.elapsed() >= time_budget {
            println!("\n⏱️  Time budget exhausted");
            return (results, files_tested);
        }
        // Test the file...
    }

    iteration += 1;
    println!("Completed iteration {}, cycling through files again...", iteration);

    if start_time.elapsed() >= time_budget {
        break;
    }
}
```

2. **Test incrementally**:
   - First test locally with short time budget: `SQLLOGICTEST_TIME_BUDGET=60 cargo test --test sqllogictest_suite -- --nocapture`
   - Verify it runs for 60 seconds, cycling through files multiple times
   - Then test on remote with 1 worker: `./scripts/remote_test.sh --sync --workers 1 --time 300`
   - Then with 8 workers: `./scripts/remote_test.sh --sync --workers 8 --time 300`
   - Finally full scale: `./scripts/remote_quick.sh sync && ./scripts/remote_quick.sh quick`

3. **Monitor success**:
   ```bash
   # SSH to remote and watch processes
   ssh rwalters-sandbox-1 'watch -n 5 "ps aux | grep sqllogictest_suite | grep -v grep | wc -l"'

   # Check CPU utilization
   ssh rwalters-sandbox-1 'top -bn1 | head -20'
   ```

### Success Criteria

✅ Workers run for their full time budget (verified by timestamps in logs)
✅ CPU utilization >70% when running parallel tests
✅ `ps aux | grep sqllogictest | wc -l` shows 8+ active processes consistently
✅ Worker log files show multiple iterations through file lists
✅ Each worker tests hundreds of files instead of just ~10

## Context Files

**Read these first**:
- `docs/ISSUE_REMOTE_TEST_PARALLELISM.md` - Detailed investigation and analysis
- `tests/sqllogictest_suite.rs` - Main test runner (needs modification)
- `tests/sqllogictest/scheduler.rs` - File partitioning logic
- `scripts/run_parallel_tests.py` - Python orchestration
- `docs/REMOTE_TESTING.md` - Usage guide for remote testing

## Testing Commands

```bash
# Local test - should run for 60 seconds
SQLLOGICTEST_TIME_BUDGET=60 cargo test --test sqllogictest_suite -- --nocapture

# Remote quick test - 8 workers, 5 minutes
./scripts/remote_quick.sh sync
./scripts/remote_quick.sh quick

# Monitor on remote
ssh rwalters-sandbox-1 'top -bn1 | head -20'
ssh rwalters-sandbox-1 'ps aux | grep sqllogictest_suite | grep -v grep'

# Fetch results
./scripts/remote_quick.sh fetch
./scripts/remote_quick.sh status
```

## Additional Considerations

1. **Preserve test statistics**: Make sure iteration counts and pass/fail stats aggregate correctly across multiple passes

2. **Handle flaky tests**: Multiple iterations will help catch non-deterministic failures

3. **Logging**: Add clear logging for iteration counts so we can verify workers are cycling

4. **Edge cases**:
   - What if worker partition is empty?
   - What if all files pass/fail quickly?
   - Ensure time checks are frequent enough

## Deliverables

1. Modified `tests/sqllogictest_suite.rs` with outer loop implementation
2. Local test verification showing time budget is respected
3. Remote test verification showing parallel execution works
4. Updated documentation if behavior changes
5. Commit with clear message explaining the fix

## Notes

- The remote machine `rwalters-sandbox-1` is already set up and accessible via SSH
- Tests are currently running but exiting early - this is expected
- The infrastructure (scripts, Python orchestration) is working correctly
- This is purely a Rust test suite logic fix
- Low risk change - just adding a loop wrapper

## Questions to Answer

Before you start coding:
1. How will you handle test result aggregation across multiple iterations?
2. Should failed files be retested in subsequent iterations (probably yes for flaky test detection)?
3. How frequently should time budget be checked (every file? every N files)?
4. What iteration count seems reasonable (10? 100? unlimited until time expires)?

Read the detailed investigation in `docs/ISSUE_REMOTE_TEST_PARALLELISM.md` for full context.

Good luck! This is a straightforward but impactful fix that will unlock the full power of our 64-core remote testing infrastructure.
