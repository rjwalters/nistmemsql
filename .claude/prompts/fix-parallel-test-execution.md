# Agent Task: Fix SQLLogicTest Parallel Testing - Work Queue Implementation

## Status: IN PROGRESS

This task has been partially completed. The work queue system is implemented but needs final testing and verification.

## What Has Been Done

1. ✅ **Fixed time budget looping** (Commit 7840c2f)
   - Workers now loop through files until time budget expires
   - Verified 94%+ CPU utilization with 64 workers

2. ✅ **Implemented work queue system** (Commit 8a80ed6)
   - Created `tests/sqllogictest/work_queue.rs`
   - Modified test suite to claim files dynamically
   - Updated Python orchestrator to initialize queue
   - Each file tested exactly once (no redundant work)

## What Needs To Be Done

### Immediate Tasks

1. **Test work queue locally**
   - Run: `python3 scripts/run_parallel_tests.py --workers 2 --time-budget 30`
   - Verify both workers claim files from queue
   - Check that all 623 files get tested exactly once
   - Verify queue directories: pending → claimed → completed

2. **Test on remote with 8 workers**
   - Run: `./scripts/remote_test.sh --sync --workers 8 --time 300`
   - Monitor: `ssh rwalters-sandbox-1 'ls /tmp/sqllogictest_work_queue/*/ | wc -l'`
   - Verify all files tested and workers exit cleanly

3. **Full scale test with 64 workers**
   - Run: `./scripts/remote_quick.sh sync && ./scripts/remote_quick.sh run`
   - Should complete all 623 files in under 10 minutes
   - All workers should exit when queue is empty

### Known Issues Being Debugged

- Path resolution: Work queue returns relative paths, need to prepend test_dir
- Workers are claiming files but may fail to read them (check logs)
- Need to verify `__` path separator works correctly on all platforms

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
