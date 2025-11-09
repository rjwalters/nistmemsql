# Investigation: Remote SQLLogicTest Parallelism Issues

## Problem Statement

When running the SQLLogicTest suite on a remote machine with 64 cores using the parallel test runner (`scripts/run_parallel_tests.py`), we observed that only 2-4 test processes are actively running at any given time, despite spawning 64 Python worker processes. This results in poor CPU utilization (~3% across 64 cores) instead of the expected near-100% utilization.

## Background Context

### Remote Testing Setup

We created remote testing infrastructure for running the SQLLogicTest suite on powerful remote machines:

- **Scripts Created**:
  - `scripts/remote_test.sh` - Full-featured remote execution with options
  - `scripts/remote_quick.sh` - Simplified wrapper for common operations
  - `docs/REMOTE_TESTING.md` - Comprehensive usage documentation

- **Remote Machine**: `rwalters-sandbox-1`
  - 64 CPU cores (verified with `nproc`)
  - Ubuntu Linux on AWS
  - Rust toolchain installed
  - Repository at `~/vibesql`

### Test Execution Command

```bash
./scripts/remote_quick.sh run
# Internally runs:
# ./scripts/sqllogictest run --parallel --workers 64 --time 3600
# Which calls:
# python3 scripts/run_parallel_tests.py --workers 64 --time-budget 3600
```

## Observed Behavior

### What We Expected

- 64 Python worker processes spawn
- Each worker runs `cargo test --test sqllogictest_suite --release` with unique `SQLLOGICTEST_WORKER_ID`
- All 64 workers run tests concurrently for 1 hour (3600 seconds)
- High CPU utilization across all 64 cores
- Each worker continuously tests files until time budget expires

### What Actually Happened

**Process Analysis**:
```bash
# Python processes: 65 total (1 parent + 64 workers) ✓
$ pgrep -a python3 | grep run_parallel | wc -l
65

# Active test processes: Only 2-4 at any given time ✗
$ ps aux | grep sqllogictest_suite | grep -v grep | wc -l
4

# CPU usage: Only 2-4 processes at ~100%, rest idle ✗
$ top -bn1
%Cpu(s):  3.1 us, 0.1 sy, 0.0 ni, 96.4 id
```

**Log File Analysis**:
```bash
# Workers create log files quickly
$ ls /tmp/sqllogictest_results/worker_*.log | wc -l
320  # More than 64 - workers are cycling through quickly

# Each worker only tests ~10 files before completing
$ tail -50 /tmp/sqllogictest_results/worker_3.log
# Shows:
# - Worker starts
# - Tests 10 files
# - Completes in seconds
# - Exits with summary
```

**Time Analysis**:
- Expected: Each worker runs for 3600 seconds (1 hour)
- Actual: Workers complete in seconds and exit
- Result: Workers are cycling through their work too quickly

## Root Cause Analysis

### Issue 1: Workers Exit Early Instead of Using Full Time Budget

**Location**: `tests/sqllogictest_suite.rs:66-76`

```rust
for (files_tested, test_file) in prioritized_files.into_iter().enumerate() {
    // Check time budget
    if start_time.elapsed() >= time_budget {
        println!("\n⏱️  Time budget exhausted after {} seconds", time_budget_secs);
        // ...
        break;
    }
    // Test the file...
}
```

**Problem**: The test suite iterates through `prioritized_files` **once** and then exits. If a worker's partition has only 10 files and they test quickly (few seconds each), the worker completes in ~30 seconds and exits, even though the time budget is 3600 seconds.

**Why This Happens**: Worker partitioning in `tests/sqllogictest/scheduler.rs:90-120` divides untested files among workers:

```rust
if total_workers > 1 && worker_id > 0 && worker_id <= total_workers {
    // Partition untested files among workers
    let untested_partition = partition_files(&untested_files, worker_id, total_workers);

    prioritized.extend(failed_files);      // All workers share failed files
    prioritized.extend(untested_partition); // Each worker gets 1/64th of untested
    prioritized.extend(passed_files);       // All workers share passed files
}
```

With 623 total test files and 64 workers, each worker gets ~10 untested files. They test those 10 files in seconds and exit.

### Issue 2: Python Multiprocessing Pool Serialization

**Location**: `scripts/run_parallel_tests.py:289-307`

```python
with Pool(processes=args.workers) as pool:
    worker_args = [
        (worker_id, args.workers, seed, args.time_budget, repo_root, results_dir)
        for worker_id in range(1, args.workers + 1)
    ]

    results = pool.starmap(run_worker, worker_args)  # Blocking call
```

**Status**: Actually working correctly! The Pool spawns all 64 workers in parallel.

### Issue 3: Cargo Test Subprocess Blocking

**Location**: `scripts/run_parallel_tests.py:66-97`

```python
def run_worker(worker_id, total_workers, seed, time_budget, repo_root, results_dir):
    # ...
    result = subprocess.run(  # BLOCKING call
        ["cargo", "test", "--test", "sqllogictest_suite", "--release",
         "run_sqllogictest_suite", "--", "--nocapture"],
        cwd=repo_root,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.returncode
```

**Analysis**: Each Python worker calls `subprocess.run()` which blocks until cargo completes. This is fine because:
1. All 64 Python workers run in parallel (via multiprocessing.Pool)
2. Each worker should block for ~3600 seconds while cargo runs tests
3. Problem is cargo completes in seconds due to Issue 1

### Issue 4: Potential Cargo Global Lock

**Hypothesis**: Multiple `cargo test` processes running from the same repository directory might serialize due to cargo's build lock.

**Evidence**:
- We pre-compile with `cargo test --no-run` to avoid rebuild contention
- Only 2-4 active test processes at a time despite 64 workers
- Cargo uses a jobserver for coordinating parallel builds

**Status**: Needs investigation, but likely not the primary issue since we pre-compile.

## Impact

### Current State
- ✅ All 64 workers spawn correctly
- ✅ Workers process their file partitions successfully
- ✅ Tests are running and producing results
- ❌ Workers finish in seconds instead of hours
- ❌ Poor CPU utilization (~3% vs expected ~90%+)
- ❌ Not maximizing test coverage per run
- ❌ Time budget parameter is effectively ignored

### Expected vs Actual
| Metric | Expected | Actual | Gap |
|--------|----------|--------|-----|
| Active test processes | ~64 | 2-4 | 93% fewer |
| CPU utilization | ~90% | ~3% | 30x lower |
| Runtime per worker | 3600s | ~30s | 120x faster |
| Files tested per worker | Hundreds | ~10 | 10x fewer |
| Total test coverage | Maximum | Minimal | Significant |

## Proposed Solutions

### Solution 1: Loop Workers Until Time Budget Exhausted (Recommended)

**Modify**: `tests/sqllogictest_suite.rs:66-76`

**Current**:
```rust
for (files_tested, test_file) in prioritized_files.into_iter().enumerate() {
    if start_time.elapsed() >= time_budget {
        break;
    }
    // Test file...
}
```

**Proposed**:
```rust
let mut iteration = 0;
loop {
    for (files_tested, test_file) in prioritized_files.iter().enumerate() {
        // Check time budget before EACH file
        if start_time.elapsed() >= time_budget {
            println!("\n⏱️  Time budget exhausted after {} seconds", time_budget_secs);
            return (results, files_tested);
        }

        // Test file...
    }

    iteration += 1;
    println!("Completed iteration {}, restarting file list...", iteration);

    // Check if we should continue
    if start_time.elapsed() >= time_budget {
        break;
    }
}
```

**Benefits**:
- Workers continue testing until time budget expires
- Re-tests files to catch flaky tests
- Maximizes CPU utilization
- Simple change, low risk

**Drawbacks**:
- May test same files multiple times (but that's useful for flaky test detection)
- Doesn't increase coverage beyond the partitioned file set

### Solution 2: Dynamic Work Stealing (Advanced)

**Approach**: Implement a work queue where workers can steal work from other workers' partitions.

**Implementation**:
- Shared Redis/file-based work queue
- Workers pull files from queue instead of pre-partitioned lists
- Workers continue pulling until time budget expires

**Benefits**:
- Maximum coverage - all files get tested
- Perfect load balancing
- Handles variable-speed workers

**Drawbacks**:
- Complex implementation
- Requires coordination mechanism
- Potential bottleneck at queue

### Solution 3: Hybrid - Expand Partitions with Sampling

**Modify**: `tests/sqllogictest/scheduler.rs:90-120`

**Approach**: After exhausting worker's partition, continue sampling from the full file list.

```rust
// After worker exhausts their partition, sample from all files
let untested_partition = partition_files(&untested_files, worker_id, total_workers);
let mut all_files_for_sampling = all_test_files.clone();
shuffle_with_seed(&mut all_files_for_sampling);

prioritized.extend(failed_files);
prioritized.extend(untested_partition);
prioritized.extend(all_files_for_sampling); // Add full shuffled list for continuous sampling
prioritized.extend(passed_files);
```

Combined with Solution 1's loop, this ensures workers always have work.

**Benefits**:
- Simple modification
- Increases coverage
- Works with existing architecture

### Solution 4: Investigate and Fix Cargo Serialization (If Applicable)

**Investigation Steps**:
1. Check if cargo lock file is causing serialization
2. Consider using separate `CARGO_TARGET_DIR` for each worker
3. Verify if `--release` builds are truly cached

**If Lock Contention Exists**:
```python
# Modify run_worker() to use unique target dir
env["CARGO_TARGET_DIR"] = f"{repo_root}/target/worker_{worker_id}"
```

**Verification**:
```bash
# Monitor cargo processes
watch -n 1 'ps aux | grep cargo | wc -l'

# Check for lock files
lsof /path/to/repo/target | grep -i lock
```

## Investigation Tasks

For the agent tasked with fixing this issue:

### Phase 1: Verify Root Cause

1. **Confirm Worker Behavior**:
   ```bash
   # On remote machine, monitor one worker's lifetime
   ssh rwalters-sandbox-1
   cd vibesql
   export SQLLOGICTEST_WORKER_ID=1
   export SQLLOGICTEST_TOTAL_WORKERS=64
   export SQLLOGICTEST_TIME_BUDGET=3600
   export SQLLOGICTEST_SEED=12345

   # Time how long it runs
   time cargo test --test sqllogictest_suite --release -- --nocapture
   ```

2. **Check File Partitioning**:
   - Verify how many files each worker gets
   - Check if partitions are balanced
   - Confirm workers are exiting after partition completion

3. **Monitor Cargo Processes**:
   ```bash
   # Watch cargo process creation over time
   ssh rwalters-sandbox-1 'watch -n 5 "ps aux | grep cargo | wc -l"'
   ```

4. **Check for Cargo Lock Contention**:
   ```bash
   # Look for .cargo-lock or similar
   ssh rwalters-sandbox-1 'find ~/vibesql/target -name "*lock*" -type f'
   ```

### Phase 2: Implement Solution 1 (Quick Win)

1. **Modify Test Suite Loop** (`tests/sqllogictest_suite.rs`)
   - Implement outer loop that continues until time budget
   - Add iteration counter and logging
   - Ensure time check happens frequently

2. **Test Locally**:
   ```bash
   SQLLOGICTEST_TIME_BUDGET=60 cargo test --test sqllogictest_suite -- --nocapture
   # Verify it runs for 60 seconds, not just one pass
   ```

3. **Test on Remote with Single Worker**:
   ```bash
   ./scripts/remote_test.sh --sync --workers 1 --time 300
   # Should run for 5 minutes, cycling through files
   ```

4. **Test on Remote with Multiple Workers**:
   ```bash
   ./scripts/remote_test.sh --sync --workers 8 --time 300
   # Monitor with: ssh rwalters-sandbox-1 'top'
   # Should see 8 processes at high CPU
   ```

5. **Full Parallel Test**:
   ```bash
   ./scripts/remote_quick.sh sync
   ./scripts/remote_quick.sh quick  # 8 workers, 5 min
   # Monitor CPU utilization
   ```

### Phase 3: Measure Improvement

**Metrics to Collect**:

Before Fix:
- Workers active: 2-4
- CPU utilization: ~3%
- Runtime per worker: ~30s
- Files tested per worker: ~10
- Total coverage: Minimal

After Fix:
- Workers active: Target ~64 (or at least 8 for quick test)
- CPU utilization: Target >70%
- Runtime per worker: Should match time budget
- Files tested per worker: Should be hundreds
- Total coverage: Maximized

**Monitoring Commands**:
```bash
# CPU usage over time
ssh rwalters-sandbox-1 'for i in {1..60}; do echo "=== $(date) ==="; top -bn1 | head -20; sleep 60; done'

# Process count over time
ssh rwalters-sandbox-1 'for i in {1..60}; do echo "$(date): $(ps aux | grep sqllogictest_suite | grep -v grep | wc -l) active"; sleep 60; done'

# Worker log growth (workers should keep writing)
ssh rwalters-sandbox-1 'watch -n 10 "ls -lh /tmp/sqllogictest_results/ | tail -10"'
```

### Phase 4: Documentation and Rollout

1. Document the fix in commit message
2. Update `docs/REMOTE_TESTING.md` with expected behavior
3. Add troubleshooting section about time budget
4. Create tests to prevent regression
5. Consider adding telemetry/logging for worker lifetime

## Files to Review

### Core Implementation
- `tests/sqllogictest_suite.rs` - Main test suite runner (time budget loop issue)
- `tests/sqllogictest/scheduler.rs` - File partitioning logic
- `tests/sqllogictest/execution.rs` - Individual file execution
- `scripts/run_parallel_tests.py` - Python worker orchestration

### Configuration
- `.cargo/config.toml` - Cargo configuration (check for job limits)
- `Cargo.toml` - Build settings

### Helper Scripts
- `scripts/remote_test.sh` - Remote execution wrapper
- `scripts/sqllogictest` - Unified CLI tool

### Documentation
- `docs/REMOTE_TESTING.md` - Usage guide
- `docs/TESTING.md` - General testing docs
- This file: `docs/ISSUE_REMOTE_TEST_PARALLELISM.md` - Investigation notes

## Success Criteria

The issue is resolved when:

1. ✅ Running `./scripts/remote_quick.sh quick` (8 workers, 5 min) shows:
   - All 8 workers run for full 5 minutes
   - CPU utilization >70%
   - Each worker tests hundreds of files
   - Workers cycle through files multiple times if needed

2. ✅ Running `./scripts/remote_quick.sh run` (64 workers, 1 hour) shows:
   - Sustained high CPU usage for full hour
   - All workers active throughout duration
   - Maximum test coverage achieved
   - Time budget parameter is respected

3. ✅ Monitoring shows:
   - `ps aux | grep sqllogictest | wc -l` returns 8+ consistently
   - `top` shows >70% CPU usage across cores
   - Worker log files grow continuously over full time budget

4. ✅ Results show increased coverage:
   - More files tested overall
   - Better distribution of test execution
   - Higher confidence in test suite coverage

## References

- Remote testing docs: `docs/REMOTE_TESTING.md`
- SQLLogicTest suite status: `SQLLOGICTEST_SUITE_STATUS.md`
- Parallel runner implementation: `scripts/run_parallel_tests.py`
- Scheduler implementation: `tests/sqllogictest/scheduler.rs`

## Notes

- This investigation was conducted on 2025-11-09
- Remote machine: `rwalters-sandbox-1` (64 cores, Ubuntu Linux, AWS)
- Test suite: ~623 files, ~5.9M test cases
- Current time budget: 3600s (1 hour)
- Workers spawning correctly but not utilizing time budget
- Root cause: Workers exit after completing small partitions instead of continuing until time expires
