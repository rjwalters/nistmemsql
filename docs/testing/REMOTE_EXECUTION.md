# Remote Execution Guide: Full SQLLogicTest Suite

Instructions for running the complete SQLLogicTest suite on a remote high-performance machine.

## Quick Start

```bash
# 1. SSH to sandbox and setup
ssh rwalters-sandbox-1
git clone https://github.com/rjwalters/vibesql.git
cd vibesql
git submodule update --init --recursive

# 2. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# 3. Run full test suite (320 workers, 1 hour each)
python3 scripts/run_parallel_tests.py --workers 320 --time-budget 3600
```

## Hardware Specifications

Current sandbox: `rwalters-sandbox-1`
- **CPUs**: 64 cores
- **RAM**: 124GB
- **Recommended Workers**: 320 (5x oversubscription)
- **Platform**: AWS x86_64

## Recommended Configurations

### Full Coverage Run (Recommended)

Use 5x CPU oversubscription for maximum throughput on I/O-bound test workloads:

```bash
python3 scripts/run_parallel_tests.py --workers 320 --time-budget 3600
```

**Expected Results:**
- **Duration**: ~1-2 hours (depending on convergence)
- **Coverage**: Near 100% of 623 test files
- **Tests Executed**: ~5.9 million tests across all files

### Quick Validation Run

Test with fewer workers and shorter time:

```bash
python3 scripts/run_parallel_tests.py --workers 64 --time-budget 600
```

**Expected Results:**
- **Duration**: ~10-15 minutes
- **Coverage**: ~50-70% of test files
- **Tests Executed**: ~2-3 million tests

### Conservative Run

Use 1:1 CPU mapping (no oversubscription):

```bash
python3 scripts/run_parallel_tests.py --workers 64 --time-budget 3600
```

**Expected Results:**
- **Duration**: ~1-2 hours
- **Coverage**: ~80-90% of test files
- **Tests Executed**: ~4-5 million tests

## Why 5x Oversubscription?

SQLLogicTest workloads are **I/O and memory-bound**, not CPU-bound:
- Test files must be read from disk
- SQL parsing has memory allocation overhead
- Query execution involves cache lookups and data structure traversal
- Workers spend significant time waiting on I/O

With 320 workers on 64 cores, the OS scheduler efficiently manages CPU time across waiting workers, maximizing throughput.

## Remote Execution Workflow

### Option 1: Interactive tmux Session

```bash
# SSH and start tmux
ssh rwalters-sandbox-1
tmux new -s sqllogictest

# Clone and setup
git clone https://github.com/rjwalters/vibesql.git
cd vibesql
git submodule update --init --recursive
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Run tests with logging
python3 scripts/run_parallel_tests.py --workers 320 --time-budget 3600 2>&1 | \
  tee sqllogictest_run_$(date +%Y%m%d_%H%M%S).log

# Detach: Ctrl+B, then D
# Reattach later: tmux attach -t sqllogictest
```

### Option 2: Non-interactive nohup

```bash
ssh rwalters-sandbox-1 << 'EOF'
cd vibesql
nohup python3 scripts/run_parallel_tests.py --workers 320 --time-budget 3600 \
  > sqllogictest_$(date +%Y%m%d_%H%M%S).log 2>&1 &
EOF

# Check progress
ssh rwalters-sandbox-1 "tail -f vibesql/sqllogictest_*.log"
```

### Option 3: Direct from Local Machine

```bash
# Run everything via single SSH command
ssh rwalters-sandbox-1 << 'EOF'
  cd vibesql || git clone https://github.com/rjwalters/vibesql.git && cd vibesql
  git pull
  git submodule update --init --recursive

  # Ensure Rust is installed
  if ! command -v cargo &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
  fi

  # Run tests
  python3 scripts/run_parallel_tests.py --workers 320 --time-budget 3600
EOF
```

## Monitoring Progress

### Check Worker Status

```bash
# See how many workers are running
ssh rwalters-sandbox-1 "ps aux | grep cargo | grep sqllogictest | wc -l"

# View CPU usage
ssh rwalters-sandbox-1 "top -bn1 | head -20"

# Check memory usage
ssh rwalters-sandbox-1 "free -h"
```

### Monitor Logs

```bash
# Watch latest log file
ssh rwalters-sandbox-1 "tail -f vibesql/sqllogictest_*.log"

# Check worker-specific logs
ssh rwalters-sandbox-1 "tail -f /tmp/sqllogictest_results/worker_*.log"
```

### Estimate Time Remaining

Each worker processes test files until time budget expires. Progress depends on:
- File complexity (tests per file varies widely)
- Test execution speed (simple SELECTs vs complex JOINs)
- I/O performance

**Typical Progress Rate** (320 workers):
- **First 30 min**: ~40-50% coverage (easy files)
- **Next 30 min**: ~30-40% more coverage (medium files)
- **Final 30+ min**: ~10-20% more coverage (hard files)

## Retrieving Results

### Option 1: Download Cumulative Results

```bash
# Download merged results from sandbox
scp rwalters-sandbox-1:~/vibesql/target/sqllogictest_cumulative.json .

# Copy to local target directory
mkdir -p target
cp sqllogictest_cumulative.json target/

# Commit and push to update gh-pages
git add target/sqllogictest_cumulative.json
git commit -m "feat: Update SQLLogicTest results from full remote run"
git push
```

### Option 2: Sync Entire Target Directory

```bash
# Sync target/ directory including all worker results
rsync -avz --progress \
  rwalters-sandbox-1:~/vibesql/target/sqllogictest*.json \
  ./target/
```

### Option 3: Trigger GitHub Deployment

After retrieving results, trigger a deployment to update badges:

```bash
# Copy cumulative results to artifacts for manual deployment
mkdir -p artifacts
cp target/sqllogictest_cumulative.json artifacts/sqllogictest_merged.json

# Manually trigger deployment workflow
gh workflow run deploy-pages.yml \
  -f artifact-name='' \
  -f rebuild-web-demo=false
```

Or upload as an artifact and let CI handle it:

```bash
# The aggregate-results step in boost workflow will pick this up
# Just commit the cumulative results
git add target/sqllogictest_cumulative.json
git commit -m "feat: Full SQLLogicTest coverage from remote execution (320 workers, 1 hour)"
git push
```

## Expected Output

### During Execution

```
=== Parallel SQLLogicTest Suite Runner ===
Workers: 320
Time budget per worker: 3600s
Shared seed: 1699123456
Results directory: /tmp/sqllogictest_results
Repository: /root/vibesql

Pre-compiling test binary...
✓ Test binary compiled

All 320 workers started! Waiting for completion...
Monitor progress:
  tail -f /tmp/sqllogictest_results/worker_*.log

Starting worker 1/320...
Starting worker 2/320...
...
Starting worker 320/320...
```

### After Completion

```
=== All Workers Complete ===
Total time: 4235.3s

Merging results from all workers...
Merging worker 1...
Merging worker 2...
...
Merging worker 320...

=== Final Results ===
{
  "total_available_files": 623,
  "total_tested_files": 612,
  "coverage_rate": 98.2,
  "total_passed_files": 534,
  "total_failed_files": 78,
  "pass_rate": 87.3,
  "total_tests_passed": 5142823,
  "total_tests_failed": 748291
}

Results saved to /root/vibesql/target/sqllogictest_cumulative.json
```

## Performance Benchmarks

Based on test runs with bucket-brigade (similar Rust test harness):

| Workers | Time Budget | Expected Coverage | Wall Time |
|---------|-------------|-------------------|-----------|
| 64      | 600s        | ~60%             | ~10-15 min |
| 64      | 3600s       | ~85%             | ~1 hour    |
| 320     | 600s        | ~75%             | ~10-15 min |
| 320     | 3600s       | ~98%             | ~1-2 hours |

**Note**: Actual results depend on MySQL feature completeness. With recent MySQL additions, expect higher pass rates than historical runs.

## Troubleshooting

### Out of Memory

Despite 124GB RAM, 320 workers may exhaust memory if test files are large. Reduce workers:

```bash
python3 scripts/run_parallel_tests.py --workers 160 --time-budget 3600
```

### Disk I/O Bottleneck

If disk I/O is saturated, reduce parallelism:

```bash
python3 scripts/run_parallel_tests.py --workers 128 --time-budget 3600
```

### Compilation Errors

Ensure Rust toolchain is current:

```bash
rustup update
cargo clean
cargo test --test sqllogictest_suite --release --no-run
```

### Worker Crashes

Check individual worker logs:

```bash
grep -l "panic\|error\|FAILED" /tmp/sqllogictest_results/worker_*.log
```

## Cost Optimization

The sandbox is billed per hour. To minimize cost:

1. **Pre-compile before large runs**: `cargo test --release --no-run`
2. **Use appropriate worker count**: Don't over-provision
3. **Stop sandbox when done**: `sky stop rwalters-sandbox-1`
4. **Check status before starting**: `sky status`

## Integration with CI

After a successful remote run, the results can feed back into CI:

1. **Download cumulative results** from sandbox
2. **Commit to main branch** (or create PR)
3. **CI workflows merge** with historical data
4. **Badges update** automatically via GitHub Pages

This allows manual "boost" runs on powerful hardware while maintaining continuous coverage tracking in CI.

## Next Steps After Completion

1. **Review pass/fail rates**: Check if MySQL features improved coverage
2. **Analyze failures**: Use `scripts/analyze_sqllogictest_failures.py`
3. **Update GitHub Pages**: Trigger deploy-pages workflow
4. **Document improvements**: Update README badges and conformance report
5. **Stop sandbox**: `sky stop rwalters-sandbox-1` to save costs

---

*Last updated: 2025-11-05*
