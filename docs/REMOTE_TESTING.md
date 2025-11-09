# Remote SQLLogicTest Execution

This guide explains how to run the SQLLogicTest suite on a remote machine with many cores for maximum parallelization.

## Overview

The remote testing setup allows you to:
- Run tests on a powerful remote machine (e.g., 64 cores)
- Parallelize across all available CPUs
- Sync your local changes to the remote
- Fetch results back to analyze locally

## Quick Start

### 1. Initial Setup (One Time)

Set up the remote machine with Rust, dependencies, and the repository:

```bash
./scripts/remote_quick.sh setup
```

This will:
- Clone the repository to `~/vibesql` on the remote
- Install Rust and build tools
- Initialize the SQLLogicTest submodule
- Build the test suite in release mode

### 2. Typical Workflow

```bash
# Sync your local changes to remote
./scripts/remote_quick.sh sync

# Run the full test suite (64 workers, 1 hour)
./scripts/remote_quick.sh run

# Fetch results back to local machine
./scripts/remote_quick.sh fetch

# View results
./scripts/remote_quick.sh status
./scripts/sqllogictest punchlist
```

## Available Commands

### Quick Commands (`remote_quick.sh`)

Simple wrapper for common operations:

| Command | Description | Details |
|---------|-------------|---------|
| `setup` | Initial remote setup | Install Rust, clone repo, build |
| `sync` | Sync local code to remote | Uses rsync, excludes target/ |
| `run` | Full test run | 64 workers, 1 hour per worker |
| `quick` | Quick test | 8 workers, 5 minutes |
| `fetch` | Fetch results to local | Downloads JSON and SQL results |
| `status` | Show results summary | From fetched results |
| `clean` | Clean remote workspace | Remove target/, results |

### Advanced Commands (`remote_test.sh`)

Full control over remote execution:

```bash
# Custom worker count and time budget
./scripts/remote_test.sh --sync --workers 32 --time 1800

# Different remote host
./scripts/remote_test.sh --host my-server --sync --workers 16 --time 600

# Fetch results only (no new run)
./scripts/remote_test.sh --fetch-results

# Clean and rebuild
./scripts/remote_test.sh --clean
./scripts/remote_test.sh --sync
```

### Options for `remote_test.sh`

| Option | Default | Description |
|--------|---------|-------------|
| `--host HOST` | rwalters-sandbox-1 | Remote hostname |
| `--workers N` | auto-detect | Number of parallel workers |
| `--time N` | 3600 | Time budget (seconds) per worker |
| `--repo-path PATH` | ~/vibesql | Remote repository path |
| `--setup` | - | Perform initial setup |
| `--sync` | - | Sync local code to remote |
| `--fetch-results` | - | Fetch results to local |
| `--clean` | - | Clean remote workspace |

## Understanding Parallel Testing

### How Parallel Testing Works

The test suite uses the `run_parallel_tests.py` script which:

1. **Spawns N workers** (one per CPU core)
2. **Partitions test files** across workers using `SQLLOGICTEST_WORKER_ID` and `SQLLOGICTEST_TOTAL_WORKERS`
3. **Each worker runs independently** for the specified time budget
4. **Results are merged** into `target/sqllogictest_cumulative.json`
5. **Processed into SQLite** database at `target/sqllogictest_results.sql`

### Worker Assignment

Each worker gets a unique subset of test files:
- Worker 1 tests files where `hash(filename) % total_workers == 0`
- Worker 2 tests files where `hash(filename) % total_workers == 1`
- And so on...

This ensures:
- No file is tested twice (no wasted work)
- All files get coverage (given enough time)
- Previously-failed tests are prioritized

### Time Budget

The `--time` parameter controls how long each worker runs:
- **5 minutes (300s)**: Quick smoke test
- **1 hour (3600s)**: Standard full run
- **2+ hours**: Comprehensive coverage

Workers prioritize:
1. Previously failed tests (from last run)
2. Random sampling of remaining tests
3. Continue until time budget expires

## Analyzing Results

### Quick Summary

```bash
# After fetching results
./scripts/remote_quick.sh status
```

Shows:
- Total files tested
- Pass/fail counts
- Pass rate percentage
- Breakdown by category

### Punchlist

Generate a markdown checklist of failing tests:

```bash
./scripts/sqllogictest punchlist
cat target/sqllogictest_punchlist.md
```

### Query Database

Interactive SQL queries on results:

```bash
# Failed files
./scripts/sqllogictest query --preset failed-files

# By category
./scripts/sqllogictest query --preset by-category

# Custom SQL (requires sqlite3)
sqlite3 target/sqllogictest_results.sql "SELECT * FROM test_files WHERE status = 'failed'"
```

## Remote Machine Requirements

### System Requirements

- **OS**: Linux (Ubuntu/Debian recommended)
- **CPU**: More cores = faster coverage (64+ recommended)
- **RAM**: ~1GB per worker minimum
- **Disk**: ~10GB for repo + build artifacts
- **Network**: SSH access, outbound HTTPS for git/cargo

### Software Requirements

The `--setup` command installs:
- Rust toolchain (via rustup)
- Build essentials (gcc, make, etc.)
- Python 3 + pip
- jq (JSON processor)
- Git

## SSH Configuration

### Passwordless Access

For smooth operation, set up SSH key authentication:

```bash
# Generate SSH key if you don't have one
ssh-keygen -t ed25519

# Copy to remote machine
ssh-copy-id rwalters-sandbox-1
```

### SSH Config

Add to `~/.ssh/config` for convenience:

```
Host sandbox
    HostName rwalters-sandbox-1
    User root
    IdentityFile ~/.ssh/id_ed25519
    ServerAliveInterval 60
```

Then use: `./scripts/remote_test.sh --host sandbox`

## Troubleshooting

### "Repository not found"

Run setup first:
```bash
./scripts/remote_quick.sh setup
```

### "Rust not installed"

The setup script should install Rust. If it fails:
```bash
ssh rwalters-sandbox-1
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

### Build failures after sync

Clean and rebuild:
```bash
./scripts/remote_test.sh --clean
./scripts/remote_test.sh --sync
```

### Workers timing out

Increase time budget:
```bash
./scripts/remote_test.sh --sync --time 7200  # 2 hours
```

### Out of memory

Reduce worker count:
```bash
./scripts/remote_test.sh --sync --workers 32  # Half the cores
```

### Can't fetch results

Check files exist on remote:
```bash
ssh rwalters-sandbox-1 "ls -lh ~/vibesql/target/sqllogictest_*.json"
```

## Advanced Usage

### Running on Multiple Machines

You can run different test sets on different machines:

```bash
# Machine 1: Focus on random tests
ssh machine1 "cd vibesql && SQLLOGICTEST_SEED=12345 ./scripts/sqllogictest run --parallel --workers 64 --time 3600"

# Machine 2: Focus on evidence tests
ssh machine2 "cd vibesql && SQLLOGICTEST_SEED=67890 ./scripts/sqllogictest run --parallel --workers 64 --time 3600"

# Merge results locally
./scripts/merge_sqllogictest_results.py \
    --inputs machine1_results.json machine2_results.json \
    --output combined_results.json
```

### Custom Test Selection

Override environment variables for control:

```bash
ssh rwalters-sandbox-1 << 'EOF'
cd vibesql
export SQLLOGICTEST_SEED=42
export SQLLOGICTEST_TIME_BUDGET=1800
./scripts/sqllogictest run --parallel --workers 64
EOF
```

### Continuous Testing

Set up a cron job on the remote for periodic testing:

```bash
# Run tests every 6 hours
0 */6 * * * cd ~/vibesql && git pull && ./scripts/sqllogictest run --parallel --workers 64 --time 3600
```

## See Also

- [TESTING.md](TESTING.md) - General testing documentation
- [scripts/sqllogictest](../scripts/sqllogictest) - Main testing tool
- [scripts/run_parallel_tests.py](../scripts/run_parallel_tests.py) - Parallel execution engine
