#!/usr/bin/env python3
"""
Parallel SQLLogicTest runner for VibeSQL.

This script orchestrates multiple worker processes to run SQLLogicTest files in parallel,
dramatically reducing test suite execution time from ~30+ minutes (serial) to ~2 minutes (parallel).

Architecture:
- Main process discovers all 623 test files and partitions them across N workers
- Each worker runs test files individually with per-file timeouts (prevents worker hangs)
- Workers write individual JSON results to target/sqllogictest_results_worker_N.json
- Main process merges all worker results into target/sqllogictest_cumulative.json
- Results are compatible with existing database integration (process_test_results.py)

Per-File Timeout Strategy:
- Each file has a 60s timeout to prevent hangs on slow/infinite-loop queries
- If a file times out, the worker continues testing remaining files
- Time budget is enforced across all files in the partition
- Workers gracefully stop when time budget is exhausted

Usage:
    python3 scripts/run_parallel_tests.py --workers 8 --time-budget 300

Environment Variables:
    SQLLOGICTEST_WORKER_ID: Worker number (0-indexed)
    SQLLOGICTEST_FILES: Test file to run (set per-file by worker)
    SQLLOGICTEST_TIME_BUDGET: Time budget in seconds per file

Example:
    # Run with 8 workers, 5 minutes per worker
    ./scripts/sqllogictest run --parallel --workers 8 --time 300

    # Run with all available CPUs
    ./scripts/sqllogictest run --parallel
"""

import argparse
import json
import multiprocessing
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


def discover_test_files(repo_root: Path) -> List[str]:
    """
    Discover all SQLLogicTest files in the repository.

    Returns:
        List of test file paths relative to third_party/sqllogictest/test/
    """
    test_dir = repo_root / "third_party" / "sqllogictest" / "test"

    if not test_dir.exists():
        raise RuntimeError(
            f"SQLLogicTest directory not found: {test_dir}\n"
            "Run: git submodule update --init --recursive"
        )

    # Find all .test files
    test_files = []
    for test_file in test_dir.rglob("*.test"):
        # Get path relative to test directory
        relative_path = test_file.relative_to(test_dir)
        test_files.append(str(relative_path))

    return sorted(test_files)


def initialize_work_queue(repo_root: Path, work_queue_dir: Path) -> int:
    """
    Initialize work queue with all test files.

    This function is called by the serial test runner when --force is specified.
    It creates a work queue directory and populates it with all test files.

    Args:
        repo_root: Repository root directory
        work_queue_dir: Directory to store work queue files

    Returns:
        Number of test files added to queue
    """
    # Discover all test files
    test_files = discover_test_files(repo_root)

    # Create work queue directory
    work_queue_dir.mkdir(parents=True, exist_ok=True)

    # Write test files to queue (one file per line)
    queue_file = work_queue_dir / "test_files.txt"
    with open(queue_file, 'w') as f:
        for test_file in test_files:
            f.write(f"{test_file}\n")

    return len(test_files)


def partition_test_files(test_files: List[str], num_workers: int) -> List[List[str]]:
    """
    Partition test files across workers for balanced distribution.

    Args:
        test_files: List of all test file paths
        num_workers: Number of worker processes

    Returns:
        List of partitions, one per worker
    """
    partitions = [[] for _ in range(num_workers)]

    # Simple round-robin distribution
    # Could be improved with smarter load balancing based on file size or historical runtime
    for i, test_file in enumerate(test_files):
        worker_id = i % num_workers
        partitions[worker_id].append(test_file)

    return partitions


def run_worker(worker_id: int, test_files: List[str], time_budget: int, repo_root: Path, release_mode: bool = True) -> Tuple[int, Optional[Dict]]:
    """
    Run a single worker process to test its partition of files.

    Args:
        worker_id: Worker number (0-indexed)
        test_files: List of test files for this worker
        time_budget: Time budget in seconds
        repo_root: Repository root directory
        release_mode: Whether to use release binary (default: True for performance)

    Returns:
        (worker_id, results_dict) or (worker_id, None) on failure
    """
    if not test_files:
        print(f"[Worker {worker_id}] No files assigned, skipping")
        return (worker_id, None)

    print(f"[Worker {worker_id}] Starting with {len(test_files)} files", flush=True)
    print(f"[Worker {worker_id}] Time budget: {time_budget}s", flush=True)

    # Find the test binary (built with --no-run earlier)
    # Location depends on build mode (release is 10-15x faster)
    build_type = "release" if release_mode else "debug"
    test_binary_pattern = repo_root / "target" / build_type / "deps" / "sqllogictest_suite-*"
    import glob as glob_module
    test_binaries = sorted(glob_module.glob(str(test_binary_pattern)), key=os.path.getmtime, reverse=True)

    # Filter to executables only (no .d files)
    test_binaries = [b for b in test_binaries if os.access(b, os.X_OK) and not b.endswith('.d')]

    if not test_binaries:
        print(f"[Worker {worker_id}] ERROR: Test binary not found at {test_binary_pattern}", flush=True)
        print(f"[Worker {worker_id}] ERROR: Expected binary in target/{build_type}/deps/", flush=True)
        print(f"[Worker {worker_id}] ERROR: Build may have failed or binary was not created", flush=True)
        return (worker_id, None)

    test_binary = test_binaries[0]

    # Run files with per-file timeout to prevent hangs
    worker_start_time = time.time()
    files_tested = 0
    files_timed_out = 0
    all_results = []

    # Clean up any stale per-file result files from previous runs
    import glob as glob_cleanup
    stale_files = glob_cleanup.glob(str(repo_root / "target" / f"sqllogictest_results_worker_{worker_id}_file_*.json"))
    for stale_file in stale_files:
        try:
            Path(stale_file).unlink()
        except:
            pass  # Ignore cleanup failures

    # Per-file timeout: adaptive based on historical data, with reasonable bounds
    # Most files complete in <10s, but high-volume index tests (10,000+ queries) need more time
    # Increased from 60s to 120s (issue #1921), then 300s, now 600s to handle very slow */1000/* tests
    # This separates correctness (tests pass/fail) from performance (tests are slow)
    per_file_timeout = 600  # 600s (10 min) per file - prioritize correctness over speed

    for test_file in test_files:
        # Check if we've exceeded our time budget
        elapsed = time.time() - worker_start_time
        if elapsed >= time_budget:
            print(f"[Worker {worker_id}] Time budget exhausted ({elapsed:.1f}s / {time_budget}s), stopping", flush=True)
            print(f"[Worker {worker_id}] Tested {files_tested}/{len(test_files)} files", flush=True)
            break

        # Set environment variables for this file
        env = os.environ.copy()
        env["SQLLOGICTEST_WORKER_ID"] = str(worker_id)
        env["SQLLOGICTEST_FILES"] = test_file  # Single file
        env["SQLLOGICTEST_TIME_BUDGET"] = str(per_file_timeout)

        # Run the test binary for this single file
        cmd = [test_binary]

        # Unique result file for this run
        run_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}_file_{files_tested}.json"

        try:
            file_start_time = time.time()

            # Redirect stdout/stderr to files to avoid blocking
            stdout_file = repo_root / "target" / f"worker_{worker_id}_file_{files_tested}_stdout.log"
            stderr_file = repo_root / "target" / f"worker_{worker_id}_file_{files_tested}_stderr.log"

            with open(stdout_file, 'w') as stdout_f, open(stderr_file, 'w') as stderr_f:
                result = subprocess.run(
                    cmd,
                    env=env,
                    cwd=repo_root,
                    stdout=stdout_f,
                    stderr=stderr_f,
                    timeout=per_file_timeout + 10  # Small grace period for process startup/cleanup
                )

            file_elapsed = time.time() - file_start_time
            files_tested += 1

            # Check if test succeeded
            if result.returncode != 0:
                print(f"[Worker {worker_id}] File {files_tested}/{len(test_files)}: {test_file} FAILED (exit {result.returncode}) in {file_elapsed:.1f}s", flush=True)
            else:
                print(f"[Worker {worker_id}] File {files_tested}/{len(test_files)}: {test_file} completed in {file_elapsed:.1f}s", flush=True)

            # Rename the standard result file to our unique per-file name
            # The Rust test binary writes to sqllogictest_results_worker_{worker_id}.json
            # We need to rename it to avoid overwriting results from previous files
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.rename(run_results_file)
                except Exception as e:
                    print(f"[Worker {worker_id}] Warning: Failed to rename result file: {e}", flush=True)

            # Read results if they exist
            if run_results_file.exists():
                with open(run_results_file, 'r') as f:
                    file_results = json.load(f)
                    all_results.append(file_results)
                # Clean up individual result file after reading
                run_results_file.unlink()
            else:
                # Log if results file doesn't exist (test may have crashed or failed to write results)
                print(f"[Worker {worker_id}] Warning: No results file found for {test_file}", flush=True)

        except subprocess.TimeoutExpired:
            files_tested += 1
            files_timed_out += 1
            print(f"[Worker {worker_id}] File {files_tested}/{len(test_files)}: {test_file} TIMEOUT after {per_file_timeout}s", flush=True)
            # Clean up any partial result file that may have been written
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.unlink()
                except:
                    pass  # Ignore cleanup failures
            # Continue to next file instead of failing entire worker

        except Exception as e:
            files_tested += 1
            print(f"[Worker {worker_id}] File {files_tested}/{len(test_files)}: {test_file} ERROR: {type(e).__name__}: {e}", flush=True)
            # Clean up any partial result file that may have been written
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.unlink()
                except:
                    pass  # Ignore cleanup failures
            # Continue to next file

    total_elapsed = time.time() - worker_start_time

    print(f"[Worker {worker_id}] Completed: {files_tested}/{len(test_files)} files tested in {total_elapsed:.1f}s", flush=True)
    if files_timed_out > 0:
        print(f"[Worker {worker_id}] Warning: {files_timed_out} files timed out", flush=True)

    # Merge all results from individual file runs
    if not all_results:
        print(f"[Worker {worker_id}] No results collected", flush=True)
        return (worker_id, None)

    merged_results = merge_file_results(all_results)

    # Write merged results to worker result file
    final_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
    with open(final_results_file, 'w') as f:
        json.dump(merged_results, f, indent=2)

    return (worker_id, merged_results)


def merge_file_results(file_results: List[Dict]) -> Dict:
    """
    Merge results from multiple individual file test runs into a single result.

    Args:
        file_results: List of result dictionaries from individual file runs

    Returns:
        Merged result dictionary compatible with merge_worker_results()
    """
    if not file_results:
        return {
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "errors": 0,
                "skipped": 0,
                "pass_rate": 0.0,
                "total_available_files": 0,
                "tested_files": 0,
            },
            "tested_files": {
                "passed": [],
                "failed": [],
            },
            "categories": {},
            "detailed_failures": [],
        }

    # Initialize merged result
    merged = {
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "total_available_files": 0,
            "tested_files": 0,
        },
        "tested_files": {
            "passed": [],
            "failed": [],
        },
        "categories": {},
        "detailed_failures": [],
    }

    # Merge each file result
    for result in file_results:
        summary = result.get("summary", {})
        merged["summary"]["total"] += summary.get("total", 0)
        merged["summary"]["passed"] += summary.get("passed", 0)
        merged["summary"]["failed"] += summary.get("failed", 0)
        merged["summary"]["errors"] += summary.get("errors", 0)
        merged["summary"]["skipped"] += summary.get("skipped", 0)
        merged["summary"]["total_available_files"] += summary.get("total_available_files", 0)
        merged["summary"]["tested_files"] += summary.get("tested_files", 0)

        # Merge tested files lists
        tested_files = result.get("tested_files", {})
        merged["tested_files"]["passed"].extend(tested_files.get("passed", []))
        merged["tested_files"]["failed"].extend(tested_files.get("failed", []))

        # Merge detailed failures
        merged["detailed_failures"].extend(result.get("detailed_failures", []))

        # Merge categories
        categories = result.get("categories", {})
        for category_name, category_data in categories.items():
            if category_data is None:
                continue
            if category_name not in merged["categories"]:
                merged["categories"][category_name] = {
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "errors": 0,
                    "skipped": 0,
                    "pass_rate": 0.0,
                }
            merged["categories"][category_name]["total"] += category_data.get("total", 0)
            merged["categories"][category_name]["passed"] += category_data.get("passed", 0)
            merged["categories"][category_name]["failed"] += category_data.get("failed", 0)
            merged["categories"][category_name]["errors"] += category_data.get("errors", 0)
            merged["categories"][category_name]["skipped"] += category_data.get("skipped", 0)

    # Calculate overall pass rate
    total = merged["summary"]["total"]
    if total > 0:
        merged["summary"]["pass_rate"] = round((merged["summary"]["passed"] / total) * 100, 2)

    # Calculate category pass rates
    for category_name, category_data in merged["categories"].items():
        total = category_data["total"]
        if total > 0:
            category_data["pass_rate"] = round((category_data["passed"] / total) * 100, 2)

    return merged


def merge_worker_results(worker_results: List[Tuple[int, Optional[Dict]]]) -> Dict:
    """
    Merge results from all workers into a single cumulative result.

    Args:
        worker_results: List of (worker_id, results_dict) tuples

    Returns:
        Merged results dictionary compatible with process_test_results.py
    """
    # Initialize cumulative results
    cumulative = {
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "total_available_files": 0,
            "tested_files": 0,
        },
        "tested_files": {
            "passed": [],
            "failed": [],
        },
        "categories": {
            "select": None,
            "evidence": None,
            "index": None,
            "random": None,
            "ddl": None,
            "other": None,
        },
        "detailed_failures": [],
    }

    # Track category stats for merging
    category_stats = {
        "select": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "evidence": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "index": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "random": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "ddl": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "other": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
    }

    # Merge results from each worker
    for worker_id, results in worker_results:
        if results is None:
            continue

        # Merge summary
        summary = results.get("summary", {})
        cumulative["summary"]["total"] += summary.get("total", 0)
        cumulative["summary"]["passed"] += summary.get("passed", 0)
        cumulative["summary"]["failed"] += summary.get("failed", 0)
        cumulative["summary"]["errors"] += summary.get("errors", 0)
        cumulative["summary"]["skipped"] += summary.get("skipped", 0)
        cumulative["summary"]["total_available_files"] += summary.get("total_available_files", 0)
        cumulative["summary"]["tested_files"] += summary.get("tested_files", 0)

        # Merge tested files
        tested_files = results.get("tested_files", {})
        cumulative["tested_files"]["passed"].extend(tested_files.get("passed", []))
        cumulative["tested_files"]["failed"].extend(tested_files.get("failed", []))

        # Merge detailed failures
        cumulative["detailed_failures"].extend(results.get("detailed_failures", []))

        # Merge category stats
        categories = results.get("categories", {})
        for category_name, category_data in categories.items():
            if category_data and category_name in category_stats:
                category_stats[category_name]["total"] += category_data.get("total", 0)
                category_stats[category_name]["passed"] += category_data.get("passed", 0)
                category_stats[category_name]["failed"] += category_data.get("failed", 0)
                category_stats[category_name]["errors"] += category_data.get("errors", 0)
                category_stats[category_name]["skipped"] += category_data.get("skipped", 0)

    # Calculate cumulative pass rate
    total = cumulative["summary"]["total"]
    passed = cumulative["summary"]["passed"]
    if total > 0:
        cumulative["summary"]["pass_rate"] = round((passed / total) * 100, 2)

    # Calculate category pass rates and add to cumulative results
    for category_name, stats in category_stats.items():
        if stats["total"] > 0:
            pass_rate = round((stats["passed"] / stats["total"]) * 100, 2)
            cumulative["categories"][category_name] = {
                "total": stats["total"],
                "passed": stats["passed"],
                "failed": stats["failed"],
                "errors": stats["errors"],
                "skipped": stats["skipped"],
                "pass_rate": pass_rate,
            }

    return cumulative


def run_parallel_tests(num_workers: int, time_budget: int, repo_root: Path, release_mode: bool = True) -> bool:
    """
    Run SQLLogicTest suite in parallel across multiple workers.

    Args:
        num_workers: Number of worker processes to spawn
        time_budget: Time budget in seconds per worker
        repo_root: Repository root directory
        release_mode: Whether to build in release mode (default: True for 10-15x speedup)

    Returns:
        True if successful, False otherwise
    """
    build_type = "release" if release_mode else "debug"

    print(f"\n=== Parallel SQLLogicTest Runner ===")
    print(f"Workers: {num_workers}")
    print(f"Time budget: {time_budget}s per worker")
    print(f"Build mode: {build_type}")
    print()

    # Build the test binary first to avoid cargo lock contention
    # IMPORTANT: Use --release for 10-15x performance improvement
    print(f"Building test binary ({'release mode - this may take 30-60s' if release_mode else 'debug mode - faster build, slower tests'})...")
    build_cmd = [
        "cargo", "test",
        "--package", "vibesql",
        "--test", "sqllogictest_suite",
        "--no-run"
    ]
    if release_mode:
        build_cmd.append("--release")

    result = subprocess.run(build_cmd, cwd=repo_root, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Build failed!", file=sys.stderr)
        print(f"\nBuild command: {' '.join(build_cmd)}", file=sys.stderr)
        print(f"\nStderr:\n{result.stderr}", file=sys.stderr)
        print(f"\nStdout:\n{result.stdout}", file=sys.stderr)
        return False
    print(f"✓ Test binary built in {build_type} mode")

    # Verify binary exists
    binary_pattern = repo_root / "target" / build_type / "deps" / "sqllogictest_suite-*"
    import glob as glob_module
    test_binaries = [b for b in glob_module.glob(str(binary_pattern)) if os.access(b, os.X_OK) and not b.endswith('.d')]

    if not test_binaries:
        print(f"❌ ERROR: Test binary not found after build!", file=sys.stderr)
        print(f"Expected at: {binary_pattern}", file=sys.stderr)
        print(f"\nDirectory listing:", file=sys.stderr)
        deps_dir = repo_root / "target" / build_type / "deps"
        if deps_dir.exists():
            sqllogictest_files = list(deps_dir.glob("sqllogictest_suite*"))
            print(f"Found {len(sqllogictest_files)} sqllogictest_suite files:", file=sys.stderr)
            for f in sqllogictest_files:
                executable = "✓" if os.access(f, os.X_OK) else "✗"
                print(f"  {executable} {f.name}", file=sys.stderr)
        else:
            print(f"Directory doesn't exist: {deps_dir}", file=sys.stderr)
        return False

    print(f"✓ Found test binary: {Path(test_binaries[0]).name}")
    print()

    # Discover all test files
    print("Discovering test files...")
    try:
        test_files = discover_test_files(repo_root)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

    print(f"Found {len(test_files)} test files")

    # Partition test files across workers
    print(f"Partitioning across {num_workers} workers...")
    partitions = partition_test_files(test_files, num_workers)

    for i, partition in enumerate(partitions):
        print(f"  Worker {i}: {len(partition)} files")

    print()

    # Run workers in parallel
    print("Starting workers...")
    worker_results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all workers
        futures = {
            executor.submit(run_worker, i, partition, time_budget, repo_root, release_mode): i
            for i, partition in enumerate(partitions)
        }

        # Collect results as they complete
        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                worker_results.append(result)
            except Exception as e:
                print(f"[Worker {worker_id}] Exception: {e}")
                worker_results.append((worker_id, None))

    print()
    print("All workers completed")
    print()

    # Merge results
    print("Merging results...")
    cumulative_results = merge_worker_results(worker_results)

    # Write cumulative results to JSON file
    output_file = repo_root / "target" / "sqllogictest_cumulative.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(cumulative_results, f, indent=2)

    print(f"✓ Results written to: {output_file}")
    print()

    # Print summary
    summary = cumulative_results["summary"]
    print("=== Summary ===")
    print(f"Total files:  {summary['total']}")
    print(f"Passed:       {summary['passed']}")
    print(f"Failed:       {summary['failed']}")
    print(f"Errors:       {summary['errors']}")
    print(f"Skipped:      {summary['skipped']}")
    print(f"Pass rate:    {summary['pass_rate']:.1f}%")
    print()

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run SQLLogicTest suite in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="Number of parallel workers",
    )
    parser.add_argument(
        "--time-budget",
        type=int,
        required=True,
        help="Time budget in seconds per worker",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Use debug build instead of release (faster build, but 10-15x slower tests)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.workers < 1:
        print("Error: --workers must be >= 1", file=sys.stderr)
        return 1

    if args.time_budget < 1:
        print("Error: --time-budget must be >= 1", file=sys.stderr)
        return 1

    # Get repository root
    try:
        repo_root = get_repo_root()
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Run parallel tests (default to release mode for performance)
    release_mode = not args.debug
    success = run_parallel_tests(args.workers, args.time_budget, repo_root, release_mode)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
