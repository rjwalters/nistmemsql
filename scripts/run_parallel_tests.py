#!/usr/bin/env python3
"""
Parallel SQLLogicTest runner for VibeSQL.

This script orchestrates multiple worker processes to run SQLLogicTest files in parallel,
dramatically reducing test suite execution time from ~30+ minutes (serial) to ~2 minutes (parallel).

Architecture:
- Main process discovers all 623 test files and partitions them across N workers
- Each worker runs `cargo test` with its partition of test files
- Workers write individual JSON results to target/sqllogictest_results_worker_N.json
- Main process merges all worker results into target/sqllogictest_cumulative.json
- Results are compatible with existing database integration (process_test_results.py)

Usage:
    python3 scripts/run_parallel_tests.py --workers 8 --time-budget 300

Environment Variables:
    SQLLOGICTEST_WORKER_ID: Worker number (0-indexed)
    SQLLOGICTEST_FILES: Comma-separated list of test files for this worker
    SQLLOGICTEST_TIME_BUDGET: Time budget in seconds per worker

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
from concurrent.futures import ProcessPoolExecutor, as_completed
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


def run_worker(worker_id: int, test_files: List[str], time_budget: int, repo_root: Path) -> Tuple[int, Optional[Dict]]:
    """
    Run a single worker process to test its partition of files.

    Args:
        worker_id: Worker number (0-indexed)
        test_files: List of test files for this worker
        time_budget: Time budget in seconds
        repo_root: Repository root directory

    Returns:
        (worker_id, results_dict) or (worker_id, None) on failure
    """
    if not test_files:
        print(f"[Worker {worker_id}] No files assigned, skipping")
        return (worker_id, None)

    print(f"[Worker {worker_id}] Starting with {len(test_files)} files")

    # Set environment variables for this worker
    env = os.environ.copy()
    env["SQLLOGICTEST_WORKER_ID"] = str(worker_id)
    env["SQLLOGICTEST_FILES"] = ",".join(test_files)
    env["SQLLOGICTEST_TIME_BUDGET"] = str(time_budget)

    # Run cargo test for this worker
    # The Rust test suite detects SQLLOGICTEST_WORKER_ID and outputs to worker-specific file
    cmd = [
        "cargo", "test",
        "--package", "vibesql",
        "--test", "sqllogictest_suite",
        "run_sqllogictest_suite",
        "--",
        "--nocapture"
    ]

    try:
        start_time = time.time()

        result = subprocess.run(
            cmd,
            env=env,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=time_budget + 60  # Allow 60s grace period for startup/shutdown
        )

        elapsed = time.time() - start_time

        # Check if test succeeded
        if result.returncode != 0:
            print(f"[Worker {worker_id}] Failed (exit code {result.returncode})")
            print(f"[Worker {worker_id}] stderr: {result.stderr[:500]}")
            return (worker_id, None)

        print(f"[Worker {worker_id}] Completed in {elapsed:.1f}s")

        # Read worker results from JSON file
        results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"

        if not results_file.exists():
            print(f"[Worker {worker_id}] Warning: Results file not found: {results_file}")
            return (worker_id, None)

        with open(results_file, 'r') as f:
            results = json.load(f)

        return (worker_id, results)

    except subprocess.TimeoutExpired:
        print(f"[Worker {worker_id}] Timeout after {time_budget}s")
        return (worker_id, None)
    except Exception as e:
        print(f"[Worker {worker_id}] Error: {e}")
        return (worker_id, None)


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


def run_parallel_tests(num_workers: int, time_budget: int, repo_root: Path) -> bool:
    """
    Run SQLLogicTest suite in parallel across multiple workers.

    Args:
        num_workers: Number of worker processes to spawn
        time_budget: Time budget in seconds per worker
        repo_root: Repository root directory

    Returns:
        True if successful, False otherwise
    """
    print(f"\n=== Parallel SQLLogicTest Runner ===")
    print(f"Workers: {num_workers}")
    print(f"Time budget: {time_budget}s per worker")
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

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all workers
        futures = {
            executor.submit(run_worker, i, partition, time_budget, repo_root): i
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

    # Run parallel tests
    success = run_parallel_tests(args.workers, args.time_budget, repo_root)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
