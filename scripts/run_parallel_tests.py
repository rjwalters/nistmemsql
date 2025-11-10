#!/usr/bin/env python3
"""
Parallel SQLLogicTest Suite Runner

Runs the SQLLogicTest suite across multiple parallel workers to maximize CPU utilization.
Each worker gets a unique partition of test files determined by SQLLOGICTEST_WORKER_ID
and SQLLOGICTEST_TOTAL_WORKERS environment variables.

Usage:
    python scripts/run_parallel_tests.py [--workers N] [--time-budget SECONDS]

Examples:
    # Run with 20 workers, 1 hour per worker
    python scripts/run_parallel_tests.py --workers 20 --time-budget 3600

    # Use all available CPUs
    python scripts/run_parallel_tests.py --workers $(nproc)

    # Quick test with 4 workers, 5 minutes each
    python scripts/run_parallel_tests.py --workers 4 --time-budget 300
"""

import argparse
import glob
import json
import multiprocessing
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


def is_mysql_specific(test_file_path: Path) -> bool:
    """
    Check if test file contains MySQL-specific syntax.

    MySQL-specific features we filter out:
    - System variables: @@sql_mode, @@session.variable_name
    - Session management: SET SESSION
    - MySQL sql_mode flags: ONLY_FULL_GROUP_BY

    These are MySQL extensions not part of SQL:1999 standard.

    Args:
        test_file_path: Path to the test file

    Returns:
        True if the test contains MySQL-specific syntax, False otherwise
    """
    mysql_patterns = [
        r'@@\w+',                    # System variables like @@sql_mode
        r'SET\s+SESSION',            # Session settings
        r'ONLY_FULL_GROUP_BY',       # MySQL sql_mode flag
    ]

    try:
        content = test_file_path.read_text()
        return any(re.search(pattern, content, re.IGNORECASE)
                   for pattern in mysql_patterns)
    except Exception:
        # If we can't read the file, assume it's not MySQL-specific
        return False


def initialize_work_queue(repo_root: Path, work_queue_dir: Path) -> int:
    """
    Initialize work queue by creating work items for all test files.

    Filters out only blocklisted files (memory leaks, OOM issues).
    MySQL-specific tests are included and tagged for separate reporting.

    Returns:
        Number of test files added to the queue
    """
    # Clean up old work queue
    import shutil
    if work_queue_dir.exists():
        shutil.rmtree(work_queue_dir)

    # Create queue directories
    pending_dir = work_queue_dir / "pending"
    claimed_dir = work_queue_dir / "claimed"
    completed_dir = work_queue_dir / "completed"

    pending_dir.mkdir(parents=True, exist_ok=True)
    claimed_dir.mkdir(parents=True, exist_ok=True)
    completed_dir.mkdir(parents=True, exist_ok=True)

    # Blocklist of test files that cause memory leaks or OOM
    # select5.test: Still has memory leak - grows from 6.5GB to 94GB+ over time
    blocklist = {"select5.test"}

    # Find all test files
    test_dir = repo_root / "third_party" / "sqllogictest" / "test"
    all_test_files = sorted(glob.glob(str(test_dir / "**" / "*.test"), recursive=True))

    # Filter out only blocklisted files (MySQL tests are included)
    test_files = []
    skipped_count = 0
    for test_file_path in all_test_files:
        test_file = Path(test_file_path)
        if test_file.name in blocklist:
            skipped_count += 1
            continue
        test_files.append(test_file_path)

    print(f"Initializing work queue with {len(test_files)} test files (skipped {skipped_count})...")

    # Create work items
    for counter, test_file_path in enumerate(test_files, start=1):
        # Convert absolute path to relative path from test_dir
        test_file = Path(test_file_path)
        rel_path = test_file.relative_to(test_dir)

        # Encode work item (same format as Rust: {counter:04}-{sanitized_path})
        # Use __ (double underscore) as separator - safe on all filesystems
        sanitized = str(rel_path).replace("/", "__").replace("\\", "__")
        work_item_name = f"{counter:04}-{sanitized}"

        # Create work item file in pending queue
        work_item_path = pending_dir / work_item_name
        work_item_path.touch()

    print(f"✓ Work queue initialized: {len(test_files)} files in pending queue")
    return len(test_files)


def run_worker(
    worker_id: int,
    total_workers: int,
    seed: int,
    time_budget: int,
    repo_root: Path,
    results_dir: Path,
    work_queue_dir: Optional[Path] = None,
    test_binary: Optional[Path] = None,
) -> int:
    """
    Run a single test worker.

    Args:
        worker_id: Worker ID (1-indexed)
        total_workers: Total number of parallel workers
        seed: Shared random seed for deterministic test selection
        time_budget: Maximum time in seconds for this worker
        repo_root: Path to repository root
        results_dir: Directory to store worker results
        work_queue_dir: Optional work queue directory (enables work queue mode)
        test_binary: Optional path to pre-compiled test binary (if not provided, uses cargo test)

    Returns:
        Exit code (0 = success, non-zero = failure)
    """
    log_file = results_dir / f"worker_{worker_id}.log"
    analysis_file = results_dir / f"worker_{worker_id}_analysis.json"

    env = os.environ.copy()
    env.update({
        "SQLLOGICTEST_SEED": str(seed),
        "SQLLOGICTEST_WORKER_ID": str(worker_id),
        "SQLLOGICTEST_TOTAL_WORKERS": str(total_workers),
        "SQLLOGICTEST_TIME_BUDGET": str(time_budget),
    })

    # Enable work queue mode if work_queue_dir is provided
    if work_queue_dir:
        env["SQLLOGICTEST_USE_WORK_QUEUE"] = "1"
        env["SQLLOGICTEST_WORK_QUEUE"] = str(work_queue_dir)

    print(f"Starting worker {worker_id}/{total_workers}...", flush=True)

    try:
        with open(log_file, "w") as log:
            # Use pre-compiled binary if available, otherwise fall back to cargo test
            if test_binary and test_binary.exists():
                cmd = [str(test_binary), "run_sqllogictest_suite", "--nocapture"]
            else:
                cmd = [
                    "cargo",
                    "test",
                    "--test",
                    "sqllogictest_suite",
                    "--release",
                    "run_sqllogictest_suite",
                    "--",
                    "--nocapture",
                ]

            result = subprocess.run(
                cmd,
                cwd=repo_root,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )

        # Copy analysis results if they exist (worker writes to worker-specific file)
        target_analysis = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
        if target_analysis.exists():
            import shutil
            shutil.copy(target_analysis, analysis_file)
        else:
            print(f"Warning: Worker {worker_id} did not produce results file: {target_analysis}", flush=True)

        return result.returncode

    except Exception as e:
        print(f"Worker {worker_id} failed with error: {e}", file=sys.stderr, flush=True)
        return 1


def merge_results(
    total_workers: int,
    repo_root: Path,
    results_dir: Path,
) -> bool:
    """
    Merge results from all workers into a cumulative result.

    Args:
        total_workers: Total number of workers
        repo_root: Path to repository root
        results_dir: Directory containing worker results

    Returns:
        True if merge succeeded, False otherwise
    """
    print("\nMerging results from all workers...", flush=True)

    merge_script = repo_root / "scripts" / "merge_sqllogictest_results.py"
    if not merge_script.exists():
        print(f"Warning: Merge script not found at {merge_script}", file=sys.stderr)
        return False

    target_dir = repo_root / "target"
    historical_file = target_dir / "sqllogictest_historical.json"
    cumulative_file = target_dir / "sqllogictest_cumulative.json"

    # Initialize historical file if it doesn't exist
    if not historical_file.exists():
        historical_file.write_text("{}")

    # Merge worker results sequentially
    for worker_id in range(1, total_workers + 1):
        analysis_file = results_dir / f"worker_{worker_id}_analysis.json"

        if not analysis_file.exists():
            print(f"Warning: No results found for worker {worker_id}", flush=True)
            continue

        print(f"Merging worker {worker_id}...", flush=True)

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(merge_script),
                    str(analysis_file),
                    str(historical_file),
                    str(target_dir / "sqllogictest_cumulative_new.json"),
                ],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )

            # Update historical file with new cumulative results
            new_cumulative = target_dir / "sqllogictest_cumulative_new.json"
            if new_cumulative.exists():
                import shutil
                shutil.copy(new_cumulative, historical_file)

        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to merge worker {worker_id}: {e}", file=sys.stderr)
            print(f"stdout: {e.stdout}", file=sys.stderr)
            print(f"stderr: {e.stderr}", file=sys.stderr)
            continue

    # Final cumulative results
    if historical_file.exists():
        import shutil
        shutil.copy(historical_file, cumulative_file)

        # Print summary
        try:
            with open(cumulative_file) as f:
                data = json.load(f)
                summary = data.get("summary", {})
                print("\n=== Final Results ===")
                print(json.dumps(summary, indent=2))

                # Print detailed failure summary if available
                detailed_failures = data.get("detailed_failures", [])
                if detailed_failures:
                    print(f"\n=== Detailed Failures ({len(detailed_failures)} files) ===")
                    for failure_info in detailed_failures[:5]:  # Show first 5
                        file_path = failure_info.get("file_path", "unknown")
                        failures = failure_info.get("failures", [])
                        print(f"  {file_path}: {len(failures)} failures")
                        for failure in failures[:2]:  # Show first 2 failures per file
                            error_msg = failure.get("error_message", "")[:100]  # Truncate long messages
                            print(f"    - {error_msg}")
                    if len(detailed_failures) > 5:
                        print(f"  ... and {len(detailed_failures) - 5} more files")
        except Exception as e:
            print(f"Warning: Could not read summary: {e}", file=sys.stderr)

        print(f"\nResults saved to {cumulative_file}")
        return True

    return False


def main():
    parser = argparse.ArgumentParser(
        description="Run SQLLogicTest suite with parallel workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=multiprocessing.cpu_count(),
        help="Number of parallel workers (default: number of CPUs)",
    )
    parser.add_argument(
        "--time-budget",
        type=int,
        default=3600,
        help="Time budget per worker in seconds (default: 3600 = 1 hour)",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="Directory to store worker results (default: /tmp/sqllogictest_results)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.workers < 1:
        print("Error: --workers must be at least 1", file=sys.stderr)
        return 1

    if args.time_budget < 1:
        print("Error: --time-budget must be at least 1 second", file=sys.stderr)
        return 1

    # Setup
    repo_root = get_repo_root()
    results_dir = args.results_dir or Path("/tmp/sqllogictest_results")
    results_dir.mkdir(parents=True, exist_ok=True)

    # Generate shared seed
    seed = int(time.time())

    print("=== Parallel SQLLogicTest Suite Runner ===")
    print(f"Workers: {args.workers}")
    print(f"Time budget per worker: {args.time_budget}s")
    print(f"Shared seed: {seed}")
    print(f"Results directory: {results_dir}")
    print(f"Repository: {repo_root}")
    print()

    # Pre-build test binary once to avoid 64x compilation
    print("Pre-compiling test binary...")
    try:
        result = subprocess.run(
            ["cargo", "test", "--test", "sqllogictest_suite", "--release", "--no-run", "--message-format=json"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )

        # Parse cargo output to find the test binary path
        test_binary = None
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
                if msg.get("reason") == "compiler-artifact" and "sqllogictest_suite" in msg.get("target", {}).get("name", ""):
                    executable = msg.get("executable")
                    if executable:
                        test_binary = Path(executable)
                        break
            except json.JSONDecodeError:
                continue

        if not test_binary:
            # Fallback: find most recent binary in target/release/deps
            deps_dir = repo_root / "target" / "release" / "deps"
            binaries = sorted(deps_dir.glob("sqllogictest_suite-*"), key=lambda p: p.stat().st_mtime, reverse=True)
            binaries = [b for b in binaries if b.is_file() and not b.name.endswith('.d')]
            if binaries:
                test_binary = binaries[0]

        if not test_binary or not test_binary.exists():
            print("Error: Could not find test binary", file=sys.stderr)
            return 1

        print(f"✓ Test binary compiled: {test_binary.name}")
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to compile test binary", file=sys.stderr)
        print(f"stdout: {e.stdout}", file=sys.stderr)
        print(f"stderr: {e.stderr}", file=sys.stderr)
        return 1
    print()

    # Initialize work queue
    work_queue_dir = Path("/tmp/sqllogictest_work_queue")
    total_files = initialize_work_queue(repo_root, work_queue_dir)
    print()

    # Start all workers in parallel
    from multiprocessing import Pool

    start_time = time.time()

    with Pool(processes=args.workers) as pool:
        worker_args = [
            (
                worker_id,
                args.workers,
                seed,
                args.time_budget,
                repo_root,
                results_dir,
                work_queue_dir,  # Pass work queue directory
                test_binary,     # Pass pre-compiled binary
            )
            for worker_id in range(1, args.workers + 1)
        ]

        print(f"All {args.workers} workers started! Waiting for completion...")
        print(f"Monitor progress:")
        print(f"  tail -f {results_dir}/worker_*.log")
        print()

        results = pool.starmap(run_worker, worker_args)

    elapsed = time.time() - start_time

    print()
    print("=== All Workers Complete ===")
    print(f"Total time: {elapsed:.1f}s")
    print()

    # Check for failures
    failed_workers = [
        i + 1 for i, code in enumerate(results) if code != 0
    ]
    if failed_workers:
        print(f"Warning: {len(failed_workers)} workers failed: {failed_workers}")

    # Merge results
    merge_success = merge_results(args.workers, repo_root, results_dir)

    # Exit with error if any workers failed or merge failed
    if failed_workers or not merge_success:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
