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
import json
import multiprocessing
import os
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


def run_worker(
    worker_id: int,
    total_workers: int,
    seed: int,
    time_budget: int,
    repo_root: Path,
    results_dir: Path,
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

    print(f"Starting worker {worker_id}/{total_workers}...", flush=True)

    try:
        with open(log_file, "w") as log:
            result = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--test",
                    "sqllogictest_suite",
                    "--release",
                    "run_sqllogictest_suite",
                    "--",
                    "--nocapture",
                ],
                cwd=repo_root,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )

        # Copy analysis results if they exist
        target_analysis = repo_root / "target" / "sqllogictest_analysis.json"
        if target_analysis.exists():
            import shutil
            shutil.copy(target_analysis, analysis_file)

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
