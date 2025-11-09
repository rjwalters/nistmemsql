#!/usr/bin/env python3
"""
Generate a prioritized punchlist of SQLLogicTest files for 100% conformance.

This script:
1. Scans all test files in third_party/sqllogictest/test/
2. Categorizes them by type (index, random, evidence, select, ddl)
3. Checks existing results to identify passed/failed/untested
4. Generates a CSV punchlist for systematic testing
5. Prioritizes by likelihood of success (passed categories first)
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import csv
from datetime import datetime


def categorize_test_file(file_path: Path, test_dir: Path) -> str:
    """Categorize test file by its directory structure."""
    relative = file_path.relative_to(test_dir)
    parts = relative.parts
    
    if len(parts) == 0:
        return "other"
    
    category = parts[0]
    if category in ["index", "random", "evidence", "select", "ddl"]:
        return category
    
    return "other"


def get_subcategory(file_path: Path, test_dir: Path) -> str:
    """Get subcategory (e.g., commute, between, orderby)."""
    relative = file_path.relative_to(test_dir)
    parts = relative.parts
    
    if len(parts) >= 2:
        return parts[1]
    return "root"


def load_existing_results(results_file: Path, test_dir: Path) -> Tuple[Set[str], Set[str]]:
    """Load passed/failed test files from existing results."""
    passed = set()
    failed = set()
    
    if not results_file.exists():
        return passed, failed
    
    try:
        with open(results_file, 'r') as f:
            data = json.load(f)
            
        if "tested_files" in data:
            for f in data["tested_files"].get("passed", []):
                # Normalize path - handle both absolute and relative paths
                try:
                    rel = Path(f).relative_to(test_dir) if Path(f).is_absolute() else Path(f)
                    passed.add(str(rel))
                except ValueError:
                    # Already relative
                    passed.add(str(Path(f)))
            for f in data["tested_files"].get("failed", []):
                try:
                    rel = Path(f).relative_to(test_dir) if Path(f).is_absolute() else Path(f)
                    failed.add(str(rel))
                except ValueError:
                    failed.add(str(Path(f)))
    except Exception as e:
        print(f"Warning: Could not load results file: {e}", file=sys.stderr)
    
    return passed, failed


def generate_punchlist(test_dir: Path, results_file: Path = None) -> List[Dict]:
    """
    Generate punchlist with all test files organized by status and category.
    """
    # Load existing results
    passed_files, failed_files = load_existing_results(results_file, test_dir) if results_file else (set(), set())
    
    # Scan all test files
    all_files = sorted(test_dir.glob("**/*.test"))
    
    # Organize by category and status
    punchlist = []
    stats = defaultdict(lambda: {"total": 0, "passed": 0, "failed": 0, "untested": 0})
    
    for file_path in all_files:
        # Normalize relative path for comparison
        rel_path = str(file_path.relative_to(test_dir))
        
        category = categorize_test_file(file_path, test_dir)
        subcategory = get_subcategory(file_path, test_dir)
        
        # Determine status
        if rel_path in passed_files:
            status = "PASS"
            priority = 3  # Lower priority, already passing
        elif rel_path in failed_files:
            status = "FAIL"
            priority = 1  # High priority
        else:
            status = "UNTESTED"
            priority = 2  # Medium priority (unknown)
        
        stats[category]["total"] += 1
        if status == "PASS":
            stats[category]["passed"] += 1
        elif status == "FAIL":
            stats[category]["failed"] += 1
        else:
            stats[category]["untested"] += 1
        
        punchlist.append({
            "file": rel_path,
            "category": category,
            "subcategory": subcategory,
            "status": status,
            "priority": priority,  # 1=high (fail), 2=medium (untested), 3=low (pass)
            "full_path": str(file_path),
        })
    
    # Sort by: priority (high first), then category (index first), then file name
    category_order = {"index": 0, "evidence": 1, "select": 2, "random": 3, "ddl": 4, "other": 5}
    punchlist.sort(key=lambda x: (
        -x["priority"],  # High priority first (fail > untested > pass)
        category_order.get(x["category"], 99),
        x["file"]
    ))
    
    return punchlist, stats


def print_summary(punchlist: List[Dict], stats: Dict):
    """Print summary statistics."""
    print("\n" + "="*80)
    print("SQLLogicTest Punchlist Summary")
    print("="*80)
    
    total_files = len(punchlist)
    passed = sum(1 for p in punchlist if p["status"] == "PASS")
    failed = sum(1 for p in punchlist if p["status"] == "FAIL")
    untested = sum(1 for p in punchlist if p["status"] == "UNTESTED")
    
    print(f"\nTotal Files: {total_files}")
    print(f"  ✓ Passed:  {passed} ({100*passed/total_files:.1f}%)")
    print(f"  ✗ Failed:  {failed} ({100*failed/total_files:.1f}%)")
    print(f"  ? Untested: {untested} ({100*untested/total_files:.1f}%)")
    
    print("\nBy Category:")
    print("-" * 60)
    print(f"{'Category':<12} {'Total':>6} {'Passed':>6} {'Failed':>6} {'Untested':>8}")
    print("-" * 60)
    
    for category in ["index", "evidence", "select", "random", "ddl", "other"]:
        if category in stats:
            s = stats[category]
            print(f"{category:<12} {s['total']:>6} {s['passed']:>6} {s['failed']:>6} {s['untested']:>8}")
    
    print("-" * 60)
    total_stats = {
        "total": sum(s["total"] for s in stats.values()),
        "passed": sum(s["passed"] for s in stats.values()),
        "failed": sum(s["failed"] for s in stats.values()),
        "untested": sum(s["untested"] for s in stats.values()),
    }
    print(f"{'TOTAL':<12} {total_stats['total']:>6} {total_stats['passed']:>6} {total_stats['failed']:>6} {total_stats['untested']:>8}")
    print()


def write_punchlist_csv(punchlist: List[Dict], output_file: Path):
    """Write punchlist to CSV file."""
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["file", "category", "subcategory", "status", "priority"])
        writer.writeheader()
        
        # Write with priority sorting
        for item in punchlist:
            writer.writerow({
                "file": item["file"],
                "category": item["category"],
                "subcategory": item["subcategory"],
                "status": item["status"],
                "priority": item["priority"]
            })


def write_punchlist_json(punchlist: List[Dict], output_file: Path, stats: Dict):
    """Write punchlist to JSON file with separate summary and details."""
    # Write compact summary file (for badges and quick status)
    summary_output = {
        "timestamp": datetime.now().isoformat(),
        "total_files": len(punchlist),
        "summary": {
            "passed": sum(1 for p in punchlist if p["status"] == "PASS"),
            "failed": sum(1 for p in punchlist if p["status"] == "FAIL"),
            "untested": sum(1 for p in punchlist if p["status"] == "UNTESTED"),
        },
        "by_category": stats,
    }

    # Write summary file (small, for badges and CI)
    summary_file = output_file.parent / "sqllogictest_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary_output, f, indent=2)

    # Write full details file (only file names by status, no redundant data)
    details_output = {
        "timestamp": datetime.now().isoformat(),
        "passed_files": sorted([p["file"] for p in punchlist if p["status"] == "PASS"]),
        "failed_files": sorted([p["file"] for p in punchlist if p["status"] == "FAIL"]),
        "untested_files": sorted([p["file"] for p in punchlist if p["status"] == "UNTESTED"]),
    }

    with open(output_file, 'w') as f:
        json.dump(details_output, f, indent=2)


def main():
    # Determine paths
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    test_dir = repo_root / "third_party/sqllogictest/test"
    results_file = repo_root / "target/sqllogictest_cumulative.json"
    
    # Alternative results file if cumulative doesn't exist
    if not results_file.exists():
        alt_results = repo_root / "target/sqllogictest_results.json"
        if alt_results.exists():
            results_file = alt_results
        else:
            results_file = None
    
    if not test_dir.exists():
        print(f"Error: Test directory not found: {test_dir}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Scanning test files in: {test_dir}")
    if results_file and results_file.exists():
        print(f"Loading results from: {results_file}")
    else:
        print("No previous results found - all files will be marked as UNTESTED")
    
    # Generate punchlist
    punchlist, stats = generate_punchlist(test_dir, results_file)
    
    # Print summary
    print_summary(punchlist, stats)
    
    # Write output files
    output_dir = repo_root / "target"
    output_dir.mkdir(exist_ok=True)
    
    csv_file = output_dir / "sqllogictest_punchlist.csv"
    json_file = output_dir / "sqllogictest_punchlist.json"
    summary_file = output_dir / "sqllogictest_summary.json"

    write_punchlist_csv(punchlist, csv_file)
    write_punchlist_json(punchlist, json_file, stats)

    print(f"✓ Punchlist written to:")
    print(f"  - {csv_file} (detailed list)")
    print(f"  - {json_file} (file lists by status)")
    print(f"  - {summary_file} (compact summary for badges)")
    
    # Show which tests to focus on
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    
    failing_files = [p for p in punchlist if p["status"] == "FAIL"]
    untested_files = [p for p in punchlist if p["status"] == "UNTESTED"]
    
    if failing_files:
        print(f"\n{len(failing_files)} Failing tests to fix:")
        print("(Sample of first 10)")
        for item in failing_files[:10]:
            print(f"  cargo test --test sqllogictest_suite -- --exact {item['file'].replace('/', '::').replace('.test', '')}")
    
    if untested_files:
        print(f"\n{len(untested_files)} Untested files to validate:")
        print("(Run a few to test your environment)")


if __name__ == "__main__":
    main()
