#!/usr/bin/env python3
"""
Validate index/between and index/commute tests for Issue #1449
Tests the fix from PR #1442
"""

import subprocess
import sys
from pathlib import Path
import json
from datetime import datetime

# Colors for terminal output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color

def run_test_file(test_file: Path, timeout: int = 120) -> tuple[bool, str]:
    """
    Run a single test file and return (passed, output)
    """
    env = {
        'SQLLOGICTEST_FILES': str(test_file),
        'PATH': subprocess.os.environ.get('PATH', '')
    }

    try:
        result = subprocess.run(
            ['cargo', 'test', '--package', 'vibesql', '--test', 'sqllogictest_suite',
             'run_sqllogictest_file', '--', '--nocapture'],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env
        )

        # Check if test passed
        passed = result.returncode == 0
        return passed, result.stdout + result.stderr

    except subprocess.TimeoutExpired:
        return False, f"Test timed out after {timeout} seconds"
    except Exception as e:
        return False, f"Error running test: {e}"

def main():
    # Find test files
    repo_root = Path.cwd()
    test_base = repo_root / 'third_party' / 'sqllogictest' / 'test' / 'index'

    between_files = sorted((test_base / 'between').rglob('*.test'))
    commute_files = sorted((test_base / 'commute').rglob('*.test'))

    print("=" * 70)
    print("Index Ordering Validation (Issue #1449)")
    print("Testing PR #1442 fix")
    print("=" * 70)
    print()
    print(f"Found {len(between_files)} BETWEEN test files")
    print(f"Found {len(commute_files)} COMMUTE test files")
    print()

    # Sample a few files from each category to test
    # Testing all 65 files would take too long
    between_sample = between_files[:3] if len(between_files) >= 3 else between_files
    commute_sample = commute_files[:5] if len(commute_files) >= 5 else commute_files

    print(f"Testing {len(between_sample)} BETWEEN files (sample)")
    print(f"Testing {len(commute_sample)} COMMUTE files (sample)")
    print()

    results = {
        'between': {'passed': 0, 'failed': 0, 'files': []},
        'commute': {'passed': 0, 'failed': 0, 'files': []}
    }

    # Test BETWEEN files
    print(f"{BLUE}=== Testing BETWEEN Files ==={NC}")
    for test_file in between_sample:
        rel_path = test_file.relative_to(test_base)
        print(f"  {rel_path}... ", end='', flush=True)

        passed, output = run_test_file(test_file)

        if passed:
            print(f"{GREEN}PASS{NC}")
            results['between']['passed'] += 1
        else:
            print(f"{RED}FAIL{NC}")
            results['between']['failed'] += 1

        results['between']['files'].append({
            'file': str(rel_path),
            'passed': passed
        })

    print()

    # Test COMMUTE files
    print(f"{BLUE}=== Testing COMMUTE Files ==={NC}")
    for test_file in commute_sample:
        rel_path = test_file.relative_to(test_base)
        print(f"  {rel_path}... ", end='', flush=True)

        passed, output = run_test_file(test_file)

        if passed:
            print(f"{GREEN}PASS{NC}")
            results['commute']['passed'] += 1
        else:
            print(f"{RED}FAIL{NC}")
            results['commute']['failed'] += 1

        results['commute']['files'].append({
            'file': str(rel_path),
            'passed': passed
        })

    print()

    # Summary
    total_passed = results['between']['passed'] + results['commute']['passed']
    total_failed = results['between']['failed'] + results['commute']['failed']
    total_tests = total_passed + total_failed
    pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print(f"BETWEEN Tests (sample of {len(between_sample)} / {len(between_files)} total):")
    print(f"  Passed: {results['between']['passed']}")
    print(f"  Failed: {results['between']['failed']}")
    print()
    print(f"COMMUTE Tests (sample of {len(commute_sample)} / {len(commute_files)} total):")
    print(f"  Passed: {results['commute']['passed']}")
    print(f"  Failed: {results['commute']['failed']}")
    print()
    print(f"TOTAL (sampled):")
    print(f"  Passed: {total_passed} / {total_tests}")
    print(f"  Failed: {total_failed} / {total_tests}")
    print(f"  Pass Rate: {pass_rate:.1f}%")
    print()

    # Save results to JSON
    results['summary'] = {
        'total_between_files': len(between_files),
        'total_commute_files': len(commute_files),
        'sampled_between': len(between_sample),
        'sampled_commute': len(commute_sample),
        'total_passed': total_passed,
        'total_failed': total_failed,
        'pass_rate': pass_rate,
        'timestamp': datetime.now().isoformat()
    }

    results_file = repo_root / 'index_validation_results.json'
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Results saved to: {results_file}")
    print()

    # Create markdown summary
    md_file = repo_root / 'INDEX_VALIDATION.md'
    with open(md_file, 'w') as f:
        f.write(f"# Index Ordering Validation Results\\n\\n")
        f.write(f"**Issue**: #1449\\n")
        f.write(f"**PR Tested**: #1442 (fix: Preserve index ordering in range_scan and multi_lookup)\\n")
        f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\n")
        f.write(f"\\n")
        f.write(f"## Summary\\n\\n")
        f.write(f"| Category | Sample | Total Files | Passed | Failed | Pass Rate |\\n")
        f.write(f"|----------|--------|-------------|--------|--------|-----------|\\n")

        between_pass_rate = (results['between']['passed'] / len(between_sample) * 100) if len(between_sample) > 0 else 0
        f.write(f"| BETWEEN  | {len(between_sample)} | {len(between_files)} | {results['between']['passed']} | {results['between']['failed']} | {between_pass_rate:.1f}% |\\n")

        commute_pass_rate = (results['commute']['passed'] / len(commute_sample) * 100) if len(commute_sample) > 0 else 0
        f.write(f"| COMMUTE  | {len(commute_sample)} | {len(commute_files)} | {results['commute']['passed']} | {results['commute']['failed']} | {commute_pass_rate:.1f}% |\\n")

        f.write(f"| **TOTAL** | **{total_tests}** | **{len(between_files) + len(commute_files)}** | **{total_passed}** | **{total_failed}** | **{pass_rate:.1f}%** |\\n")
        f.write(f"\\n")
        f.write(f"## Test Results\\n\\n")
        f.write(f"### BETWEEN Tests\\n\\n")

        for file_result in results['between']['files']:
            status = "✅" if file_result['passed'] else "❌"
            f.write(f"- {status} `{file_result['file']}`\\n")

        f.write(f"\\n### COMMUTE Tests\\n\\n")

        for file_result in results['commute']['files']:
            status = "✅" if file_result['passed'] else "❌"
            f.write(f"- {status} `{file_result['file']}`\\n")

        f.write(f"\\n## Notes\\n\\n")
        f.write(f"- This validation tested a sample of files from each category\\n")
        f.write(f"- Total available: {len(between_files)} BETWEEN files, {len(commute_files)} COMMUTE files\\n")
        f.write(f"- PR #1442 fixed index ordering in `range_scan()` and `multi_lookup()` methods\\n")

    print(f"Markdown summary saved to: {md_file}")
    print()

    # Exit code
    if total_failed == 0:
        print(f"{GREEN}✓ All sampled tests passed!{NC}")
        return 0
    else:
        print(f"{YELLOW}⚠ Some tests failed{NC}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
