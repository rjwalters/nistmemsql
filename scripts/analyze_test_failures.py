#!/usr/bin/env python3
"""
Analyze SQLLogicTest failures and cluster them by root cause.

This script processes worker logs to identify failure patterns and
group them into actionable categories for fixing.
"""

import re
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Set


class FailureAnalyzer:
    def __init__(self):
        self.failures = []
        self.failure_patterns = Counter()
        self.clusters = defaultdict(list)

    def parse_failure(self, line: str) -> Tuple[str, str, str]:
        """
        Parse a failure line and extract file, error type, and message.

        Returns:
            (test_file, error_type, error_message)
        """
        # Match: ✗ path/to/test.test - error message
        match = re.match(r'✗ ([^\s]+) - (.+)', line)
        if not match:
            return None, None, None

        test_file = match.group(1)
        error_msg = match.group(2).strip()

        # Classify error type
        if "Test panicked" in error_msg:
            error_type = "parse_error"
        elif "statement failed" in error_msg:
            if "Parse error" in error_msg:
                error_type = "sql_parse_error"
            elif "Execution error" in error_msg:
                error_type = "execution_error"
            else:
                error_type = "statement_error"
        elif "query result mismatch" in error_msg:
            error_type = "result_mismatch"
        else:
            error_type = "other"

        return test_file, error_type, error_msg

    def cluster_parse_errors(self):
        """Cluster SQLLogicTest parser errors by directive type."""
        parse_errors = [f for f in self.failures if f[1] == "parse_error"]

        for test_file, _, error_msg in parse_errors:
            # Extract the directive that failed
            if "InvalidLine(" in error_msg:
                match = re.search(r'InvalidLine\("([^"]+)"\)', error_msg)
                if match:
                    directive = match.group(1)

                    # Normalize directives
                    if directive.startswith("onlyif"):
                        pattern = "onlyif directive"
                    elif directive.startswith("skipif"):
                        pattern = "skipif directive"
                    else:
                        pattern = f"unknown directive: {directive[:50]}"

                    self.clusters["parse_errors"].append({
                        "file": test_file,
                        "pattern": pattern,
                        "directive": directive
                    })

    def cluster_execution_errors(self):
        """Cluster execution errors by error message."""
        exec_errors = [f for f in self.failures if f[1] == "execution_error"]

        for test_file, _, error_msg in exec_errors:
            # Extract the specific error
            if "StorageError(" in error_msg:
                match = re.search(r'StorageError\("([^"]+)"\)', error_msg)
                if match:
                    storage_error = match.group(1)
                    self.clusters["storage_errors"].append({
                        "file": test_file,
                        "error": storage_error
                    })
            elif "Not implemented" in error_msg:
                match = re.search(r'Not implemented: ([^"]+)', error_msg)
                if match:
                    feature = match.group(1).strip('"')
                    self.clusters["not_implemented"].append({
                        "file": test_file,
                        "feature": feature
                    })
            else:
                self.clusters["other_execution"].append({
                    "file": test_file,
                    "error": error_msg
                })

    def cluster_parse_errors_sql(self):
        """Cluster SQL parse errors."""
        sql_errors = [f for f in self.failures if f[1] == "sql_parse_error"]

        for test_file, _, error_msg in sql_errors:
            # Extract parse error message
            if 'ParseError { message: "' in error_msg:
                match = re.search(r'ParseError \{ message: "([^"]+)"', error_msg)
                if match:
                    parse_msg = match.group(1)
                    self.clusters["sql_parse_errors"].append({
                        "file": test_file,
                        "error": parse_msg
                    })

    def cluster_result_mismatches(self):
        """Cluster query result mismatches."""
        mismatches = [f for f in self.failures if f[1] == "result_mismatch"]

        for test_file, _, error_msg in mismatches:
            self.clusters["result_mismatches"].append({
                "file": test_file,
                "error": error_msg
            })

    def cluster_mysql_specific_errors(self):
        """Cluster MySQL-specific parse errors (not part of SQL:1999 standard)."""
        sql_errors = [f for f in self.failures if f[1] == "sql_parse_error"]

        mysql_indicators = [
            (r"Unexpected character: '@'", "MySQL system variables (@@)"),
            (r"SET\s+SESSION", "MySQL session management"),
            (r"ONLY_FULL_GROUP_BY", "MySQL sql_mode flags"),
        ]

        for test_file, _, error_msg in sql_errors:
            # Check if this is a MySQL-specific error
            for pattern, description in mysql_indicators:
                if re.search(pattern, error_msg, re.IGNORECASE):
                    self.clusters["mysql_specific_errors"].append({
                        "file": test_file,
                        "error": error_msg,
                        "pattern": description
                    })
                    break  # Don't count the same error twice

    def generate_summary(self) -> Dict:
        """Generate summary statistics."""
        summary = {
            "total_failures": len(self.failures),
            "by_type": Counter(f[1] for f in self.failures),
            "clusters": {}
        }

        for cluster_name, items in self.clusters.items():
            summary["clusters"][cluster_name] = {
                "count": len(items),
                "files": len(set(item["file"] for item in items))
            }

        return summary

    def generate_actionable_report(self) -> str:
        """Generate markdown report with actionable issues."""
        report = []
        report.append("# SQLLogicTest Failure Analysis")
        report.append("")
        report.append("Detailed analysis of test failures with actionable recommendations.")
        report.append("")

        summary = self.generate_summary()

        report.append("## Summary")
        report.append("")
        report.append(f"- **Total failures**: {summary['total_failures']:,}")
        report.append(f"- **Unique test files**: {len(set(f[0] for f in self.failures)):,}")
        report.append("")

        report.append("### Failure Types")
        report.append("")
        for error_type, count in summary['by_type'].most_common():
            pct = (count / summary['total_failures'] * 100)
            report.append(f"- **{error_type}**: {count:,} ({pct:.1f}%)")
        report.append("")

        # Storage errors (highest impact)
        if "storage_errors" in self.clusters:
            report.append("## 🔥 Critical: Storage Layer Issues")
            report.append("")
            storage_by_error = defaultdict(list)
            for item in self.clusters["storage_errors"]:
                storage_by_error[item["error"]].append(item["file"])

            for error, files in sorted(storage_by_error.items(), key=lambda x: len(x[1]), reverse=True):
                report.append(f"### {error}")
                report.append("")
                report.append(f"**Impact**: {len(files):,} test files")
                report.append("")
                report.append("**Recommendation**: Implement this storage feature")
                report.append("")
                report.append("<details>")
                report.append(f"<summary>Affected files ({len(files)})</summary>")
                report.append("")
                for f in sorted(files)[:20]:
                    report.append(f"- {f}")
                if len(files) > 20:
                    report.append(f"- ... and {len(files) - 20} more")
                report.append("")
                report.append("</details>")
                report.append("")

        # Parse errors (test infrastructure)
        if "parse_errors" in self.clusters:
            report.append("## 📝 Test Infrastructure: Parser Issues")
            report.append("")
            report.append("The SQLLogicTest Rust library doesn't support these directives:")
            report.append("")

            by_pattern = defaultdict(set)
            by_directive = defaultdict(set)
            for item in self.clusters["parse_errors"]:
                by_pattern[item["pattern"]].add(item["file"])
                by_directive[item["directive"]].add(item["file"])

            report.append(f"**Total affected files**: {len(set(item['file'] for item in self.clusters['parse_errors'])):,}")
            report.append("")

            report.append("### Top 10 Unsupported Directives")
            report.append("")
            for directive, files in sorted(by_directive.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
                report.append(f"- `{directive}` - {len(files):,} files")
            report.append("")

            report.append("**Recommendation**: Fork `sqllogictest` crate and add MySQL directive support")
            report.append("")

        # MySQL-specific errors (tracked separately for metrics)
        if "mysql_specific_errors" in self.clusters:
            report.append("## 🔍 MySQL-Specific Syntax")
            report.append("")
            report.append("These failures are from MySQL extensions not part of SQL:1999 standard.")
            report.append("**Goal**: Pass these tests by implementing MySQL compatibility.")
            report.append("")

            by_pattern = defaultdict(set)
            for item in self.clusters["mysql_specific_errors"]:
                by_pattern[item["pattern"]].add(item["file"])

            report.append(f"**Total failures**: {len(self.clusters['mysql_specific_errors']):,}")
            report.append(f"**Unique files**: {len(set(item['file'] for item in self.clusters['mysql_specific_errors'])):,}")
            report.append("")

            report.append("### MySQL Patterns Needing Implementation")
            report.append("")
            for pattern, files in sorted(by_pattern.items(), key=lambda x: len(x[1]), reverse=True):
                report.append(f"- **{pattern}**: {len(files):,} files")
            report.append("")

            report.append("**Metrics**: These are reported separately to distinguish SQL:1999 compliance from MySQL dialect support.")
            report.append("")

        # SQL parse errors
        if "sql_parse_errors" in self.clusters:
            report.append("## 🐛 SQL Parser Bugs")
            report.append("")

            by_error = defaultdict(set)
            for item in self.clusters["sql_parse_errors"]:
                by_error[item["error"]].add(item["file"])

            report.append(f"**Total**: {len(self.clusters['sql_parse_errors']):,} failures")
            report.append("")

            for error, files in sorted(by_error.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
                report.append(f"### {error}")
                report.append(f"- **Files affected**: {len(files)}")
                report.append("")

        # Result mismatches (real SQL bugs)
        if "result_mismatches" in self.clusters:
            report.append("## 🎯 Query Result Mismatches (Real SQL Bugs)")
            report.append("")
            report.append(f"**Total**: {len(self.clusters['result_mismatches']):,} failures")
            report.append("")
            report.append("These are actual SQL execution bugs where query results don't match expected values.")
            report.append("")
            report.append("**Recommendation**: Investigate these test files individually")
            report.append("")

            # Sample some files
            sample_files = sorted(set(item["file"] for item in self.clusters["result_mismatches"]))[:20]
            report.append("<details>")
            report.append("<summary>Sample failing files</summary>")
            report.append("")
            for f in sample_files:
                report.append(f"- {f}")
            report.append("")
            report.append("</details>")
            report.append("")

        return "\n".join(report)

    def generate_issue_recommendations(self) -> List[Dict]:
        """Generate recommended GitHub issues to create."""
        issues = []

        # Issue 1: Multi-column indexes
        if "storage_errors" in self.clusters:
            storage_by_error = defaultdict(list)
            for item in self.clusters["storage_errors"]:
                storage_by_error[item["error"]].append(item["file"])

            for error, files in storage_by_error.items():
                if "Multi-column" in error:
                    issues.append({
                        "title": "Implement multi-column index support",
                        "labels": ["enhancement", "storage", "high-priority"],
                        "impact": len(files),
                        "description": f"Multi-column indexes are not yet supported in the storage layer.\n\n**Impact**: {len(files):,} test files failing\n\n**Failing tests**: See SQLLogicTest results"
                    })

        # Issue 2: SQLLogicTest parser
        if "parse_errors" in self.clusters:
            files = set(item["file"] for item in self.clusters["parse_errors"])
            issues.append({
                "title": "Fork sqllogictest crate to support MySQL directives",
                "labels": ["enhancement", "testing", "infrastructure"],
                "impact": len(files),
                "description": f"The sqllogictest Rust library doesn't support MySQL-specific directives (onlyif/skipif).\n\n**Impact**: {len(files):,} test files cannot run\n\n**Solution**: Fork the crate and add directive parsing support"
            })

        # Issue 3: SQL parser bugs
        if "sql_parse_errors" in self.clusters:
            by_error = defaultdict(set)
            for item in self.clusters["sql_parse_errors"]:
                by_error[item["error"]].add(item["file"])

            # Create issues for top SQL parse errors
            for error, files in sorted(by_error.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
                issues.append({
                    "title": f"SQL Parser: {error[:60]}...",
                    "labels": ["bug", "parser"],
                    "impact": len(files),
                    "description": f"SQL parsing fails with: {error}\n\n**Impact**: {len(files)} test files\n\n**Error**: `{error}`"
                })

        return sorted(issues, key=lambda x: x["impact"], reverse=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_test_failures.py <results_directory>")
        print("Example: analyze_test_failures.py /tmp/sqllogictest_results_20251106_070141")
        sys.exit(1)

    results_dir = Path(sys.argv[1])

    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Analyzing failures from: {results_dir}")

    analyzer = FailureAnalyzer()

    # Parse all worker logs
    worker_logs = sorted(results_dir.glob("worker_*.log"))
    print(f"Processing {len(worker_logs)} worker logs...")

    for log_path in worker_logs:
        with open(log_path, 'r') as f:
            for line in f:
                if line.startswith("✗"):
                    test_file, error_type, error_msg = analyzer.parse_failure(line)
                    if test_file:
                        analyzer.failures.append((test_file, error_type, error_msg))

    print(f"✓ Found {len(analyzer.failures):,} total failures")
    print("")

    # Cluster failures
    print("Clustering failures...")
    analyzer.cluster_parse_errors()
    analyzer.cluster_execution_errors()
    analyzer.cluster_parse_errors_sql()
    analyzer.cluster_mysql_specific_errors()
    analyzer.cluster_result_mismatches()
    print("✓ Clustering complete")
    print("")

    # Generate report
    report = analyzer.generate_actionable_report()

    # Save report
    report_path = Path("target/sqllogictest_failure_analysis.md")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"✓ Report written to: {report_path}")
    print("")

    # Generate issue recommendations
    issues = analyzer.generate_issue_recommendations()
    issues_path = Path("target/sqllogictest_recommended_issues.json")
    with open(issues_path, 'w') as f:
        json.dump(issues, f, indent=2)
    print(f"✓ Issue recommendations written to: {issues_path}")
    print("")

    # Print summary
    print("=== Summary ===")
    summary = analyzer.generate_summary()
    print(json.dumps(summary["clusters"], indent=2))


if __name__ == "__main__":
    main()
