#!/usr/bin/env python3
"""
Analyze SQLLogicTest output and generate useful reports.

This script parses the output from cargo test sqllogictest_suite and:
- Categorizes errors by type (parser errors, missing features, etc.)
- Counts occurrences of each error type
- Generates actionable summary reports
- Tracks progress over time
"""

import sys
import re
import json
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Tuple


class SQLLogicTestAnalyzer:
    def __init__(self):
        self.passed = []
        self.failed = []
        self.error_categories = defaultdict(list)
        self.parse_errors = Counter()
        self.sqllogictest_syntax_errors = Counter()
        self.missing_features = Counter()

    def parse_line(self, line: str) -> None:
        """Parse a single line of test output."""
        line = line.strip()

        # Match passed tests
        if line.startswith("✓ "):
            test_file = line[2:].strip()
            self.passed.append(test_file)

        # Match failed tests with parse errors
        elif line.startswith("✗ ") and "Parse error:" in line:
            match = re.match(r'✗ (.+?) - statement failed: Parse error: ParseError \{ message: "(.+?)" \}', line)
            if match:
                test_file, error_msg = match.groups()
                self.failed.append(test_file)
                self.categorize_parse_error(test_file, error_msg)

        # Match failed tests with SQLLogicTest syntax errors (panics)
        elif line.startswith("✗ ") and "Test panicked" in line:
            match = re.match(r'✗ (.+?) - Test panicked \(likely unsupported SQLLogicTest syntax\)', line)
            if match:
                test_file = match.group(1)
                self.failed.append(test_file)
                self.error_categories['sqllogictest_syntax'].append(test_file)

        # Match panic details for SQLLogicTest syntax errors
        elif "InvalidLine" in line:
            match = re.search(r'InvalidLine\("(.+?)"\)', line)
            if match:
                invalid_line = match.group(1)
                self.sqllogictest_syntax_errors[invalid_line] += 1

    def categorize_parse_error(self, test_file: str, error_msg: str) -> None:
        """Categorize a parse error into meaningful buckets."""
        error_lower = error_msg.lower()

        # Missing data types
        if "unknown data type:" in error_lower:
            data_type = re.search(r'unknown data type: (\w+)', error_lower, re.IGNORECASE)
            if data_type:
                feature = f"Data type: {data_type.group(1)}"
                self.missing_features[feature] += 1
                self.error_categories['missing_data_types'].append(test_file)

        # Missing SQL features (CREATE INDEX, CREATE VIEW, etc.)
        elif "expected table, schema, role" in error_lower:
            # This usually means CREATE INDEX, CREATE VIEW, CREATE TRIGGER, etc.
            self.missing_features["CREATE INDEX/VIEW/TRIGGER"] += 1
            self.error_categories['missing_ddl_statements'].append(test_file)

        # Lexer errors (backticks, special characters)
        elif "lexer error" in error_lower:
            if "unexpected character: '`'" in error_lower:
                self.missing_features["Backtick identifiers"] += 1
                self.error_categories['lexer_errors'].append(test_file)
            else:
                self.parse_errors[error_msg] += 1
                self.error_categories['lexer_errors'].append(test_file)

        # Other parse errors
        else:
            self.parse_errors[error_msg] += 1
            self.error_categories['other_parse_errors'].append(test_file)

    def generate_report(self) -> str:
        """Generate a human-readable report."""
        total = len(self.passed) + len(self.failed)
        pass_rate = (len(self.passed) / total * 100) if total > 0 else 0

        report = []
        report.append("=" * 80)
        report.append("SQLLogicTest Analysis Report")
        report.append("=" * 80)
        report.append("")

        # Overall statistics
        report.append(f"Total test files: {total}")
        report.append(f"Passed: {len(self.passed)} ({pass_rate:.1f}%)")
        report.append(f"Failed: {len(self.failed)} ({100-pass_rate:.1f}%)")
        report.append("")

        # Error categories
        report.append("Error Categories:")
        report.append("-" * 80)
        for category, tests in sorted(self.error_categories.items(),
                                     key=lambda x: len(x[1]), reverse=True):
            report.append(f"  {category:40} {len(tests):5} files")
        report.append("")

        # Missing features (sorted by frequency)
        if self.missing_features:
            report.append("Missing Features (Top 10):")
            report.append("-" * 80)
            for feature, count in self.missing_features.most_common(10):
                report.append(f"  {feature:50} {count:5} occurrences")
            report.append("")

        # SQLLogicTest syntax errors
        if self.sqllogictest_syntax_errors:
            report.append("Unsupported SQLLogicTest Syntax (Top 10):")
            report.append("-" * 80)
            for syntax, count in self.sqllogictest_syntax_errors.most_common(10):
                # Truncate long lines
                syntax_short = syntax[:70] + "..." if len(syntax) > 70 else syntax
                report.append(f"  {syntax_short:70} {count:5} occurrences")
            report.append("")

        # Other parse errors
        if self.parse_errors:
            report.append("Other Parse Errors (Top 10):")
            report.append("-" * 80)
            for error, count in self.parse_errors.most_common(10):
                error_short = error[:70] + "..." if len(error) > 70 else error
                report.append(f"  {error_short:70} {count:5} occurrences")
            report.append("")

        report.append("=" * 80)
        return "\n".join(report)

    def generate_json_summary(self) -> Dict:
        """Generate a JSON summary for programmatic use."""
        total = len(self.passed) + len(self.failed)
        pass_rate = (len(self.passed) / total * 100) if total > 0 else 0

        return {
            "summary": {
                "total_files": total,
                "passed": len(self.passed),
                "failed": len(self.failed),
                "pass_rate": round(pass_rate, 2)
            },
            "error_categories": {
                category: len(tests)
                for category, tests in self.error_categories.items()
            },
            "missing_features": dict(self.missing_features.most_common()),
            "sqllogictest_syntax_errors": dict(self.sqllogictest_syntax_errors.most_common(20)),
            "top_parse_errors": dict(self.parse_errors.most_common(20)),
        }

    def generate_markdown_report(self) -> str:
        """Generate a markdown report for documentation."""
        total = len(self.passed) + len(self.failed)
        pass_rate = (len(self.passed) / total * 100) if total > 0 else 0

        report = []
        report.append("# SQLLogicTest Analysis Report")
        report.append("")
        report.append("## Summary")
        report.append("")
        report.append(f"- **Total test files**: {total}")
        report.append(f"- **Passed**: {len(self.passed)} ({pass_rate:.1f}%)")
        report.append(f"- **Failed**: {len(self.failed)} ({100-pass_rate:.1f}%)")
        report.append("")

        # Error categories table
        report.append("## Error Categories")
        report.append("")
        report.append("| Category | Count |")
        report.append("|----------|------:|")
        for category, tests in sorted(self.error_categories.items(),
                                     key=lambda x: len(x[1]), reverse=True):
            report.append(f"| {category} | {len(tests)} |")
        report.append("")

        # Missing features
        if self.missing_features:
            report.append("## Missing Features")
            report.append("")
            report.append("These features need to be implemented:")
            report.append("")
            report.append("| Feature | Occurrences |")
            report.append("|---------|------------:|")
            for feature, count in self.missing_features.most_common(20):
                report.append(f"| {feature} | {count} |")
            report.append("")

        # SQLLogicTest syntax
        if self.sqllogictest_syntax_errors:
            report.append("## Unsupported SQLLogicTest Syntax")
            report.append("")
            report.append("These SQLLogicTest directives are not supported by the library:")
            report.append("")
            report.append("| Directive | Occurrences |")
            report.append("|-----------|------------:|")
            for syntax, count in self.sqllogictest_syntax_errors.most_common(10):
                # Escape pipe characters for markdown
                syntax_escaped = syntax.replace("|", "\\|")
                report.append(f"| `{syntax_escaped}` | {count} |")
            report.append("")

        return "\n".join(report)


def main():
    """Main entry point."""
    analyzer = SQLLogicTestAnalyzer()

    # Read from stdin or file
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            for line in f:
                analyzer.parse_line(line)
    else:
        for line in sys.stdin:
            analyzer.parse_line(line)

    # Generate reports
    print(analyzer.generate_report())

    # Save JSON summary
    json_path = Path("target/sqllogictest_analysis.json")
    json_path.parent.mkdir(exist_ok=True)
    with open(json_path, 'w') as f:
        json.dump(analyzer.generate_json_summary(), f, indent=2)
    print(f"\n✓ JSON summary written to {json_path}")

    # Save markdown report
    md_path = Path("target/sqllogictest_analysis.md")
    with open(md_path, 'w') as f:
        f.write(analyzer.generate_markdown_report())
    print(f"✓ Markdown report written to {md_path}")


if __name__ == "__main__":
    main()
