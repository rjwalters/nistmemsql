#!/usr/bin/env python3
"""
Analyze failure patterns in SQLLogicTest results to identify high-impact fixes.

This script processes the detailed analysis file to:
1. Group failures by error type/pattern
2. Calculate impact (number of tests affected)
3. Identify common root causes
4. Generate prioritized fix recommendations
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple


def extract_error_pattern(error_msg: str) -> str:
    """
    Extract a normalized error pattern from an error message.

    Examples:
        "Result Mismatch: 7.000 vs 7" -> "Decimal formatting mismatch"
        "ColumnNotFound { column_name: COL0" -> "Case-sensitive column lookup"
        "TypeMismatch { left: Integer(77), op: NOT, right: Null }" -> "NOT NULL type mismatch"
    """
    # Decimal formatting issues
    if "query result mismatch" in error_msg:
        # Check for decimal vs integer formatting
        if re.search(r'\d+\.0+\n\+.*?\d+\n', error_msg):
            return "Decimal formatting (7.000 vs 7)"
        # Check for multi-row vs single-row
        if re.search(r'-\s+\d+.*?\n-\s+\d+.*?\n\+\s+\d+ \d+', error_msg):
            return "Multi-row formatting (separate rows vs single row)"
        # Check for hash mismatches
        if "hashing to" in error_msg:
            return "Result hash mismatch (complex query)"
        return "Result mismatch (other)"

    # Column resolution issues
    if "ColumnNotFound" in error_msg:
        # Check if it's a case sensitivity issue
        if re.search(r'column_name: "[A-Z]+.*available_columns:.*"[a-z]', error_msg):
            return "Case-sensitive column resolution (COL0 vs col0)"
        return "Column not found (table/alias issue)"

    # Type mismatches
    if "TypeMismatch" in error_msg:
        if '"NOT"' in error_msg and "Null" in error_msg:
            return "NOT NULL type mismatch"
        return "Type mismatch (other)"

    # Parse errors
    if "Parse" in error_msg or "ParseError" in error_msg:
        return "Parse error (complex expression)"

    # Unsupported expressions
    if "UnsupportedExpression" in error_msg:
        if "NULLIF" in error_msg:
            return "Missing function: NULLIF"
        if "COALESCE" in error_msg:
            return "Missing function: COALESCE"
        if "AggregateFunction" in error_msg and "Plus" in error_msg:
            return "Unary plus in aggregate (SUM(+col))"
        return "Unsupported expression (other)"

    # Division by zero
    if "DivisionByZero" in error_msg:
        return "Division by zero"

    # Unsupported features
    if "UnsupportedFeature" in error_msg:
        if "subquery" in error_msg:
            return "IN subquery not supported"
        return "Unsupported feature (other)"

    return "Other error"


def analyze_patterns(analysis_file: Path) -> Dict:
    """Analyze failure patterns from detailed analysis file."""

    with open(analysis_file, 'r') as f:
        data = json.load(f)

    # Pattern-based analysis
    patterns = defaultdict(list)
    error_counts = Counter()

    # Analyze each failed file
    for filename, file_data in data.get("files", {}).items():
        if file_data.get("status") == "failed":
            error_msg = file_data.get("error_msg", "")
            error_type = file_data.get("error_type", "")

            # Extract normalized pattern
            pattern = extract_error_pattern(error_msg)
            patterns[pattern].append({
                "file": filename,
                "error_type": error_type,
                "error_msg": error_msg[:200]  # First 200 chars
            })
            error_counts[pattern] += 1

    # Calculate impact and effort estimates
    impact_analysis = []
    for pattern, files in patterns.items():
        count = len(files)

        # Estimate effort (Low/Medium/High)
        if "formatting" in pattern.lower():
            effort = "Low"
            description = "Likely a single output formatting function"
        elif "missing function" in pattern.lower():
            effort = "Medium"
            description = "Need to implement SQL function"
        elif "case-sensitive" in pattern.lower():
            effort = "Medium"
            description = "Update column resolution logic"
        elif "parse error" in pattern.lower():
            effort = "High"
            description = "May require parser updates"
        elif "type mismatch" in pattern.lower():
            effort = "High"
            description = "May require type system changes"
        else:
            effort = "Medium"
            description = "Unknown complexity"

        # Calculate impact/effort ratio
        effort_score = {"Low": 1, "Medium": 2, "High": 3}[effort]
        impact_ratio = count / effort_score

        impact_analysis.append({
            "pattern": pattern,
            "count": count,
            "percentage": round(count / data["summary"]["failed"] * 100, 1),
            "effort": effort,
            "impact_ratio": round(impact_ratio, 1),
            "description": description,
            "examples": files[:3]  # First 3 examples
        })

    # Sort by impact ratio (descending)
    impact_analysis.sort(key=lambda x: x["impact_ratio"], reverse=True)

    return {
        "summary": data.get("summary", {}),
        "patterns": impact_analysis,
        "top_opportunities": impact_analysis[:10]
    }


def print_report(analysis: Dict, output_format: str = "markdown"):
    """Print analysis report in specified format."""

    if output_format == "markdown":
        print("# SQLLogicTest Failure Pattern Analysis")
        print()
        print("## Summary")
        print()
        summary = analysis["summary"]
        print(f"- **Total Files Tested**: {summary.get('total_files', 'N/A')}")
        print(f"- **Passed**: {summary.get('passed', 'N/A')}")
        print(f"- **Failed**: {summary.get('failed', 'N/A')}")
        print(f"- **Pass Rate**: {summary.get('pass_rate', 'N/A')}%")
        print()

        print("## Top 10 High-Impact Fix Opportunities")
        print()
        print("| Rank | Pattern | Tests Affected | % of Failures | Effort | Impact/Effort | Priority |")
        print("|------|---------|----------------|---------------|--------|---------------|----------|")

        for i, item in enumerate(analysis["top_opportunities"], 1):
            priority = "P0" if i <= 3 else ("P1" if i <= 7 else "P2")
            print(f"| {i} | {item['pattern']} | {item['count']} | {item['percentage']}% | {item['effort']} | {item['impact_ratio']} | {priority} |")

        print()
        print("## Detailed Breakdown")
        print()

        for i, item in enumerate(analysis["top_opportunities"], 1):
            print(f"### {i}. {item['pattern']}")
            print()
            print(f"**Impact**: {item['count']} tests ({item['percentage']}% of failures)")
            print(f"**Effort**: {item['effort']}")
            print(f"**Impact/Effort Ratio**: {item['impact_ratio']}")
            print(f"**Description**: {item['description']}")
            print()
            print("**Example failures:**")
            print()
            for example in item["examples"]:
                print(f"- `{example['file']}`")
                print(f"  - Type: `{example['error_type']}`")
                print(f"  - Message: `{example['error_msg'][:150]}...`")
            print()

        print("## Recommended Fix Order")
        print()
        print("Based on impact/effort ratio, prioritize fixes in this order:")
        print()
        for i, item in enumerate(analysis["top_opportunities"][:5], 1):
            print(f"{i}. **{item['pattern']}** - Expected improvement: ~{item['percentage']}% pass rate increase")
        print()

        # Calculate cumulative impact
        cumulative = 0
        print("## Expected Cumulative Impact")
        print()
        print("| After Fix | Pattern | Cumulative Tests Fixed | Expected Pass Rate |")
        print("|-----------|---------|----------------------|-------------------|")

        current_pass_rate = analysis["summary"].get("pass_rate", 13.5)
        total_tests = analysis["summary"].get("total_files", 613)

        for i, item in enumerate(analysis["top_opportunities"][:5], 1):
            cumulative += item["count"]
            new_pass_rate = current_pass_rate + (cumulative / total_tests * 100)
            print(f"| Fix #{i} | {item['pattern'][:40]} | {cumulative} | {new_pass_rate:.1f}% |")

        print()

    elif output_format == "json":
        print(json.dumps(analysis, indent=2))


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_failure_patterns.py <analysis_file> [--format markdown|json]")
        print()
        print("Example:")
        print("  python3 scripts/analyze_failure_patterns.py target/sqllogictest_results_analysis.json")
        print("  python3 scripts/analyze_failure_patterns.py target/sqllogictest_results_analysis.json --format json")
        sys.exit(1)

    analysis_file = Path(sys.argv[1])
    output_format = "markdown"

    # Parse optional format argument
    if len(sys.argv) > 2:
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--format" and i + 1 < len(sys.argv):
                output_format = sys.argv[i + 1]

    if not analysis_file.exists():
        print(f"Error: Analysis file not found: {analysis_file}", file=sys.stderr)
        sys.exit(1)

    # Run analysis
    analysis = analyze_patterns(analysis_file)

    # Print report
    print_report(analysis, output_format)


if __name__ == "__main__":
    main()
