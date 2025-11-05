#!/usr/bin/env python3
"""
Generate trend visualizations from SQLLogicTest historical results.

This script analyzes historical test results and generates trend charts showing
pass rate improvements over time and per-category progress.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List
try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def load_historical_results(history_dir: Path) -> List[Dict]:
    """Load all historical result files."""
    results = []

    if not history_dir.exists():
        return results

    for json_file in sorted(history_dir.glob("sqllogictest_results_*.json")):
        try:
            with open(json_file) as f:
                data = json.load(f)
                # Add filename for debugging
                data["_filename"] = json_file.name
                results.append(data)
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}", file=sys.stderr)

    # Sort by timestamp
    results.sort(key=lambda x: x.get("timestamp", ""))
    return results


def generate_trend_data(results: List[Dict]) -> Dict:
    """Generate trend data from historical results."""
    if not results:
        return {}

    timestamps = []
    pass_rates = []
    coverage_rates = []
    total_tested = []
    categories_trends = {
        "select": {"pass_rates": [], "timestamps": []},
        "evidence": {"pass_rates": [], "timestamps": []},
        "index": {"pass_rates": [], "timestamps": []},
        "random": {"pass_rates": [], "timestamps": []},
        "ddl": {"pass_rates": [], "timestamps": []},
        "other": {"pass_rates": [], "timestamps": []},
    }

    for result in results:
        timestamp = result.get("timestamp", "")
        if not timestamp:
            continue

        try:
            # Parse timestamp (handle various formats)
            if "T" in timestamp:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        except:
            # Fallback: just use index as x-axis
            dt = datetime.now()

        summary = result.get("summary", {})
        categories = result.get("categories", {})

        timestamps.append(dt)
        pass_rates.append(summary.get("pass_rate", 0))
        coverage_rates.append(summary.get("coverage_rate", 0))
        total_tested.append(summary.get("total_tested_files", 0))

        # Category trends
        for cat_name in categories_trends.keys():
            cat_data = categories.get(cat_name)
            if cat_data and cat_data.get("total", 0) > 0:
                cat_pass_rate = cat_data.get("passed", 0) / cat_data.get("total", 1) * 100
                categories_trends[cat_name]["pass_rates"].append(cat_pass_rate)
                categories_trends[cat_name]["timestamps"].append(dt)

    return {
        "timestamps": timestamps,
        "pass_rates": pass_rates,
        "coverage_rates": coverage_rates,
        "total_tested": total_tested,
        "categories": categories_trends,
    }


def generate_trend_chart(trend_data: Dict, output_dir: Path):
    """Generate trend visualization charts."""
    if not HAS_MATPLOTLIB:
        print("Matplotlib not available, skipping chart generation")
        return

    if not trend_data.get("timestamps"):
        print("No historical data available for trend generation")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # Overall pass rate trend
    plt.figure(figsize=(12, 8))

    plt.subplot(2, 2, 1)
    plt.plot(trend_data["timestamps"], trend_data["pass_rates"], 'b-o', linewidth=2, markersize=4)
    plt.title("Overall Pass Rate Trend")
    plt.xlabel("Date")
    plt.ylabel("Pass Rate (%)")
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 100)

    # Coverage trend
    plt.subplot(2, 2, 2)
    plt.plot(trend_data["timestamps"], trend_data["coverage_rates"], 'g-s', linewidth=2, markersize=4)
    plt.title("Test Coverage Trend")
    plt.xlabel("Date")
    plt.ylabel("Coverage (%)")
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 100)

    # Files tested trend
    plt.subplot(2, 2, 3)
    plt.plot(trend_data["timestamps"], trend_data["total_tested"], 'r-^', linewidth=2, markersize=4)
    plt.title("Files Tested Over Time")
    plt.xlabel("Date")
    plt.ylabel("Total Files Tested")
    plt.grid(True, alpha=0.3)

    # Category trends
    plt.subplot(2, 2, 4)
    colors = ['blue', 'green', 'red', 'orange', 'purple', 'brown']
    for i, (cat_name, cat_data) in enumerate(trend_data["categories"].items()):
        if cat_data["pass_rates"]:
            plt.plot(cat_data["timestamps"], cat_data["pass_rates"],
                    color=colors[i % len(colors)], marker='o', linewidth=2,
                    label=cat_name.title(), markersize=3)

    plt.title("Category Pass Rate Trends")
    plt.xlabel("Date")
    plt.ylabel("Pass Rate (%)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 100)

    plt.tight_layout()
    plt.savefig(output_dir / "sqllogictest_trends.png", dpi=150, bbox_inches='tight')
    plt.close()

    print(f"✓ Trend chart saved to {output_dir / 'sqllogictest_trends.png'}")


def generate_trend_report(trend_data: Dict, output_dir: Path):
    """Generate a text report of trends."""
    if not trend_data.get("timestamps"):
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / "sqllogictest_trends.md"

    with open(report_file, 'w') as f:
        f.write("# SQLLogicTest Trends Report\n\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        if trend_data["pass_rates"]:
            latest_pass_rate = trend_data["pass_rates"][-1]
            first_pass_rate = trend_data["pass_rates"][0]
            improvement = latest_pass_rate - first_pass_rate

            f.write("## Overall Progress\n\n")
            f.write(f"- **Current Pass Rate**: {latest_pass_rate:.1f}%\n")
            f.write(f"- **Initial Pass Rate**: {first_pass_rate:.1f}%\n")
            f.write(f"- **Total Improvement**: {improvement:+.1f} percentage points\n")
            f.write(f"- **Data Points**: {len(trend_data['pass_rates'])}\n\n")

        f.write("## Category Progress\n\n")
        f.write("| Category | Latest Pass Rate | Trend |\n")
        f.write("|----------|------------------|-------|\n")

        for cat_name, cat_data in trend_data["categories"].items():
            if cat_data["pass_rates"]:
                latest = cat_data["pass_rates"][-1]
                if len(cat_data["pass_rates"]) > 1:
                    first = cat_data["pass_rates"][0]
                    trend = "↗️ Improving" if latest > first else "↘️ Declining" if latest < first else "➡️ Stable"
                else:
                    trend = "📊 New"
                f.write(f"| {cat_name.title()} | {latest:.1f}% | {trend} |\n")

        f.write("\n## Recent Changes\n\n")
        # Show last 5 data points
        recent_indices = range(max(0, len(trend_data["timestamps"]) - 5), len(trend_data["timestamps"]))
        for i in recent_indices:
            ts = trend_data["timestamps"][i].strftime("%Y-%m-%d %H:%M")
            pass_rate = trend_data["pass_rates"][i]
            coverage = trend_data["coverage_rates"][i]
            tested = trend_data["total_tested"][i]
            f.write(f"- **{ts}**: {pass_rate:.1f}% pass rate, {coverage:.1f}% coverage, {tested} files tested\n")

    print(f"✓ Trend report saved to {report_file}")


def main():
    """Main entry point."""
    history_dir = Path("badges/history")
    output_dir = Path("badges")

    print("Generating SQLLogicTest trend analysis...")

    # Load historical data
    results = load_historical_results(history_dir)
    if not results:
        print("No historical data found. Run some tests first to generate trend data.")
        return 1

    print(f"Loaded {len(results)} historical result files")

    # Generate trend data
    trend_data = generate_trend_data(results)

    # Generate visualizations and reports
    try:
        generate_trend_chart(trend_data, output_dir)
    except Exception as e:
        print(f"Warning: Failed to generate trend chart: {e}")

    generate_trend_report(trend_data, output_dir)

    print("✓ Trend analysis complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
