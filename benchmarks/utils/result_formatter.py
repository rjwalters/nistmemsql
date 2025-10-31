"""
Result formatting and reporting utilities for benchmark results.

Handles JSON output, comparison calculations, and report generation.
"""
import json
import statistics
from datetime import datetime
from typing import Dict, List, Any


class BenchmarkResult:
    """Container for benchmark result data."""

    def __init__(self, name: str, nistmemsql_time: float = None, sqlite_time: float = None):
        self.name = name
        self.nistmemsql_time = nistmemsql_time
        self.sqlite_time = sqlite_time
        self.ratio = None

        if nistmemsql_time and sqlite_time and sqlite_time > 0:
            self.ratio = nistmemsql_time / sqlite_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'name': self.name,
            'nistmemsql_ms': self.nistmemsql_time,
            'sqlite_ms': self.sqlite_time,
            'ratio': self.ratio
        }


class BenchmarkResults:
    """Collection of benchmark results with analysis."""

    def __init__(self):
        self.timestamp = datetime.now().isoformat()
        self.hardware_info = self._get_hardware_info()
        self.results: List[BenchmarkResult] = []

    def add_result(self, result: BenchmarkResult):
        """Add a benchmark result."""
        self.results.append(result)

    def get_summary_stats(self) -> Dict[str, Any]:
        """Calculate summary statistics across all results."""
        if not self.results:
            return {}

        ratios = [r.ratio for r in self.results if r.ratio is not None]
        if not ratios:
            return {}

        return {
            'geometric_mean_ratio': statistics.geometric_mean(ratios),
            'median_ratio': statistics.median(ratios),
            'mean_ratio': statistics.mean(ratios),
            'range': [min(ratios), max(ratios)],
            'count': len(ratios)
        }

    def to_json(self, indent: int = 2) -> str:
        """Export results to JSON format."""
        data = {
            'timestamp': self.timestamp,
            'hardware': self.hardware_info,
            'benchmarks': [r.to_dict() for r in self.results],
            'summary': self.get_summary_stats()
        }
        return json.dumps(data, indent=indent)

    def save_json(self, filepath: str):
        """Save results to JSON file."""
        with open(filepath, 'w') as f:
            f.write(self.to_json())

    def _get_hardware_info(self) -> Dict[str, str]:
        """Get basic hardware information."""
        try:
            import platform
            import psutil

            return {
                'cpu': platform.processor() or 'Unknown',
                'memory_gb': str(round(psutil.virtual_memory().total / (1024**3), 1)),
                'os': f"{platform.system()} {platform.release()}",
                'python': platform.python_version()
            }
        except ImportError:
            return {
                'cpu': 'Unknown',
                'memory_gb': 'Unknown',
                'os': 'Unknown',
                'python': platform.python_version()
            }


def format_performance_report(results: BenchmarkResults, title: str = "Performance Report") -> str:
    """Format results as a human-readable performance report."""
    lines = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"**Generated**: {results.timestamp}")
    lines.append("")
    lines.append("## Hardware Configuration")
    lines.append("")
    hw = results.hardware_info
    lines.append(f"- **CPU**: {hw['cpu']}")
    lines.append(f"- **Memory**: {hw['memory_gb']} GB")
    lines.append(f"- **OS**: {hw['os']}")
    lines.append(f"- **Python**: {hw['python']}")
    lines.append("")

    if results.results:
        lines.append("## Benchmark Results")
        lines.append("")
        lines.append("| Benchmark | nistmemsql (ms) | SQLite (ms) | Ratio |")
        lines.append("|-----------|-----------------|-------------|-------|")

        for result in sorted(results.results, key=lambda r: r.name):
            nist = ".2f" if result.nistmemsql_time else "N/A"
            sqlite = ".2f" if result.sqlite_time else "N/A"
            ratio = ".2f" if result.ratio else "N/A"
            lines.append(f"| {result.name} | {nist} | {sqlite} | {ratio}x |")

        lines.append("")

        summary = results.get_summary_stats()
        if summary:
            lines.append("## Summary Statistics")
            lines.append("")
            lines.append(".2f")
            lines.append(".2f")
            lines.append(".2f")
            lines.append(f"- **Range**: {summary['range'][0]:.2f}x - {summary['range'][1]:.2f}x")
            lines.append(f"- **Test Count**: {summary['count']}")

    return "\n".join(lines)


def compare_results(baseline_results: BenchmarkResults, new_results: BenchmarkResults) -> str:
    """Compare two sets of results and generate a comparison report."""
    lines = []
    lines.append("# Performance Comparison Report")
    lines.append("")
    lines.append(f"**Baseline**: {baseline_results.timestamp}")
    lines.append(f"**Current**: {new_results.timestamp}")
    lines.append("")

    # Create lookup dictionaries
    baseline_dict = {r.name: r for r in baseline_results.results}
    new_dict = {r.name: r for r in new_results.results}

    # Find common benchmarks
    common_names = set(baseline_dict.keys()) & set(new_dict.keys())

    if not common_names:
        lines.append("No common benchmarks found for comparison.")
        return "\n".join(lines)

    lines.append("## Benchmark Comparison")
    lines.append("")
    lines.append("| Benchmark | Baseline Ratio | Current Ratio | Change |")
    lines.append("|-----------|----------------|---------------|--------|")

    changes = []
    for name in sorted(common_names):
        baseline = baseline_dict[name]
        current = new_dict[name]

        if baseline.ratio and current.ratio:
            change = ((current.ratio - baseline.ratio) / baseline.ratio) * 100
            change_str = ".1f"
            changes.append(change)
        else:
            change_str = "N/A"

        lines.append(f"| {name} | {baseline.ratio:.2f}x | {current.ratio:.2f}x | {change_str} |")

    lines.append("")

    if changes:
        avg_change = statistics.mean(changes)
        lines.append("## Overall Change")
        lines.append("")
        lines.append(".1f")

        baseline_summary = baseline_results.get_summary_stats()
        new_summary = new_results.get_summary_stats()

        if baseline_summary and new_summary:
            geom_change = ((new_summary['geometric_mean_ratio'] - baseline_summary['geometric_mean_ratio'])
                          / baseline_summary['geometric_mean_ratio']) * 100
            lines.append(".1f")

    return "\n".join(lines)


def load_results_from_json(filepath: str) -> BenchmarkResults:
    """Load benchmark results from JSON file."""
    with open(filepath, 'r') as f:
        data = json.load(f)

    results = BenchmarkResults()
    results.timestamp = data['timestamp']
    results.hardware_info = data['hardware']

    for benchmark_data in data['benchmarks']:
        result = BenchmarkResult(
            name=benchmark_data['name'],
            nistmemsql_time=benchmark_data.get('nistmemsql_ms'),
            sqlite_time=benchmark_data.get('sqlite_ms')
        )
        results.add_result(result)

    return results
