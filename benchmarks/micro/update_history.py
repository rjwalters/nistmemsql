#!/usr/bin/env python3
"""
Update benchmark history with latest results.

This script appends the current benchmark results to a historical tracking file,
allowing us to track performance trends over time.
"""
import json
import sys
from datetime import datetime
from pathlib import Path


def load_json(filepath: str) -> dict:
    """Load JSON from file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {filepath}: {e}")
        sys.exit(1)


def update_history(
    current_results_file: str,
    history_file: str,
    commit_sha: str = None,
    max_entries: int = 100
):
    """
    Update benchmark history with current results.

    Args:
        current_results_file: Path to current benchmark results JSON
        history_file: Path to history JSON file
        commit_sha: Git commit SHA for this run
        max_entries: Maximum number of historical entries to keep
    """
    # Load current results
    current_data = load_json(current_results_file)

    if not current_data or 'benchmarks' not in current_data:
        print("❌ Error: No benchmark data found in current results")
        sys.exit(1)

    # Load existing history
    history_path = Path(history_file)
    if history_path.exists() and history_path.stat().st_size > 0:
        try:
            with open(history_file, 'r') as f:
                history = json.load(f)
        except json.JSONDecodeError as e:
            print(f"⚠️  Warning: Invalid JSON in history file, creating new history: {e}")
            history = {
                'version': '1.0',
                'entries': []
            }
    else:
        if history_path.exists():
            print("⚠️  Warning: History file exists but is empty, creating new history")
        history = {
            'version': '1.0',
            'entries': []
        }

    # Extract summary from current results
    benchmarks_summary = []
    for bench in current_data['benchmarks']:
        benchmarks_summary.append({
            'name': bench['name'],
            'mean': bench['stats']['mean'],
            'stddev': bench['stats'].get('stddev', 0),
        })

    # Create new history entry
    entry = {
        'timestamp': current_data.get('datetime', datetime.utcnow().isoformat()),
        'commit': commit_sha or 'unknown',
        'benchmarks': benchmarks_summary
    }

    # Add machine info if available
    if 'machine_info' in current_data:
        entry['machine_info'] = current_data['machine_info']

    # Append to history
    history['entries'].append(entry)

    # Keep only the most recent entries
    if len(history['entries']) > max_entries:
        history['entries'] = history['entries'][-max_entries:]

    # Save updated history
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)

    print(f"✅ Updated benchmark history")
    print(f"   Total entries: {len(history['entries'])}")
    print(f"   History file: {history_file}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python update_history.py <current_results.json> <history.json> [commit_sha]")
        print()
        print("Example: python update_history.py results.json history.json abc123")
        sys.exit(1)

    current_results_file = sys.argv[1]
    history_file = sys.argv[2]
    commit_sha = sys.argv[3] if len(sys.argv) > 3 else None

    update_history(current_results_file, history_file, commit_sha)


if __name__ == '__main__':
    main()
