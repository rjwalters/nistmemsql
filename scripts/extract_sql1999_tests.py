#!/usr/bin/env python3
"""
Extract SQL:1999-compatible tests from sqltest repository.

This script parses the sqltest YAML files and extracts tests into a
JSON manifest that can be consumed by the Rust test runner.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any


def load_yaml_file(file_path: Path) -> List[Dict[str, Any]]:
    """Load and parse a simple YAML file containing test cases."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Split by --- separator
    docs = []
    current_doc = {}
    
    for line in content.split('\n'):
        line = line.strip()
        
        if line == '---':
            if current_doc:
                docs.append(current_doc)
                current_doc = {}
        elif ':' in line and not line.startswith('#'):
            key, value = line.split(':', 1)
            current_doc[key.strip()] = value.strip()
    
    # Add last document
    if current_doc:
        docs.append(current_doc)
    
    return docs


def extract_tests_from_directory(
    standards_dir: Path,
    output_dir: Path,
    limit: int = None
) -> Dict[str, Any]:
    """
    Extract SQL tests from sqltest standards directory.
    
    Args:
        standards_dir: Path to standards/2016 directory
        output_dir: Path to output directory for manifest
        limit: Optional limit on number of tests (for initial implementation)
    
    Returns:
        Dictionary containing test manifest
    """
    tests = []
    
    # Focus on Core features (E0xx) and Basic features (F0xx)
    # These are most likely to be SQL:1999 compatible
    feature_dirs = ['E', 'F']
    
    for feature_dir in feature_dirs:
        dir_path = standards_dir / feature_dir
        if not dir_path.exists():
            continue
            
        # Find all .tests.yml files (these contain generated tests)
        test_files = sorted(dir_path.glob('*.tests.yml'))
        
        for test_file in test_files:
            print(f"Processing {test_file.name}...")
            
            try:
                test_cases = load_yaml_file(test_file)
                
                for test_case in test_cases:
                    if not test_case or 'sql' not in test_case:
                        continue
                    
                    # Extract test metadata
                    test_id = test_case.get('id', f"unknown_{len(tests)}")
                    feature = test_case.get('feature', 'unknown')
                    sql = test_case['sql']
                    
                    # Determine category based on feature prefix
                    if feature.startswith('E0'):
                        category = 'Core'
                    elif feature.startswith('F0'):
                        category = 'Foundation'
                    else:
                        category = 'Optional'
                    
                    # Create test entry
                    test_entry = {
                        'id': test_id,
                        'feature': feature,
                        'category': category,
                        'sql': sql,
                        # Note: sqltest doesn't specify expected results,
                        # it just tests that SQL is valid/accepted
                        'expect_success': True
                    }
                    
                    tests.append(test_entry)
                    
                    # Apply limit if specified
                    if limit and len(tests) >= limit:
                        break
                        
            except Exception as e:
                print(f"  Error processing {test_file}: {e}")
                continue
            
            if limit and len(tests) >= limit:
                break
        
        if limit and len(tests) >= limit:
            break
    
    # Create manifest
    manifest = {
        'version': 'SQL:1999 (from SQL:2016 subset)',
        'source': 'sqltest by Elliot Chance',
        'url': 'https://github.com/elliotchance/sqltest',
        'test_count': len(tests),
        'tests': tests
    }
    
    return manifest


def main():
    """Main entry point."""
    # Paths relative to repository root
    repo_root = Path(__file__).parent.parent
    sqltest_dir = repo_root / 'third_party' / 'sqltest' / 'standards' / '2016'
    output_dir = repo_root / 'tests' / 'sql1999'
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract tests (limit to 100 for initial implementation)
    print("Extracting SQL:1999-compatible tests from sqltest...")
    manifest = extract_tests_from_directory(
        sqltest_dir,
        output_dir,
        limit=100
    )
    
    # Write manifest
    manifest_path = output_dir / 'manifest.json'
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✅ Extracted {manifest['test_count']} tests")
    print(f"📄 Manifest written to: {manifest_path}")
    
    # Print summary
    categories = {}
    for test in manifest['tests']:
        cat = test['category']
        categories[cat] = categories.get(cat, 0) + 1
    
    print("\nTest breakdown by category:")
    for category, count in sorted(categories.items()):
        print(f"  {category}: {count} tests")


if __name__ == '__main__':
    main()
