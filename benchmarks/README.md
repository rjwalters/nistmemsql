# VibeSQL Benchmarking

This directory contains comprehensive performance benchmarking tools for VibeSQL.

## Directory Structure

```
benchmarks/
├── suite/          # Full SQLLogicTest suite benchmarks
│   ├── suite.sh    # Run all 623 test files
│   ├── head-to-head.sh  # VibeSQL vs SQLite comparison
│   ├── analyze.py  # Load results into database
│   ├── schema.sql  # Database schema for results
│   └── README.md   # Detailed documentation
│
└── micro/          # Pytest-based micro-benchmarks
    ├── test_*.py   # Individual operation benchmarks
    ├── utils/      # Helper utilities
    └── README.md   # Micro-benchmark documentation
```

## Quick Start

### Full Suite Benchmark

Run all 623 SQLLogicTest files:

```bash
cd suite
./suite.sh
```

Results saved to `../target/benchmarks/comparison_YYYYMMDD_HHMMSS.json`

### Micro-Benchmarks

Run specific operation benchmarks with pytest:

```bash
cd micro
pytest test_aggregates.py -v
```

## What to Use When

### Suite Benchmarks (`suite/`)

Use for:
- Comprehensive performance testing across all features
- Tracking overall performance trends
- Comparing VibeSQL vs SQLite on standard test corpus
- Pre-release performance validation

**Tools:**
- `suite.sh` - Full 623-file benchmark (VibeSQL only)
- `head-to-head.sh` - Interleaved comparison (VibeSQL vs SQLite)
- `analyze.py` - Store results in database for analysis

### Micro-Benchmarks (`micro/`)

Use for:
- Testing specific operations (aggregates, joins, inserts, etc.)
- Profiling individual query patterns
- Development-time performance checks
- Targeted optimization work

**Tools:**
- `pytest` framework with parametrized tests
- Memory profiling support
- TPCH query benchmarks
- Custom data generation utilities

## Documentation

- **Suite benchmarks**: See [suite/README.md](suite/README.md)
- **Micro-benchmarks**: See [micro/README.md](micro/README.md) (if exists)
- **General benchmarking**: See [docs/performance/BENCHMARKING.md](../docs/performance/BENCHMARKING.md)

## Recent Performance

| Date | Total Time | Avg Time | Pass Rate | Notes |
|------|-----------|----------|-----------|-------|
| 2025-11-12 | 87.45s | 0.140s | 623/623 (100%) | After evaluator optimization |
| 2025-11-12 | 120.48s | 0.193s | 623/623 (100%) | Initial baseline |

## Contributing

When adding benchmarks:
1. Use `suite/` for SQLLogicTest-based integration benchmarks
2. Use `micro/` for pytest-based unit/micro benchmarks
3. Document what you're measuring and why
4. Include baseline results in PR descriptions
