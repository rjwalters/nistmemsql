# GitHub Workflows

This directory contains CI/CD workflows for the nistmemsql project.

## Workflows

### `ci-and-deploy.yml` - Main CI/CD Pipeline

**Trigger**: On every push and pull request to `main`

**Purpose**:
- Runs all tests (unit, integration, sqltest conformance)
- Runs SQLLogicTest suite (5-minute random sample)
- Runs benchmarks
- Generates badges and documentation
- Deploys to GitHub Pages

**SQLLogicTest Strategy**:
- Each run tests a random sample of files for 5 minutes
- Results are merged with historical cumulative data from gh-pages
- Cumulative data includes results from both regular CI runs AND boost runs
- Badge shows: `{passed}✓ {failed}✗ ({coverage}% tested)`

### `boost-sqllogictest.yml` - Coverage Boost Runs

**Trigger**: Manual (workflow_dispatch) or optional scheduled runs

**Purpose**: Rapidly build SQLLogicTest coverage by running additional test sessions

**How to Use**:

1. **Manual Trigger** (via GitHub Actions UI):
   - Go to Actions → "Boost SQLLogicTest Coverage" → "Run workflow"
   - Configure:
     - **Time budget**: Seconds per run (default: 300 = 5 minutes)
     - **Run count**: Number of sequential runs (default: 1)

   Examples:
   - Quick boost: 1 run × 5 minutes = ~3-20 files tested
   - Medium boost: 5 runs × 5 minutes = ~15-100 files tested
   - Deep boost: 10 runs × 10 minutes = ~100-300 files tested

2. **Via GitHub CLI**:
   ```bash
   # Quick boost (1 run, 5 minutes)
   gh workflow run boost-sqllogictest.yml

   # Medium boost (5 runs, 5 minutes each)
   gh workflow run boost-sqllogictest.yml \
     -f run_count=5 \
     -f time_budget=300

   # Deep boost (10 runs, 10 minutes each)
   gh workflow run boost-sqllogictest.yml \
     -f run_count=10 \
     -f time_budget=600
   ```

3. **Via API**:
   ```bash
   curl -X POST \
     -H "Accept: application/vnd.github+json" \
     -H "Authorization: token $GITHUB_TOKEN" \
     https://api.github.com/repos/rjwalters/nistmemsql/actions/workflows/boost-sqllogictest.yml/dispatches \
     -d '{"ref":"main","inputs":{"time_budget":"600","run_count":"5"}}'
   ```

**How It Works**:

1. Downloads current cumulative results from gh-pages
2. Runs N sequential test sessions (each with unique random seed)
3. After each run, merges results with cumulative data
4. Uploads updated cumulative results to gh-pages
5. Future CI runs automatically pick up the new coverage

**Benefits**:

- **Progressive Coverage**: Each run tests different files (random sampling)
- **Flexible**: Run as much or as little as you want
- **No Blocking**: Runs independently, doesn't block main CI
- **Automatic Integration**: Results automatically merged into main CI badge
- **Cost Effective**: Only run when you want faster coverage growth

**When to Use Boost Runs**:

- Before a release to maximize test coverage
- When adding new SQL features (see if existing tests pass)
- After fixing bugs (test broader corpus for regressions)
- To rapidly build coverage in the first few weeks
- Whenever you have available GitHub Actions minutes to spare

**Artifacts**:

Each boost run uploads:
- `sqllogictest_cumulative.json` - Updated cumulative results
- `sqllogictest_analysis.json` - Analysis of the latest run
- `boost_runs/*.json` - Individual run results
- `boost_summary.md` - Human-readable summary

### `label-external-contributors.yml` - PR Labeling

**Trigger**: On pull requests

**Purpose**: Automatically labels PRs from external contributors

## SQLLogicTest Coverage Strategy

The project uses a **progressive random sampling** strategy to build comprehensive test coverage over time:

### How It Works

1. **Test Corpus**: 623 files, ~5.9 million individual SQL tests
2. **Regular CI**: Each commit tests ~3-20 random files (5-minute budget)
3. **Boost Runs**: Optional manual runs to accelerate coverage
4. **Cumulative Results**: All results merge into `sqllogictest_cumulative.json`
5. **Badge**: Shows cumulative passed/failed/coverage stats

### Coverage Progression

**Without boost runs** (organic growth):
- Week 1: ~5-10% coverage (10-60 files tested)
- Month 1: ~20-30% coverage (120-190 files tested)
- Month 3: ~50-70% coverage (310-440 files tested)

**With strategic boost runs**:
- Day 1 (10 boost runs): ~20-40% coverage
- Week 1 (20 boost runs): ~50-80% coverage
- Month 1: ~90-100% coverage

### Best Practices

1. **Regular CI**: Let it run naturally on every commit
2. **Boost Runs**: Use strategically for:
   - Initial coverage buildup
   - Pre-release testing
   - After major feature additions
3. **Monitoring**: Check badge and cumulative results to track progress
4. **Analysis**: Review `sqllogictest_analysis.json` to find common failure patterns

### Files

- **Cumulative Results**: Published to `https://rjwalters.github.io/nistmemsql/badges/sqllogictest_cumulative.json`
- **Analysis**: `https://rjwalters.github.io/nistmemsql/badges/sqllogictest_analysis.json`
- **Markdown Report**: `https://rjwalters.github.io/nistmemsql/badges/sqllogictest_analysis.md`

## Maintenance

### Adding New Tests

When adding new SQL features, the SQLLogicTest suite will automatically test them over time. No changes to workflows needed.

### Resetting Coverage

To start fresh (rare):
1. Delete `sqllogictest_cumulative.json` from gh-pages branch
2. Next CI run will start a new cumulative dataset

### Troubleshooting

**Boost run fails to update gh-pages**:
- Check that the workflow has write permissions
- Verify gh-pages branch exists
- Check workflow logs for git push errors

**Coverage not increasing**:
- Check if tests are timing out (increase time_budget)
- Verify random seed is changing (should use timestamp)
- Review logs to see if same files are being tested

**Badge not updating**:
- Verify `sqllogictest_cumulative.json` exists on gh-pages
- Check that badge JSON is being generated correctly
- Clear browser cache and check badge endpoint directly
