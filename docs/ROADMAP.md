# VibeSQL Development Roadmap

**Last Updated**: 2025-01-13
**Project Status**: Core SQL:1999 Complete, Extended Features In Progress

---

## 🎯 Project Overview

VibeSQL is building towards **complete SQL:1999 compliance** through a phased approach. We've successfully completed Core SQL:1999 compliance and are now working on extended features and comprehensive test coverage.

### Current Status at a Glance

| Initiative | Status | Progress | Priority |
|-----------|--------|----------|----------|
| **SQL:1999 Core Compliance** | ✅ Complete | 739/739 tests (100%) | Achieved |
| **Parallelism (Phase 1)** | ✅ Complete | Auto-parallel execution | Achieved |
| **SQLLogicTest Conformance** | 🔄 In Progress | 83/613 files (13.5%) | **High** |
| **Predicate Pushdown Optimization** | ⏸️ Infrastructure Ready | Phase 1 done, Phase 2-3 pending | Low |
| **SQL:1999 Extended Features** | 🔄 In Progress | Views, procedures, spatial, FTS | Medium |

---

## 📊 Completed Milestones

### ✅ SQL:1999 Core Compliance (Oct 25 - Nov 1, 2024)

**Achievement**: 100% conformance in 7 days

**What was delivered:**
- All 169 mandatory Core features implemented
- 739/739 sqltest conformance tests passing
- Complete type system (14 types)
- Full query engine (joins, subqueries, CTEs, window functions)
- DDL with constraints (PK, FK, UNIQUE, CHECK, NOT NULL)
- Transaction support (ACID properties, savepoints)
- 50+ built-in functions
- Security model (roles, grants, privilege enforcement)

**Key Achievement**: Strategic pivot to direct API testing instead of ODBC/JDBC saved 4-6 months of development time while maintaining full standards compliance.

**Documentation**: [`docs/archive/ROADMAP_CORE_COMPLIANCE.md`](archive/ROADMAP_CORE_COMPLIANCE.md) (archived)

---

### ✅ Parallelism Phase 1 (January 2025)

**Achievement**: Automatic hardware-aware parallel execution

**What was delivered:**
- Automatic parallelism on 8+ core systems (no configuration needed)
- Operation-specific thresholds (scan: 2k, join: 5k, aggregate: 3k, sort: 5k)
- Parallel table scans (4-6x speedup on large scans)
- Parallel hash join build (4-6x speedup on large joins)
- Parallel aggregation with combinable accumulators (3-5x speedup)
- Parallel sorting via Rayon (2-3x speedup)
- Comprehensive benchmarks and unit tests

**Performance Impact**: 4-8x speedup on analytical queries with multi-core systems

**Documentation**: [`docs/roadmaps/PARALLELISM_ROADMAP.md`](roadmaps/PARALLELISM_ROADMAP.md)

---

## 🚧 Active Work

### 🔄 SQLLogicTest Conformance (Current Priority)

**Goal**: Achieve 100% pass rate on SQLite's official test corpus (~5.9M tests, 623 files)

**Current Status**:
- Pass rate: 13.5% (83/613 files)
- Known blocker: Index optimization bug (issue #1610)

**Strategic Approach**:
Instead of fixing individual test failures, we've identified **8 root causes** affecting 85% of failures:

| Priority | Issue | Tests Affected | Effort | Expected Impact |
|----------|-------|----------------|--------|-----------------|
| P0 | #956 - Decimal formatting | 56 | Low | +11% |
| P0 | #957 - Multi-row formatting | 19 | Low | +3% |
| P0 | #959 - Hash mismatches | 138 | Medium | +27% |
| P0 | #960 - Result mismatches | 112 | Medium | +22% |
| P1 | #958 - Column resolution | 39 | Medium | +8% |
| P1 | #963 - Parse errors | 42 | Medium | +8% |
| P2 | #962 - NOT NULL handling | 29 | Low | +6% |
| P2 | #961 - NULLIF/COALESCE | 13 | Low | +2% |

**Quick Wins**: Issues #956 and #957 are low effort and would improve pass rate from 13.5% → 29% (+15 percentage points)

**Next Steps**:
1. Fix #956 (decimal formatting) - Low effort, high impact
2. Fix #957 (multi-row formatting) - Low effort, quick validation
3. Rerun full suite to validate methodology
4. Tackle medium-effort fixes (#959, #960, #958)
5. Target 87%+ pass rate by end of Phase 2

**Documentation**: [`docs/testing/sqllogictest/SQLLOGICTEST_ROADMAP.md`](testing/sqllogictest/SQLLOGICTEST_ROADMAP.md)

---

### 🔄 Extended SQL:1999 Features (In Progress)

**Completed Nov 1-12, 2024:**
- ✅ Views with OR REPLACE and column lists
- ✅ Stored procedures & functions with parameter modes (IN/OUT/INOUT)
- ✅ Spatial/geometric functions (complete ST_* library)
- ✅ Full-text search (MATCH AGAINST with FULLTEXT indexes)
- ✅ Advanced indexing (REINDEX, composite, spatial)

**Remaining Work:**
- Triggers (CREATE/DROP TRIGGER, trigger execution)
- Information schema views
- Recursive CTEs (WITH RECURSIVE execution)
- Advanced SQL:2003+ features

---

## ⏸️ Deferred Initiatives

### Predicate Pushdown Optimization

**Status**: Infrastructure complete, implementation phases deferred

**What's Done**:
- Phase 1: Predicate analysis and decomposition infrastructure (`where_pushdown.rs`)
- CNF decomposition, table reference extraction, predicate classification

**What's Pending**:
- Phase 2: Scanner integration (apply table-local predicates during scan)
- Phase 3: Join-level pushdown (apply equijoin predicates in join operations)

**Why Deferred**:
- Not blocking any current functionality
- Performance optimization, not correctness issue
- SQLLogicTest conformance is higher priority
- Can revisit after achieving >90% test coverage

**Expected Impact When Implemented**:
- 10x+ memory reduction on large multi-table joins
- Fixes select5.test memory exhaustion (6.48 GB → <100 MB)

**Documentation**: [`docs/roadmaps/PREDICATE_PUSHDOWN_ROADMAP.md`](roadmaps/PREDICATE_PUSHDOWN_ROADMAP.md)

---

### Parallelism Phase 2+

**Status**: Phase 1 complete and working well, Phase 2+ deferred

**Phase 1 Achievements** (Complete):
- Automatic parallelism without configuration
- 4-8x speedup on multi-core systems
- All operations parallelized (scan, join, aggregate, sort)

**Phase 2 Scope** (Deferred):
- Architecture refactoring for concurrent query execution
- Database interior mutability (RwLock-based access)
- Multiple concurrent SELECT queries

**Phase 3 Scope** (Deferred):
- Morsel-driven execution
- Memory-aware parallelism
- Adaptive execution

**Why Deferred**:
- Phase 1 delivers most of the performance benefits
- No immediate need for concurrent query execution
- SQLLogicTest conformance is higher priority

**Documentation**: [`docs/roadmaps/PARALLELISM_ROADMAP.md`](roadmaps/PARALLELISM_ROADMAP.md)

---

## 🎯 Success Metrics

### Overall Project Goals

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| SQL:1999 Core Conformance | 100% | 100% | ✅ |
| sqltest conformance | 739/739 | 739/739 | ✅ |
| Custom unit tests | >2,500 | 2,991 | ✅ |
| SQLLogicTest pass rate | >90% | 13.5% | 🔄 |
| Code coverage | >85% | 86% | ✅ |
| Parallel speedup (8 cores) | 4-8x | 4-8x | ✅ |

### Near-Term Targets (Q1 2025)

1. **SQLLogicTest Quick Wins** (Week 1-2)
   - Target: 29% pass rate (180/613 files)
   - Fix issues #956, #957
   - Validate methodology

2. **SQLLogicTest Phase 2** (Week 3-6)
   - Target: 87% pass rate (533/613 files)
   - Fix issues #959, #960, #958
   - Comprehensive validation

3. **SQLLogicTest Phase 3** (Week 7-10)
   - Target: 95%+ pass rate (582+/613 files)
   - Fix remaining issues (#963, #962, #961)
   - Edge case handling

---

## 📚 Documentation Structure

### Active Roadmaps
- **This document** - Master roadmap and project overview
- [`SQLLOGICTEST_ROADMAP.md`](testing/sqllogictest/SQLLOGICTEST_ROADMAP.md) - Detailed conformance strategy
- [`PARALLELISM_ROADMAP.md`](roadmaps/PARALLELISM_ROADMAP.md) - Parallelism implementation phases

### Completed Roadmaps (Archived)
- [`ROADMAP_CORE_COMPLIANCE.md`](archive/ROADMAP_CORE_COMPLIANCE.md) - Historical record of Core compliance achievement (100% complete, Oct-Nov 2024)

### Supporting Documentation
- [`REQUIREMENTS.md`](planning/REQUIREMENTS.md) - Original project requirements
- [`TESTING_STRATEGY.md`](testing/TESTING_STRATEGY.md) - Testing methodology
- [`PREDICATE_PUSHDOWN_ROADMAP.md`](roadmaps/PREDICATE_PUSHDOWN_ROADMAP.md) - Optimization details

---

## 🔄 Development Workflow

### Current Focus Areas (Priority Order)

1. **SQLLogicTest Conformance** - Primary focus
   - Fix 8 root cause issues
   - Systematic validation after each fix
   - Target: 90%+ pass rate

2. **Bug Fixes** - Ongoing
   - Address issues as discovered
   - Maintain test pass rate
   - Regression prevention

3. **Documentation** - Ongoing
   - Keep roadmaps updated
   - Document architectural decisions
   - Maintain API documentation

4. **Performance** - Secondary focus
   - Monitor performance metrics
   - Address regressions
   - Deferred optimizations as needed

### Release Planning

Current strategy: Continuous development with milestone-based releases

**Next Milestones:**
- v0.2.0: SQLLogicTest 29% pass rate (Quick Wins)
- v0.3.0: SQLLogicTest 87% pass rate (Phase 2 complete)
- v0.4.0: SQLLogicTest 95%+ pass rate (Phase 3 complete)
- v1.0.0: 100% SQLLogicTest conformance + production polish

---

## 🤝 Contributing

### How to Help

The project follows the [Loom orchestration workflow](https://github.com/loomhq/loom):

1. **Builders**: Implement features from GitHub issues
2. **Judges**: Review PRs with `loom:review-requested` label
3. **Curators**: Enhance and organize issues
4. **Doctors**: Fix bugs and address PR feedback

### Good First Issues

Check issues with these labels:
- `loom:issue` - Ready for implementation
- `good-first-issue` - Beginner-friendly
- `P0` - High priority, high impact

### Priority Work

**Most Impactful Right Now:**
1. SQLLogicTest issue #956 (decimal formatting) - Low effort, 56 tests
2. SQLLogicTest issue #957 (multi-row formatting) - Low effort, 19 tests
3. Index optimization bug #1610 - Blocking many tests

---

## 📞 Questions or Feedback?

- **Issues**: [GitHub Issues](https://github.com/rjwalters/vibesql/issues)
- **Discussions**: [GitHub Discussions](https://github.com/rjwalters/vibesql/discussions)
- **Documentation**: [Project Wiki](https://github.com/rjwalters/vibesql/wiki)

---

**Last Updated**: 2025-01-13
**Maintained By**: [Loom AI Agents](https://github.com/loomhq/loom)
**License**: MIT
