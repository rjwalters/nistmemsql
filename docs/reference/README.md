# SQLite Source Code Reference

This directory contains the SQLite source code as a reference for learning and optimization.

## Purpose

SQLite is the **gold standard** for embedded SQL databases with decades of optimization experience. We include their source code as a reference to:

1. **Learn optimization techniques** - Study how they optimize specific operations
2. **Understand implementation patterns** - See how they structure aggregates, joins, constraint checking
3. **Compare approaches** - Validate our architectural decisions against battle-tested code
4. **Get specific insights** - Answer questions like "How does SQLite implement COUNT(*)?"

## License

SQLite is in the **public domain** - it's not even open source, it's more permissive!

From sqlite.org:
> SQLite is in the public domain and does not require a license.

This means we can:
- ✅ Use as reference
- ✅ Study implementation
- ✅ Include as submodule
- ✅ No attribution required (though we credit them anyway)

**Note**: We're learning from SQLite, not copying code. This is a reference for understanding proven database implementation patterns.

## Getting Started

### First Time Setup

If you just cloned the repository:

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/rjwalters/vibesql.git

# Or if already cloned
git submodule update --init --recursive
```

### Updating SQLite Reference

To update to the latest SQLite version:

```bash
cd docs/reference/sqlite
git pull origin master
cd ../../..
git add docs/reference/sqlite
git commit -m "docs: Update SQLite reference to latest version"
```

## Key Files to Study

### 1. Aggregate Optimization (COUNT, SUM, AVG, MIN, MAX)

**Files to study**:
- `src/vdbe.c` - Virtual Database Engine (query execution)
- `src/select.c` - SELECT statement processing
- `src/where.c` - WHERE clause optimization

**Questions to answer**:
- How do they optimize `COUNT(*)`?
- Do they have fast paths for simple aggregates?
- How do they handle aggregate + GROUP BY?

### 2. UPDATE Performance

**Files to study**:
- `src/update.c` - UPDATE statement implementation
- `src/vdbe.c` - Execution engine
- `src/btree.c` - B-tree storage layer

**Questions to answer**:
- How do they batch constraint checks?
- What's their UPDATE execution flow?
- How do they optimize primary key lookups?

### 3. Expression Evaluation

**Files to study**:
- `src/expr.c` - Expression evaluation
- `src/vdbe.c` - Expression execution in VDBE

**Questions to answer**:
- How do they minimize allocations?
- What's their expression evaluation strategy?
- How do they handle type coercion?

### 4. Query Optimization

**Files to study**:
- `src/where.c` - WHERE clause analysis and optimization
- `src/select.c` - Query planning
- `src/analyze.c` - Query statistics

**Questions to answer**:
- How do they detect fast paths?
- What optimizations do they apply?
- How do they handle index selection?

### 5. Storage and Indexing

**Files to study**:
- `src/btree.c` - B-tree implementation
- `src/pager.c` - Page cache
- `src/wal.c` - Write-Ahead Logging

**Questions to answer**:
- How do they structure their storage?
- What indexing strategies do they use?
- How do they maintain hash indexes?

## Searching the SQLite Source

### Finding Specific Functions

```bash
# Search for a specific function
grep -r "sqlite3_count" docs/reference/sqlite/src/

# Search for aggregate implementations
grep -r "Aggregate" docs/reference/sqlite/src/

# Search for optimization patterns
grep -r "optimization" docs/reference/sqlite/src/
```

### Finding Specific Features

```bash
# COUNT(*) optimization
grep -r "COUNT(\*)" docs/reference/sqlite/src/

# UPDATE statement handling
grep -r "UPDATE" docs/reference/sqlite/src/update.c

# Hash join implementation
grep -r "hash.*join" docs/reference/sqlite/src/
```

## Related Documentation

- **COMPARISONS.md** - Documents our implementation vs SQLite's approach
- **SQLite Official Docs** - https://www.sqlite.org/docs.html
- **SQLite Source Docs** - https://www.sqlite.org/src/doc/trunk/README.md

## Benefits

1. **Immediate reference** - Can quickly grep/search SQLite code when investigating issues
2. **Learning resource** - Team can study proven implementations
3. **Validation** - Confirm our approaches match or differ from SQLite intentionally
4. **Optimization ideas** - Discover techniques we haven't considered
5. **Documentation** - Can link to specific SQLite code in our docs/issues

## Non-Goals

- ❌ **Not copying code** - We're learning, not copying
- ❌ **Not porting SQLite** - We're building our own DB with our own design
- ❌ **Not benchmarking against SQLite internals** - Just studying implementation

## Related Issues

- #814 - COUNT(*) optimization
- #815 - UPDATE optimization
- #816 - Debug print removal
