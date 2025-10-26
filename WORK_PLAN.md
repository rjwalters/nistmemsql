# Work Plan: Next Steps

**Status**: Phase 1 In Progress - TDD Foundation Building
**Last Updated**: 2024-10-25
**Current Phase**: Phase 1 (Foundation)
**Development Approach**: Test-Driven Development (TDD) ✅

## What We've Accomplished

✅ **Planning Complete** (Phase 0)
- Requirements clarified (all 7 GitHub issues answered)
- SQL:1999 standard researched
- Testing strategy designed (sqltest)
- Documentation infrastructure created
- Major simplifications identified (60-70% scope reduction)
- Language chosen (Rust - ADR-0001)
- Cargo workspace initialized (7 crates)

✅ **Types Crate Complete** (TDD Cycle 1) - 27 tests passing
- DataType enum with all SQL:1999 basic types
- SqlValue enum for runtime values
- Type compatibility checking (is_compatible_with)
- NULL handling and three-valued logic foundation
- Display formatting for SQL values
- Comprehensive test coverage for all type operations

✅ **AST Crate Complete** (TDD Cycle 2) - 22 tests passing
- Statement enum: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
- Expression enum: Literals, ColumnRef, BinaryOp, UnaryOp, Function, IsNull
- SelectStmt with full clause support (from, where, group_by, having, order_by)
- BinaryOperator and UnaryOperator enums
- FromClause with JOIN support
- OrderByItem with direction support

✅ **Development Tooling**
- rustfmt configured (100 char width, Unix newlines)
- clippy configured (complexity threshold 30)
- Zero warnings, 49 tests passing total

✅ **Parser Strategy Decision** (ADR-0002) - Complete
- Evaluated 5 options: pest, lalrpop, nom, chumsky, hand-written
- **Decision**: Hand-written recursive descent + Pratt parser
- Rationale: Perfect TDD fit, SQL:1999 FULL compliance, proven approach (sqlparser-rs)
- 500+ line comprehensive ADR with decision matrix
- Implementation strategy defined

## What's Next: Immediate Priorities

### Priority 1: Build Lexer/Tokenizer (parser crate)

**Task**: Implement lexer following hand-written approach (from ADR-0002)

**What to Build** (TDD approach):
```rust
// Token types
pub enum Token {
    Keyword(Keyword),
    Identifier(String),
    Number(String),
    String(String),
    Symbol(char),
    // ... all SQL tokens
}

// Lexer
pub struct Lexer {
    input: String,
    position: usize,
}

impl Lexer {
    pub fn tokenize(&mut self) -> Result<Vec<Token>> { ... }
}
```

**TDD Steps**:
1. Write test for keywords (SELECT, FROM, WHERE)
2. Implement keyword recognition
3. Write test for identifiers (table names, column names)
4. Implement identifier tokenization
5. Write test for numbers (42, 3.14)
6. Implement number tokenization
7. Write test for strings ('hello', "world")
8. Implement string tokenization

**Deliverable**: Working lexer with comprehensive tests

**Time Estimate**: 4-6 hours

---

### Priority 2: Build Basic Parser (parser crate)

**Task**: Implement recursive descent parser for simple SELECT

**What to Build** (Incremental):

#### Step 1: Simple SELECT Parser
```sql
-- Start with basics
SELECT 42;
SELECT 'hello';
SELECT 1 + 2;
```

#### Step 2: Add WHERE
```sql
SELECT * FROM users WHERE id = 1;
SELECT name FROM users WHERE age > 18;
```

#### Step 3: Add Basic Expressions
```sql
SELECT id, name, age FROM users;
SELECT id + 1, name FROM users;
SELECT COUNT(*) FROM users;
```

#### Step 4: Expand gradually
- Joins
- Subqueries
- Aggregates
- More complex expressions

**Deliverable**: Parser that produces AST for basic SQL

**Time Estimate**: 8-12 hours for initial parser

---

## Phase 1 Roadmap (4-6 weeks estimated)

### Week 1: Foundation (✅ COMPLETE)
- [x] ADR-0002: Choose parser strategy - **Hand-written** ✅
- [x] Implement types crate (basic types) - 27 tests passing ✅
- [x] Implement ast crate (core structures) - 22 tests passing ✅
- [x] Build lexer/tokenizer with TDD (parser crate) - 34 tests passing ✅
- [x] Parse simple SELECT statements (`SELECT 42;`) - 13 tests passing ✅

**Progress**: 5 of 5 tasks complete! ✅

**Milestone Achieved**: `SELECT 42;` parses to AST ✅

### Week 2-3: Core SQL Parsing (✅ COMPLETE)
- [x] Add all SQL:1999 data types to types crate ✅
- [x] Expand AST for all statement types ✅
- [x] Implement full SELECT parsing (no joins yet) ✅
- [x] Add INSERT, UPDATE, DELETE parsing ✅
- [x] CREATE TABLE parsing ✅
- [x] AND/OR logical operators ✅
- [x] Operator precedence (*, /, +, -, comparison, AND, OR) ✅

**Total Tests**: 55 in parser crate, 107 workspace-wide

**Milestone Achieved**: Basic DML/DDL statements parse ✅

### Week 3-4: Complex Parsing (IN PROGRESS 🚧)
- [ ] JOIN operations (NEXT - starting now!)
- [ ] Subqueries
- [ ] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] GROUP BY and HAVING
- [ ] ORDER BY with multiple columns

**Current Task**: Implementing JOIN support

**Milestone**: Complex SELECTs with joins parse ✅

### Week 5-6: SQL:1999 Features
- [ ] CASE expressions
- [ ] CAST operations
- [ ] Boolean type support
- [ ] WITH (common table expressions)
- [ ] Window functions (if time)

**Milestone**: Most SQL:1999 Core features parse ✅

---

## Immediate Next Session Plan

### Session Goal: Make Parser Strategy Decision + Start Types Crate

**Tasks**:
1. **Research parser options** (1-2 hours)
   - Read pest documentation and examples
   - Read lalrpop documentation and examples
   - Look for SQL grammar examples for each
   - Evaluate based on decision criteria

2. **Create ADR-0002** (1 hour)
   - Document options
   - Make decision (likely pest or lalrpop)
   - Explain rationale
   - Update DECISIONS.md

3. **Start types crate** (2-3 hours)
   - Implement basic DataType enum
   - Implement basic SqlValue enum
   - Add tests for type operations
   - Document in crate README

**End Goal**: Have parser tool chosen and types crate started

**Estimated Total Time**: 4-6 hours

---

## Success Metrics

### Phase 1 Complete When:
- [ ] All basic SQL statements parse correctly
- [ ] AST accurately represents SQL:1999 structures
- [ ] Type system supports all SQL:1999 data types
- [ ] Parser has comprehensive error messages
- [ ] All parser tests pass
- [ ] Can parse 50+ different SQL queries

### Quality Gates:
- All code passes `cargo clippy` (no warnings)
- All tests pass (`cargo test`)
- Documentation complete for each crate
- No compiler warnings
- Clear error messages for invalid SQL

---

## Risk Management

### Known Risks

**Risk 1: SQL Grammar Complexity**
- SQL:1999 grammar is huge and ambiguous
- **Mitigation**: Start simple, iterate, use existing grammar as reference
- **Fallback**: Simplify grammar where standard allows flexibility

**Risk 2: Parser Generator Learning Curve**
- New tool, need to learn
- **Mitigation**: Start with tutorials, simple examples
- **Fallback**: Can switch tools if one proves too difficult

**Risk 3: Type System Complexity**
- User-defined types, arrays, nested structures
- **Mitigation**: Implement incrementally (basic → advanced)
- **Fallback**: Phase 4 can handle advanced types

---

## Questions to Answer

### Before Starting Parser:
- [ ] Which parser generator? (ADR-0002)
- [ ] Do we have SQL:1999 grammar reference?
- [ ] What's our test strategy for the parser?

### Before Starting Types:
- [ ] Start with all types or incrementally?
- [ ] How to handle type coercion rules?
- [ ] What about NULL handling in type system?

### Architecture Questions:
- [ ] How do crates depend on each other?
- [ ] Where does semantic analysis happen?
- [ ] How to structure error types?

---

## Resources Needed

### SQL:1999 Grammar
- [ ] SQL:1999 BNF from ronsavage.github.io ✅ (already found)
- [ ] Example grammars from pest/lalrpop projects
- [ ] PostgreSQL grammar as reference

### Parser Examples
- [ ] Find existing SQL parsers in Rust
- [ ] Look at pest SQL examples
- [ ] Look at lalrpop SQL examples

### Documentation
- [ ] Rust Book for reference
- [ ] Parser tool docs (based on ADR-0002 choice)
- [ ] SQL:1999 standard sections on types

---

## How to Track Progress

### Daily:
- Update todo list with TodoWrite
- Commit working code frequently
- Document discoveries in LESSONS_LEARNED.md

### Weekly:
- Update docs/lessons/WEEKLY.md
- Review progress against milestones
- Adjust plan if needed

### Per Milestone:
- Create summary of what was built
- Document challenges in docs/lessons/CHALLENGES.md
- Update WORK_PLAN.md with learnings

---

## Current Status Summary

**Completed**:
- ✅ Planning and research
- ✅ Requirements clarification
- ✅ Language choice (Rust - ADR-0001)
- ✅ Parser strategy choice (Hand-written - ADR-0002)
- ✅ Project structure initialized (Cargo workspace, 7 crates)
- ✅ Documentation infrastructure
- ✅ Types crate implementation (27 tests) 🦀
- ✅ AST crate implementation (22 tests) 🦀
- ✅ Development tooling (rustfmt, clippy)
- ✅ Parser crate - Lexer implementation (34 tests) 🦀
- ✅ Parser crate - Basic SELECT parsing (13 tests) 🦀
- ✅ Parser crate - INSERT/UPDATE/DELETE/CREATE TABLE (8 tests) 🦀
- ✅ Week 1 Foundation (100% complete)
- ✅ Week 2-3 Core SQL Parsing (100% complete)

**In Progress**:
- 🚧 Week 3-4 Complex Parsing (JOIN operations next!)

**Not Started**:
- ⏳ Catalog crate (schema metadata)
- ⏳ Storage crate (in-memory tables)
- ⏳ Executor crate (query execution)
- ⏳ Transaction crate (ACID properties)

**Confidence Level**: Extremely High! 🚀🚀🚀

TDD approach is working BRILLIANTLY! We have **107 passing tests**, zero warnings, solid foundation with full parser for all basic SQL statements. Three TDD cycles complete (types, ast, parser). Ready to tackle complex features like JOINs!

---

## Next Steps (Immediate)

1. **Right Now**: Begin TDD Cycle 4 - JOIN Operations
2. **This Session**:
   - Check AST for JOIN support (already defined!)
   - Write JOIN parsing tests (INNER JOIN, LEFT JOIN, RIGHT JOIN)
   - Implement JOIN parsing to make tests pass
   - Verify complex JOIN queries parse correctly
3. **Next Features**:
   - Subqueries
   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
   - GROUP BY and HAVING clauses
   - More complex ORDER BY

**Let's continue building with TDD!** 🦀

---

**Status Update** (2025-10-25):
✅ TDD Cycles 1, 2, & 3 Complete (types + ast + parser)
✅ ADR-0001 & ADR-0002 Complete (Rust + Hand-written parser)
✅ Week 1 & Week 2-3 Complete (Foundation + Core SQL Parsing)
🚧 Starting Week 3-4: Complex Parsing (JOIN operations)
📈 Confidence: Extremely High - 107 tests passing, zero warnings, proven TDD success!
