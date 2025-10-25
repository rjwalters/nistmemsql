# Lessons Learned

This document captures insights, challenges, and knowledge gained throughout the nistmemsql project.

## Purpose

As we build a FULL SQL:1999 compliant database from scratch - something no existing system has achieved - we'll encounter many challenges and learn valuable lessons. This document serves as:

- A record of what worked and what didn't
- A knowledge base for future contributors
- A reflection tool for continuous improvement
- A way to avoid repeating mistakes

## How to Use This Document

### For Current Development
- Review before starting new features to avoid known pitfalls
- Update weekly with new insights
- Reference when making similar decisions

### For AI Assistants
- Check this before implementing similar features
- Update with discoveries and surprises
- Document SQL:1999 edge cases as you find them

### For Future Contributors
- Read this to understand project history
- Learn from our mistakes and successes
- Add your own lessons as you go

## Document Structure

### Weekly Entries
Detailed lessons from each week are in `docs/lessons/WEEKLY.md`

### Challenge Log
Major challenges and their solutions are in `docs/lessons/CHALLENGES.md`

### SQL:1999 Gotchas
Edge cases and surprises from the SQL:1999 spec are in `docs/lessons/GOTCHAS.md`

## Major Lessons (So Far)

### Planning Phase (Week 1)

#### Lesson 1: Clarify Requirements Early

**What happened**: We opened 7 GitHub issues asking for clarification on the problem statement.

**Why it mattered**: The original spec was ambiguous (which SQL version? what level of compliance? which test suite?).

**Outcome**: Got clear answers from upstream - SQL:1999, FULL compliance, both ODBC and JDBC required.

**Takeaway**: Always clarify ambiguous requirements before diving into implementation. Don't make assumptions.

**Apply to**: Any new feature or requirement - ask questions first.

#### Lesson 2: No Official NIST SQL:1999 Test Suite Exists

**Discovery**: NIST SQL Test Suite Version 6.0 only covers SQL-92, not SQL:1999.

**Impact**: Must build our own comprehensive test suite.

**Solution**: Hybrid approach - sqllogictest (7M+ tests) for baseline + custom SQL:1999 feature tests.

**Takeaway**: Don't assume test infrastructure exists for old standards. Verify availability early.

#### Lesson 3: Document Everything from Day One

**Decision**: Created comprehensive docs/ structure before writing any code.

**Rationale**: This is an unprecedented challenge with complex requirements. Good documentation is essential.

**Benefit**: Clear templates, decision records, and learning logs will help throughout the project.

**Takeaway**: Documentation infrastructure is as important as code infrastructure for complex projects.

#### Lesson 4: The Scope Is Larger Than Initially Apparent

**Initial thought**: "SQL database" sounds like a well-defined problem.

**Reality**:
- SQL:1999 standard is 2000+ pages
- FULL compliance includes rarely-implemented optional features
- Need both ODBC and JDBC drivers
- Estimated 92,000-152,000 lines of code

**Implication**: This is a multi-year project, not a multi-week project.

**Takeaway**: Do thorough research to understand true scope before committing to timelines.

#### Lesson 5: Free Resources Are Available

**Discovery**:
- Complete SQL:1999 BNF grammar freely available
- "SQL-99 Complete, Really" book free online (covers Core SQL:1999)
- sqllogictest provides millions of test cases
- PostgreSQL source code as reference
- Mimer SQL validator for checking syntax

**Value**: Don't need to buy expensive ISO specs immediately; can start with free resources.

**Takeaway**: Research what's freely available before purchasing specifications.

## SQL:1999 Insights

### Three-Valued Logic Is Everywhere

SQL uses three-valued logic (TRUE, FALSE, UNKNOWN), not boolean logic. This affects:
- NULL comparisons: `NULL = NULL` is UNKNOWN, not TRUE
- WHERE clauses: Only TRUE rows are returned, not UNKNOWN
- Boolean operations: `TRUE OR UNKNOWN` is TRUE, but `FALSE AND UNKNOWN` is UNKNOWN

**Reference**: SQL:1999 Part 2, Section 4.4 - "The Boolean type"

### IS vs = for Boolean Comparisons

**Common mistake**: `WHERE bool_column = TRUE`

**Correct**: `WHERE bool_column IS TRUE`

**Why**: The `=` operator returns UNKNOWN for NULL values, while IS handles them correctly.

### Recursive Queries Are Complex

SQL:1999 added WITH RECURSIVE for hierarchical queries. Implementation requires:
- Cycle detection
- Termination guarantees
- Efficient evaluation strategies
- UNION vs UNION ALL semantics

This is more complex than it initially appears.

## Technical Insights

### Parser Complexity

SQL is not context-free - it's context-sensitive. Examples:
- Keywords can be identifiers in some contexts
- Type names can be table names
- Ambiguity between function calls and column references

**Implication**: Simple parser generators may not be sufficient.

### ODBC/JDBC Are Not Simple Wrappers

Initially thought: "Just expose our SQL engine through standard APIs"

Reality:
- ODBC: Complex C state machine with connection pooling, cursor management
- JDBC: Requires Java implementation or complex JNI bindings
- Both protocols have extensive specifications

**Implication**: Protocol implementation is a major project component, not an afterthought.

## Tools and Resources

### Valuable Tools Discovered

1. **sqllogictest-rs**: Rust implementation of SQLLogicTest (7M+ test cases)
2. **ANTLR**: Parser generator with existing SQL grammars
3. **Mimer SQL Validator**: Online SQL:1999 syntax checker
4. **PostgreSQL source**: Excellent reference implementation

### Recommended Reading

1. "SQL-99 Complete, Really" by Gulutzan & Pelzer (free at crate.io)
2. "SQL:1999 Understanding Relational Language Components" by Melton & Simon
3. PostgreSQL documentation (excellent SQL reference)
4. Modern-SQL.com (SQL standard features explained)

## Mistakes to Avoid

### Don't Assume Simple = Easy

**Mistake**: Thinking "it's just SQL" means it's straightforward.

**Reality**: SQL is complex, ambiguous, and full of edge cases.

**Prevention**: Respect the complexity; plan accordingly.

### Don't Skip Research Phase

**Temptation**: Jump straight to coding.

**Problem**: Without understanding requirements and scope, will hit blockers.

**Better approach**: Spend time upfront researching, planning, documenting.

### Don't Underestimate Testing

**Reality**: Testing a database requires:
- Millions of test cases
- Dual-protocol execution (ODBC and JDBC)
- Edge case coverage
- Compliance validation

**Implication**: Test infrastructure is as large as the implementation itself.

## Open Questions

### Question 1: Should We Target SQL-92 First?

**Context**: SQL:1999 is a superset of SQL-92. Should we implement SQL-92 completely, then add SQL:1999 features?

**Pros**: Incremental approach, can use NIST SQL-92 tests

**Cons**: More work overall, may need to refactor for SQL:1999 features

**Status**: To be decided in ADR-0002

### Question 2: How to Handle Implementation-Defined Behavior?

**Context**: SQL:1999 has many "implementation-defined" aspects where databases can choose.

**Question**: How do we make these decisions? Match PostgreSQL? Define our own?

**Status**: Document as we encounter them

## Success Metrics

How will we know we're succeeding?

- [ ] All research documents complete
- [ ] Architecture designed and documented
- [ ] Language and tools selected
- [ ] First SQL query parses successfully
- [ ] First query executes correctly
- [ ] 50% of Core SQL:1999 features implemented
- [ ] 100% of Core SQL:1999 features implemented
- [ ] ODBC driver functional
- [ ] JDBC driver functional
- [ ] All tests passing through both protocols
- [ ] FULL SQL:1999 compliance achieved

## Contributing to This Document

### When to Add Lessons

- After completing a major milestone
- When discovering SQL:1999 edge cases
- After solving difficult bugs
- When finding helpful resources
- After making mistakes (so others can avoid them)

### Format

Use the template in `docs/templates/LESSONS_TEMPLATE.md` for detailed entries.

For quick additions to this master document:
1. Choose appropriate section
2. Give it a descriptive title
3. Explain what you learned
4. Explain why it matters
5. Provide examples if helpful

## Related Documents

- [Weekly Lessons](docs/lessons/WEEKLY.md) - Detailed week-by-week learnings
- [Challenges](docs/lessons/CHALLENGES.md) - Major obstacles and solutions
- [SQL:1999 Gotchas](docs/lessons/GOTCHAS.md) - Edge cases and surprises
- [Decisions](DECISIONS.md) - Architecture decision records
- [Research Summary](RESEARCH_SUMMARY.md) - Planning phase findings

---

**Last Updated**: 2024-10-25
**Phase**: Planning and Research
**Next Update**: After first implementation milestone

**Remember**: Every challenge is a learning opportunity. Document what you learn so we can all benefit!
