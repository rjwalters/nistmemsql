# Lessons Learned: [Topic or Date Range]

**Period**: [e.g., Week of YYYY-MM-DD, or specific milestone]

**Status**: Draft | Final

**Last Updated**: YYYY-MM-DD

## Summary

[2-3 sentence summary of the main lessons from this period or topic]

## What Went Well ✅

### Success 1: [Title]

**What happened**: [Brief description]

**Why it worked**: [What made this successful]

**Key takeaway**: [Lesson to remember]

**Apply to**: [Where else can we use this approach]

Example:
```
### Success 1: Using sqllogictest-rs

**What happened**: Integrated sqllogictest-rs in 2 hours, immediately got 10,000 tests running

**Why it worked**:
- Well-documented Rust library
- Clear examples in repo
- Simple integration API

**Key takeaway**: Leverage existing test frameworks rather than building from scratch

**Apply to**: Look for more existing tools we can integrate (ODBC libraries, etc.)
```

### Success 2: [Title]
[Continue pattern]

## What Went Wrong ❌

### Challenge 1: [Title]

**What happened**: [Brief description of the problem]

**Root cause**: [Why did this happen?]

**Impact**: [How did this affect us?]

**How we solved it**: [Solution implemented]

**Prevention**: [How to avoid this in the future]

Example:
```
### Challenge 1: SQL Grammar Ambiguity

**What happened**: Parser couldn't distinguish between function calls and column references in some contexts

**Root cause**: SQL:1999 grammar is inherently ambiguous; requires lookahead or semantic analysis

**Impact**: 50+ test failures, 2 days debugging parser issues

**How we solved it**: Added semantic analysis phase after parsing to resolve ambiguities based on catalog information

**Prevention**:
- Always check SQL:1999 spec for ambiguity notes
- Test edge cases early
- Consider semantic analysis from the start for context-sensitive languages
```

### Challenge 2: [Title]
[Continue pattern]

## Technical Insights 💡

### Insight 1: [Discovery]

**What we learned**: [The insight]

**Why it matters**: [Implications]

**Example**: [Code or scenario demonstrating this]

### Insight 2: [Discovery]
[Continue pattern]

## SQL:1999 Surprises 🤔

### Surprise 1: [Unexpected spec behavior]

**What we expected**: [Our assumption]

**What the spec says**: [Actual requirement]

**Example**:
```sql
-- We thought this would work:
SELECT * FROM t WHERE b = TRUE;

-- But SQL:1999 requires IS:
SELECT * FROM t WHERE b IS TRUE;
```

**Impact**: [How this affects implementation]

**Reference**: [SQL:1999 spec section]

### Surprise 2: [Another gotcha]
[Continue pattern]

## Performance Learnings 📊

### Finding 1: [Performance discovery]

**Observation**: [What we measured]

**Cause**: [Why performance was as it was]

**Optimization**: [If applicable, what we did]

**Result**: [Impact of optimization]

Example:
```
### Finding 1: Hash Join Faster Than Nested Loop for Large Tables

**Observation**: Queries with joins on large tables (>1000 rows) were slow

**Cause**: Nested loop join is O(n*m), hash join is O(n+m)

**Optimization**: Implemented hash join for equality predicates

**Result**: 100x speedup on large joins (1000ms → 10ms)
```

## Testing Insights 🧪

### Discovery 1: [Testing-related learning]

**What we learned**: [The lesson]

**Application**: [How to apply this]

### Discovery 2: [Another testing insight]
[Continue pattern]

## Mistakes Made (And How to Avoid Them) 🚫

### Mistake 1: [What we did wrong]

**What happened**: [Description]

**Why it was a mistake**: [The problem]

**Cost**: [Time/effort wasted]

**Correct approach**: [What we should have done]

**Red flags to watch for**: [Early warning signs]

### Mistake 2: [Another mistake]
[Continue pattern]

## Questions Still Open ❓

### Question 1: [Unresolved question]

**Context**: [Why this matters]

**Options being considered**:
1. [Option 1]
2. [Option 2]

**Next steps**: [How we'll resolve this]

### Question 2: [Another open question]
[Continue pattern]

## Tools and Resources Discovered 🛠️

### Tool 1: [Name]

**URL**: [Link]

**What it does**: [Description]

**How we're using it**: [Application]

**Rating**: ⭐⭐⭐⭐⭐ (and why)

### Resource 1: [Documentation, paper, blog post, etc.]

**Link**: [URL]

**Key insights**: [What we learned]

**Recommended for**: [Who should read this]

## Code Patterns That Work ✨

### Pattern 1: [Name]

**Problem it solves**: [Scenario]

**Implementation**:
```rust
// Show the pattern
```

**When to use**: [Guidelines]

**When NOT to use**: [Anti-patterns]

### Pattern 2: [Another pattern]
[Continue pattern]

## Productivity Tips 🚀

### Tip 1: [Productivity enhancement]

**What**: [The tip]

**Why it helps**: [Benefits]

**How to apply**: [Steps]

### Tip 2: [Another tip]
[Continue pattern]

## Team Communication Notes 💬

[If working with others - what communication worked well? What could improve?]

## AI Assistance Insights 🤖

[For Claude Code specifically]

### What Worked Well
* [Effective use of AI]
* [Good prompts or strategies]

### What Could Improve
* [Areas where AI assistance was less effective]
* [Better ways to leverage AI]

## Looking Ahead 🔮

### Apply These Lessons To:
* [Upcoming feature/component]
* [Next phase of work]

### Watch Out For:
* [Potential pitfalls based on lessons]
* [Areas needing extra attention]

## Action Items

Based on these lessons:

- [ ] [Specific action to take]
- [ ] [Another action item]
- [ ] [Document to update]

## References

* [Links to related code, docs, issues]
* [External resources that helped]

## Contributors

[Who contributed to this learning/these insights?]

---

## Meta Notes

[Any notes about the documentation or learning process itself]
