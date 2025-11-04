# Rebranding Plan: nistmemsql → vibesql

**Status**: Planned (not yet executed)
**Target Name**: vibesql
**Created**: 2025-10-27
**Reason**: Detach from upstream posix4e repo and establish independent project identity

---

## 🎯 Motivation

This project has evolved beyond a fork of posix4e/nistmemsql:
- Independent development trajectory
- Loom AI orchestration as core methodology
- "Inflection point" experiment focus
- Unique branding opportunity: **"vibesql"** - SQL database built by vibes (AI-powered development)

**Goal**: Rebrand while preserving:
- ✅ All commit history
- ✅ Credit to @posix4e for original challenge
- ✅ Historical references in documentation
- ✅ Technical foundation and architecture

---

## 📋 Rebrand Checklist

### Phase 1: Git Configuration

**Remove upstream remote**:
```bash
git remote remove upstream
```

**Verify**:
```bash
git remote -v
# Should only show:
# origin  https://github.com/rjwalters/vibesql.git (fetch)
# origin  https://github.com/rjwalters/vibesql.git (push)
```

---

### Phase 2: Package Names

#### Cargo.toml (Root)
```toml
[package]
name = "vibesql"
authors = ["vibesql contributors"]
repository = "https://github.com/rjwalters/vibesql"
```

#### Crate Names (7 total)
Update `Cargo.toml` in each crate directory:

**Option A: Full Rebrand**
- `nistmemsql-parser` → `vibesql-parser`
- `nistmemsql-executor` → `vibesql-executor`
- `nistmemsql-storage` → `vibesql-storage`
- `nistmemsql-types` → `vibesql-types`
- `nistmemsql-planner` → `vibesql-planner`
- `nistmemsql-wasm` → `vibesql-wasm`
- `nistmemsql-cli` → `vibesql-cli`

**Option B: Keep Internal Names** (simpler)
- Only rename root package to `vibesql`
- Keep crate names as-is (they're internal)
- Only affects CLI binary name

**Recommendation**: Option B for simplicity

#### Binary Executable
```toml
# In CLI crate Cargo.toml
[[bin]]
name = "vibesql"  # was: nistmemsql
path = "src/main.rs"
```

---

### Phase 3: Documentation Updates

#### README.md

**Current references to update**:
```markdown
# OLD
- Aligns with [upstream posix4e/nistmemsql](https://github.com/posix4e/nistmemsql) vision

# NEW
- Inspired by the [posix4e/nistmemsql challenge](https://github.com/posix4e/nistmemsql)
```

**Backstory section** (keep @posix4e credit):
```markdown
# Keep this - it's historical context
[@posix4e](https://github.com/posix4e) was skeptical...

# Add attribution at end
- Inspired by the [posix4e/nistmemsql challenge](https://github.com/posix4e/nistmemsql)
```

**Add new branding section**:
```markdown
## 🎵 Why "vibesql"?

This database is built entirely through AI-powered development—coding by vibes, if you will.
It's a demonstration of what's possible when you combine:
- Clear specifications (SQL:1999 standard)
- Powerful AI assistance (Claude Code)
- Orchestration tooling (Loom)
- Relentless iteration (AI won't burn out)

The name captures both the experimental nature and the AI-first methodology.
```

#### WORK_PLAN.md

**Remove**:
```markdown
**Alignment**: Original posix4e/nistmemsql vision
```

**Replace with**:
```markdown
**Origin**: Inspired by posix4e/nistmemsql challenge
**Methodology**: AI-powered development via Loom orchestration
```

#### REQUIREMENTS.md

**Keep historical links** (they're valuable context):
```markdown
**Source**: [Issue #1](https://github.com/posix4e/nistmemsql/issues/1)
```

**Add header note**:
```markdown
> **Note**: These requirements originated from the posix4e/nistmemsql challenge.
> This project (vibesql) has since evolved independently with AI-first methodology.
```

#### CLAUDE.md (Loom config)

**Update project name references**:
```markdown
# OLD
You are working in the {{workspace}} repository (nistmemsql)...

# NEW
You are working in the {{workspace}} repository (vibesql)...
```

---

### Phase 4: Web Demo

#### package.json
```json
{
  "name": "vibesql-web-demo",
  "description": "Interactive SQL demo for vibesql database"
}
```

#### HTML/Meta Tags
```html
<title>vibesql - NIST SQL:1999 Database</title>
<meta name="description" content="vibesql: AI-powered SQL:1999 database">
```

#### Domain/Deployment
- GitHub Pages: `rjwalters.github.io/vibesql` (if repo renamed)
- Update deployment workflows if needed

---

### Phase 5: GitHub Repository

**Rename via GitHub UI**:
1. Go to: Settings → General → Repository name
2. Change: `nistmemsql` → `vibesql`
3. GitHub automatically redirects old URL
4. Update local remote: `git remote set-url origin https://github.com/rjwalters/vibesql.git`

**Update repository settings**:
- Description: "AI-powered SQL:1999 database - built by vibes"
- Topics: Add `vibesql`, `ai-development`, `loom-orchestration`
- Social preview: Consider custom image with vibesql branding

---

### Phase 6: CI/CD & Deployments

#### GitHub Actions
Check `.github/workflows/` for hardcoded references:
```yaml
# deploy-demo.yml
name: Deploy vibesql Demo
```

#### Badges (README.md)
```markdown
[![Demo](https://img.shields.io/badge/demo-live-success)](https://rjwalters.github.io/vibesql/)
```

---

## 🔍 Files to Update (Complete List)

### Must Update
- [ ] `Cargo.toml` (root) - package name, repository URL
- [ ] `README.md` - title, badges, links, add branding section
- [ ] `WORK_PLAN.md` - remove "alignment", add "origin"
- [ ] `CLAUDE.md` - workspace references
- [ ] `web-demo/package.json` - package name
- [ ] `web-demo/index.html` - title, meta tags
- [ ] `.github/workflows/*.yml` - workflow names, URLs
- [ ] GitHub repo settings - repository name

### Should Update
- [ ] `REQUIREMENTS.md` - add origin note
- [ ] CLI help text - update project name in `--help` output
- [ ] Error messages - check for hardcoded project name
- [ ] Documentation files - scan for references

### Keep As-Is (Historical)
- ✅ Git commit history (untouched)
- ✅ Issue/PR references to posix4e repo (historical context)
- ✅ @posix4e credit in backstory (proper attribution)

---

## ✅ Testing Verification

After rebrand, verify:

```bash
# 1. Package builds
cargo build --workspace

# 2. Tests pass
cargo test --workspace

# 3. Binary name correct
cargo build --release
./target/release/vibesql --version

# 4. WASM builds
cd web-demo
pnpm run build

# 5. GitHub Actions work
# Push and verify workflows execute

# 6. Links work
# Check all documentation links aren't broken
```

---

## 🔄 Rollback Plan

If issues arise:

```bash
# Restore upstream remote
git remote add upstream https://github.com/posix4e/nistmemsql.git

# Revert Cargo.toml changes
git checkout HEAD~1 Cargo.toml

# Revert documentation
git checkout HEAD~1 README.md WORK_PLAN.md

# Rename GitHub repo back (via UI)
```

---

## 📅 Execution Timeline

**Recommended approach**: All at once in a single PR

**Steps**:
1. Create feature branch: `git checkout -b rebrand/vibesql`
2. Execute Phase 1-4 changes
3. Commit: "rebrand: nistmemsql → vibesql"
4. Test thoroughly
5. Create PR for review
6. Merge to main
7. Execute Phase 5 (GitHub rename) manually
8. Update local clones

**Estimated time**: 2-3 hours

---

## 🎨 Branding Opportunities

Once rebranded, consider:

**Logo/Visual Identity**:
- Musical note + database icon?
- Waveform pattern?
- "Built by vibes" tagline

**Marketing**:
- Blog post: "How we built a SQL database by vibes"
- Twitter thread: Day-by-day progress
- HN post: "Show HN: vibesql - AI-built SQL:1999 database"

**Domain** (optional):
- `vibesql.dev` or `vibesql.io`
- Redirect to GitHub Pages

---

## 📝 Notes

**Why "vibesql"?**
- Memorable and unique
- Captures AI-powered development methodology
- Fun and approachable (vs technical "nistmemsql")
- Good for marketing/sharing
- Domain availability likely higher

**Preserving Attribution**:
- @posix4e deserves credit for the original challenge
- Historical context is valuable
- Keep all references to challenge origins
- Update language from "fork" to "inspired by"

**Independence Benefits**:
- Clear project identity
- No confusion with upstream
- Freedom to diverge architecturally
- Easier to market as unique experiment

---

**When ready to execute**: Follow phases 1-6 sequentially, test thoroughly, and update this document with actual results.

**Generated with [Claude Code](https://claude.com/claude-code)**
