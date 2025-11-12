# Publishing Checklist for v0.1.0

This document guides you through the steps needed before publishing vibesql to crates.io.

## 📋 Pre-Publication Checklist

### ✅ Completed Items

- [x] Rename all crates with `vibesql-*` namespace
- [x] Add version requirements to all internal dependencies
- [x] Add publishing metadata to all Cargo.toml files
- [x] Update main crate documentation (src/lib.rs)
- [x] Verify all crate names available on crates.io
- [x] Create publish script (`scripts/publish-crates.sh`)
- [x] Test dry-run of individual crates
- [x] README.md exists and is comprehensive

### ❌ Missing Critical Items

#### 1. **LICENSE Files** (REQUIRED)
You specified `license = "MIT OR Apache-2.0"` but the files don't exist.

**Action needed:**
```bash
# Create MIT license
cat > LICENSE-MIT << 'EOF'
MIT License

Copyright (c) 2024 vibesql contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF

# Create Apache 2.0 license
curl https://www.apache.org/licenses/LICENSE-2.0.txt > LICENSE-APACHE
```

#### 2. **CHANGELOG.md** (Highly Recommended)
Document what's included in v0.1.0.

**Action needed:**
Create `CHANGELOG.md` with initial release notes.

### 🔍 Optional But Recommended

#### 3. **Examples Documentation**
- [ ] Verify all examples in `examples/` directory work
- [ ] Add example documentation to README or separate EXAMPLES.md

#### 4. **Security Policy**
- [ ] Create SECURITY.md with vulnerability reporting process

#### 5. **Contributing Guidelines**
- [ ] Create CONTRIBUTING.md for contributors

#### 6. **Final Testing**
- [ ] Run full test suite: `cargo test --all`
- [ ] Run clippy: `cargo clippy --all-targets --all-features`
- [ ] Build documentation: `cargo doc --no-deps --all`
- [ ] Test examples: `cargo build --examples`

### 📦 Publishing Process

Once checklist is complete:

1. **Create and push git tag:**
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

2. **Get crates.io API token:**
   - Visit https://crates.io/settings/tokens
   - Create new token
   - Run: `cargo login <token>`

3. **Final dry-run:**
   ```bash
   ./scripts/publish-crates.sh
   ```

4. **Publish to crates.io:**
   ```bash
   ./scripts/publish-crates.sh --publish
   ```

5. **Create GitHub Release:**
   - Go to: https://github.com/rjwalters/vibesql/releases/new
   - Select tag: v0.1.0
   - Add release notes from CHANGELOG.md
   - Attach any binaries if applicable

### ⚠️ Important Notes

- **Versions are permanent**: Once published to crates.io, you cannot delete or modify a version
- **You can yank**: If there's a critical issue, you can yank the version (it won't be deleted but won't be recommended)
- **Documentation is auto-generated**: docs.rs will automatically build and host your documentation
- **License files are required**: crates.io requires actual LICENSE files, not just metadata

### 🚀 Post-Publication

After successful publication:

- [ ] Update README badges with crates.io version
- [ ] Announce on social media / relevant forums
- [ ] Update project documentation to reference crates.io
- [ ] Consider writing a blog post about the release

## Current Status

**Status**: ⚠️ **NOT READY** - Missing required LICENSE files

**Blockers:**
1. LICENSE-MIT file missing
2. LICENSE-APACHE file missing

**Next Steps:**
1. Add license files
2. Create CHANGELOG.md
3. Run final tests
4. Tag v0.1.0
5. Publish!
