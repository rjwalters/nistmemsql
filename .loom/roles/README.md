# Loom Role Definitions

This directory contains role definition templates for different terminal roles in Loom.

## ⚠️ Important: Source of Truth

**This directory is the single source of truth for all Loom role definitions.**

- **Edit role files HERE** (in `defaults/roles/*.md`)
- **Do NOT edit** `.claude/agents/*.md` files directly - they are auto-generated
- **After editing**, run `pnpm generate:agents` to regenerate Claude Code agent files

### Why This Pattern?

Claude Code agents cannot reference external files - they must be self-contained. To solve this:

1. **Source**: `defaults/roles/*.md` (edit these)
2. **Generate**: Run `pnpm generate:agents`
3. **Result**: `.claude/agents/*.md` (YAML frontmatter + role content)

This prevents duplication errors and ensures `.loom/roles/` is always the authoritative source.

## Available Prompts

Each prompt consists of two files:
- **`.md`** - The role definition text (markdown format)
- **`.json`** - Metadata with default settings (optional)

### Prompt Roles

- **`default.md`** - Plain shell environment, no specialized role
- **`worker.md`** - General development worker for features, bugs, and refactoring
- **`reviewer.md`** - Code review specialist for thorough PR reviews
- **`architect.md`** - System architecture and technical decision making
- **`curator.md`** - Issue maintenance and quality improvement
- **`critic.md`** - Critical analysis and architectural review specialist
- **`fixer.md`** - Bug fixing and PR maintenance specialist
- **`triage.md`** - Issue prioritization and focus management

## Usage

When configuring a terminal role in the Terminal Settings modal, select a prompt file from the dropdown. The prompt will be loaded and the `{{workspace}}` variable will be replaced with your workspace path.

## Creating Custom Prompts

You can add your own prompt files to `.loom/roles/` in any workspace. All `.md` files will automatically appear in the prompt selection dropdown.

### Prompt File Structure

Each prompt can have two files:

**`my-prompt.md`** (required) - The role definition text
```markdown
# My Custom Role

You are a specialist in {{workspace}} repository...

## Your Role
...
```

**`my-prompt.json`** (optional) - Metadata with default settings
```json
{
  "name": "My Custom Role",
  "description": "Brief description of what this role does",
  "defaultInterval": 300000,
  "defaultIntervalPrompt": "The prompt to send at each interval...",
  "autonomousRecommended": true,
  "suggestedWorkerType": "claude"
}
```

### Metadata Fields

- **`name`** (string): Display name for this role
- **`description`** (string): Brief description
- **`defaultInterval`** (number): Default interval in milliseconds (0 = disabled)
- **`defaultIntervalPrompt`** (string): Default prompt sent at each interval
- **`autonomousRecommended`** (boolean): Whether autonomous mode is recommended
- **`suggestedWorkerType`** (string): "claude" or "codex"

When a user selects a prompt from the dropdown, if a metadata file exists, the form fields will be pre-populated with these defaults.

### Template Variables

- `{{workspace}}` - Replaced with the absolute path to the workspace directory

### Prompt Structure

A good prompt should include:

1. **Role Definition**: Clear description of the terminal's purpose
2. **Responsibilities**: What tasks this role handles
3. **Guidelines**: Best practices and working style
4. **Examples**: Sample workflows or outputs (when helpful)

## Default vs Workspace Prompts

- **`defaults/roles/`** (this directory): Committed to git, serves as examples and fallbacks
- **`.loom/roles/`** (in each workspace): Gitignored, workspace-specific customizations

When a prompt file exists in both locations, the workspace version takes precedence.
