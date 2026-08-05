<!-- TRELLIS:START -->
# Trellis Instructions

These instructions are for AI assistants working in this project.

This project is managed by Trellis. The working knowledge you need lives under `.trellis/`:

- `.trellis/workflow.md` — development phases, when to create tasks, skill routing
- `.trellis/spec/` — package- and layer-scoped coding guidelines (read before writing code in a given layer)
- `.trellis/workspace/` — per-developer journals and session traces
- `.trellis/tasks/` — active and archived tasks (PRDs, research, jsonl context)

If a Trellis command is available on your platform (e.g. `/trellis:finish-work`, `/trellis:continue`), prefer it over manual steps. Not every platform exposes every command.

If you're using Codex or another agent-capable tool, additional project-scoped helpers may live in:
- `.agents/skills/` — reusable Trellis skills
- `.codex/agents/` — optional custom subagents

Managed by Trellis. Edits outside this block are preserved; edits inside may be overwritten by a future `trellis update`.

<!-- TRELLIS:END -->

<!-- PROJECT:RULES:START -->
# Project Rules (kept outside the Trellis-managed block on purpose)

## Commit Message Convention (REQUIRED)

All commits MUST use the Conventional Commits format, written in **English**:

```
<type>(<scope>): <imperative subject, lowercase, <=72 chars>

<body: what & why, wrapped at 72 chars>

<footer: BREAKING CHANGE / issue refs>
```

**Types** (pick the narrowest match):

- `feat` — new feature or capability
- `fix` — bug fix
- `perf` — performance improvement (no behavior change)
- `refactor` — code restructure without behavior change
- `test` — add or fix tests only
- `docs` — documentation only
- `chore` — build, tooling, config, dependencies
- `build` — build system / packaging changes
- `ci` — CI configuration changes
- `style` — formatting, whitespace, lint fixes (no behavior change)

**Rules:**

- Subject is imperative ("Add X", "Fix Y", never "Added X" or "Adds X").
- Subject is lowercase except for proper nouns (contract names, tokens).
- No trailing period in the subject.
- Scope is optional but preferred when it narrows the change: `feat(dex/v3): ...`, `fix(config): ...`, `docs(m0): ...`.
- Body explains **why**, not just what; wrap at 72 columns.
- One logical change per commit; split unrelated changes.
- Write commit messages directly with `git commit -m`/`-F` — never delegate message drafting to an LLM verbatim without reviewing it against these rules.

Examples:

```
feat(dex/v3): add UniversalRouter command encoding

V3 pools on Robinhood use a custom bytecode, so the standard
SwapRouter is absent. Encode V3_SWAP_EXACT_IN (0x08) via
UniversalRouter.execute(commands, inputs) instead.

fix(chain): reject non-ws URLs in NewWSClient

go-ethereum DialContext silently accepts https:// URLs as HTTP
clients, causing "notifications not supported" at subscribe time.
```

## Language

- Commit messages, PRs, code identifiers, and comments: **English**.
- User-facing docs may stay bilingual, but code-facing artifacts are English.
<!-- PROJECT:RULES:END -->
