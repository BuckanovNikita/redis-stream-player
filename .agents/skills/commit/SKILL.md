---
name: commit
description: >-
  Commits, and when asked pushes, a reviewed set of redis-stream-player (boomrdbox)
  changes. Runs the repository gate (`uv run pre-commit run --all-files`: ruff
  format, ruff check, import-linter, mypy, vulture, pytest), updates affected
  documentation, stages only the intended paths, writes a Conventional Commit and
  pushes over SSH. Use for "commit", "push" or "/commit" requests in this repository.
---

# Commit Workflow

Copy this checklist and work through it in order:

- [ ] 1. Establish the change set
- [ ] 2. Format and autofix
- [ ] 3. Run the gate
- [ ] 4. Fix failures and repeat
- [ ] 5. Update documentation
- [ ] 6. Stage and commit
- [ ] 7. Push (only when requested)

## 1. Establish the change set

Run `git status --short --branch`, `git diff --stat` and `git diff --cached`. Identify the
intended paths and leave unrelated changes untouched.

## 2. Format and autofix

```bash
uv run ruff format .
uv run ruff check --fix .
```

## 3. Run the gate

```bash
uv run pre-commit run --all-files
```

This runs six hooks: ruff format, ruff check, import-linter (`lint-imports`), mypy, vulture
and pytest (`-n auto`).

- If ruff format changed files, review them, re-add them and re-run.
- If unrelated unstaged changes break a hook, do not stash. Report the blocker, or run the
  gate in a separate `git worktree`.

## 4. Fix failures and repeat

- **ruff**: `uv run ruff check --fix .`, then fix by hand; add a per-file ignore only when the code is intentional
- **mypy**: `strict = true`; all code must be fully typed
- **import-linter**: respect the layers in `pyproject.toml` `[[tool.importlinter.contracts]]`:
  `cli` -> `_config | recorder | player | tools | tui` -> `io | instances` -> `models`;
  `models` imports nothing from the other layers. For a new module, update both contracts
  (see `AGENTS.md`, Architecture)
- **vulture**: remove dead code. For a confirmed false positive, add a whitelist module and
  include it in `[tool.vulture].paths`; none exists yet
- **pytest**: tests run in parallel (`-n auto`) with coverage >= 80 %; fix failing tests before committing

Re-run `uv run pre-commit run --all-files` until all hooks pass.

## 5. Update documentation

Review the change and update `README.md` (Russian) and the agent instructions (`AGENTS.md`,
this skill) wherever behavior, commands or configuration changed.

## 6. Stage and commit

```bash
git add <paths>
git commit -m "<type>: <description>"
```

Stage the intended paths explicitly. Use [Conventional Commits](https://www.conventionalcommits.org/):
- `feat:` — new feature
- `fix:` — bug fix
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `test:` — adding or updating tests
- `docs:` — documentation only
- `chore:` — maintenance (deps, config, CI)

Keep subject line concise. Use body for details when needed.

## 7. Push (only when requested)

```bash
git push
```

The remote must use SSH format (`git@github.com:<org>/<repo>.git`). If it is HTTPS, fix with:

```bash
git remote set-url origin git@github.com:<org>/<repo>.git
```

## Never bypass hooks

Do not use `--no-verify`. Fix the failure or report it.
