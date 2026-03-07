# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`boomrdbox` is a Python project for replaying Redis stream data. Primary language is Python with strict typing.

## Development Commands

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest                       # full suite
uv run pytest tests/test_foo.py     # single file
uv run pytest -k "test_name"        # single test by name

# Linting and type checking
uv run ruff check .                 # linter
uv run ruff check . --fix           # auto-fix lint issues
uv run mypy .                       # strict type checking

# Pre-commit hooks
uv run pre-commit run --all-files   # run before committing
```

## Workflow Conventions

- When asked to create a plan, stay in plan mode and do NOT start implementing code unless explicitly asked to implement. Planning and implementation are separate phases.
- After any refactoring or code edit, run the full test suite (`pytest`) and all linters (`ruff check .`, `mypy .`) before reporting completion. Do not consider a task done until all checks pass.
- This project uses Python with strict typing (mypy strict mode), ruff for linting, and pytest for testing.

## Git / Commit Conventions

- Always run `pre-commit run --all-files` before committing. If pre-commit hooks fail due to unstaged changes, stash first with `git stash`, run hooks, then `git stash pop`. Use `--no-verify` only as last resort.
- Use SSH (not HTTPS) for git push operations. Remote URL format: `git@github.com:<org>/<repo>.git`

## Documentation guidelines
1. All docs must be in russian.