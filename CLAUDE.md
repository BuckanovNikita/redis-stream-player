# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`redis-stream-player` is a Python project for replaying Redis stream data. Primary language is Python with strict typing.

## Development Commands

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest                       # full suite
pytest tests/test_foo.py     # single file
pytest -k "test_name"        # single test by name

# Linting and type checking
ruff check .                 # linter
ruff check . --fix           # auto-fix lint issues
mypy .                       # strict type checking

# Pre-commit hooks
pre-commit run --all-files   # run before committing
```

## Workflow Conventions

- When asked to create a plan, stay in plan mode and do NOT start implementing code unless explicitly asked to implement. Planning and implementation are separate phases.
- After any refactoring or code edit, run the full test suite (`pytest`) and all linters (`ruff check .`, `mypy .`) before reporting completion. Do not consider a task done until all checks pass.
- This project uses Python with strict typing (mypy strict mode), ruff for linting, and pytest for testing. YAML configs and Markdown docs are also common.

## Git / Commit Conventions

- Always run `pre-commit run --all-files` before committing. If pre-commit hooks fail due to unstaged changes, stash first with `git stash`, run hooks, then `git stash pop`. Use `--no-verify` only as last resort.
- Use SSH (not HTTPS) for git push operations. Remote URL format: `git@github.com:<org>/<repo>.git`
