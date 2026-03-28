# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`boomrdbox` is a Python project for recording and replaying Redis stream data. Primary language is Python with strict typing.

### CLI commands

- `record` — capture Redis stream messages to msgpack file
- `play` — replay recorded messages (writes to Redis, gated by host whitelist)
- `convert` — export msgpack to Parquet/CSV
- `truncate` — slice recordings by message ID range (supports interactive TUI)
- `info` — show per-stream statistics
- `setup` — manage read instances and play-host whitelist (uses argparse, not Hydra)

### Key subsystems

- **Play safety**: `player.py` validates `redis.host` against a whitelist loaded from `~/.config/boomrdbox/config.toml` via `instances.py`. Default whitelist: `["redis"]`. When adding new behavior that gates `Player.run()` or `Recorder.run()`, update existing tests to mock the new gate — otherwise they break on `host='localhost'`.
- **Read instances**: Persistent named Redis connections with optional SSH tunnels, stored in `~/.config/boomrdbox/config.toml`. Used by `record` command via `instance=<name>`. Never used by `play`.
- **Config layers**: Hydra-zen programmatic config (`_config.py`) for Hydra commands; argparse for `setup` command; persistent TOML for user-defined instances and play-host whitelist.

## Development Commands

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest                       # full suite
uv run pytest tests/test_foo.py     # single file
uv run pytest -k "test_name"        # single test by name

# Linting and type checking
uv run ruff format .                # auto-format (run first)
uv run ruff check .                 # linter
uv run ruff check . --fix           # auto-fix lint issues
uv run mypy .                       # strict type checking

# Pre-commit hooks
uv run pre-commit run --all-files   # run before committing
```

## Architecture

### Layered imports (enforced by import-linter in pyproject.toml)

```
boomrdbox.cli                          ← CLI entry point
boomrdbox._config | .recorder | .player | .tools | .tui  ← Application layer
boomrdbox.io | .instances              ← Infrastructure layer
boomrdbox.models                       ← Domain layer (pure types, no deps)
```

When adding a new module:
1. Place it in the correct layer
2. Update `[[tool.importlinter.contracts]]` in `pyproject.toml` — both "Layered architecture" layers AND "Domain isolation" forbidden_modules
3. Add per-file-ignores in `[tool.ruff.lint.per-file-ignores]` if needed (e.g., `PLC0415` for lazy imports)

### Ruff `select = ["ALL"]` — common pitfalls

This project enables ALL ruff rules. When writing new code, watch for:
- **PLC0415** — lazy imports (e.g., `from sshtunnel import ...` inside a function). Add to per-file-ignores if intentional.
- **PLR0915** — too many statements in a function (>50). Extract helpers or add to per-file-ignores.
- **PT019** — `_mock_*` params in tests that come from `@patch` decorators. Already suppressed in `tests/**`.
- **S105/S106** — hardcoded passwords in test code. Already suppressed in `tests/**`.
- **D102** — missing docstrings on public methods. Add docstrings or suppress per-file.

### Test considerations

- Player tests MUST mock `boomrdbox.player.load_allowed_play_hosts` (return `["localhost"]`) when calling `Player.run()`, because default whitelist is `["redis"]` and test fixtures use `host='localhost'`.
- Recorder tests that use `instance=` MUST mock `boomrdbox.recorder.get_instance` and `boomrdbox.recorder.create_tunneled_redis`.
- Instance tests MUST mock `boomrdbox.instances.get_config_path` to use `tmp_path` and avoid writing to real user config.

## Workflow Conventions

- When asked to create a plan, stay in plan mode and do NOT start implementing code unless explicitly asked to implement. Planning and implementation are separate phases.
- After any refactoring or code edit, run the full test suite (`pytest`) and all linters (`ruff check .`, `mypy .`) before reporting completion. Do not consider a task done until all checks pass.
- This project uses Python with strict typing (mypy strict mode), ruff for linting, and pytest for testing.

## Git / Commit Conventions

- Always run `pre-commit run --all-files` before committing. If pre-commit hooks fail due to unstaged changes, stash first with `git stash`, run hooks, then `git stash pop`. Use `--no-verify` only as last resort.
- Use SSH (not HTTPS) for git push operations. Remote URL format: `git@github.com:<org>/<repo>.git`

## Documentation guidelines
1. All docs must be in russian.