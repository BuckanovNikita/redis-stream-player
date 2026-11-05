# redis-stream-player (boomrdbox)

`boomrdbox` records Redis stream messages to msgpack files and replays them.
Python >= 3.12, uv, strict mypy, ruff `select = ["ALL"]`, pytest. Package source:
`src/boomrdbox/`; console script `boomrdbox = "boomrdbox.cli:main"`.

## Completion gate

After any code change, run all of these and do not report completion until they pass:

```bash
uv run ruff format .
uv run ruff check .
uv run lint-imports
uv run mypy .
uv run vulture
uv run pytest
```

`uv run pre-commit run --all-files` runs the same six hooks
(`.pre-commit-config.yaml`); run it before every commit. Git hooks are not
assumed to be installed, so run the gate explicitly.
Do not bypass hooks (`--no-verify`) or stash unrelated changes to make checks pass:
fix the failure, or report the blocker and, if needed, run the checks in a separate
worktree. To commit or push, use the `commit` skill.

## Development commands

```bash
uv sync --dev                                   # install
uv run pytest                                   # full suite (-n auto, coverage >= 80 %)
uv run pytest tests/test_foo.py --no-cov        # single file (partial runs fail the coverage gate without --no-cov)
uv run pytest -k "test_name" --no-cov           # single test by name
uv run ruff format .                            # auto-format (run first)
uv run ruff check . --fix                       # auto-fix lint issues
uv run pytest -m integration                    # needs `docker compose up -d redis` (port 6389); skips if Redis is absent
bash integration/integration_test.sh            # Docker end-to-end: fill, record, truncate, play, verify
```

## CLI commands

- `record` — capture Redis stream messages to msgpack file
- `play` — replay recorded messages (writes to Redis, gated by host whitelist)
- `convert` — export msgpack to Parquet/CSV
- `truncate` — slice recordings by message ID range (supports interactive TUI)
- `info` — show per-stream statistics
- `setup` — manage read instances and play-host whitelist (uses argparse, not Hydra)

## Key subsystems

- **Play safety**: `player.py` validates `redis.host` against a whitelist loaded by
  `instances.load_allowed_play_hosts()` from `platformdirs.user_config_dir("boomrdbox")/config.toml`
  (`~/.config/boomrdbox/config.toml` on Linux). Default whitelist: `["redis"]`. When adding
  new behavior that gates `Player.run()` or `Recorder.run()`, update existing tests to mock the
  new gate — otherwise they break on `host='localhost'`.
- **Read instances**: Persistent named Redis connections with optional SSH tunnels, stored in
  `config.toml` (same location as above). SSH tunnels are an `ssh -L` subprocess
  (`io.create_tunneled_redis`). Used by `record` command via `instance=<name>`. Never used by `play`.
- **Config layers**: Hydra-zen programmatic config (`_config.py`) for Hydra commands; argparse for
  `setup` command; persistent TOML for user-defined instances and play-host whitelist.

## Architecture

### Layered imports (enforced by import-linter in pyproject.toml)

```
boomrdbox.cli                          <- CLI entry point
boomrdbox._config | .recorder | .player | .tools | .tui  <- Application layer
boomrdbox.io | .instances              <- Infrastructure layer
boomrdbox.models                       <- Domain layer (pure types, no deps)
```

When adding a new module:
1. Place it in the correct layer
2. Update `[[tool.importlinter.contracts]]` in `pyproject.toml` — both "Layered architecture" layers AND "Domain isolation" forbidden_modules
3. Add per-file-ignores in `[tool.ruff.lint.per-file-ignores]` if needed (e.g., `PLC0415` for lazy imports)

### Ruff `select = ["ALL"]` — common pitfalls

This project enables ALL ruff rules. When writing new code, watch for:
- **PLC0415** — imports inside a function (lazy imports). Already ignored for `cli.py`, `tools.py`, `tui.py` and `tests/**`; add a per-file ignore elsewhere only if intentional.
- **PLR0915** — too many statements in a function (>50). Extract helpers or add to per-file-ignores.
- **PT019** — `_mock_*` params in tests that come from `@patch` decorators. Already suppressed in `tests/**`.
- **S105/S106** — hardcoded passwords in test code. Already suppressed in `tests/**`.
- **D102** — missing docstrings on public methods. Add docstrings or suppress per-file.

### Test mocking rules

- Player tests MUST mock `boomrdbox.player.load_allowed_play_hosts` (return `["localhost"]`) when calling `Player.run()`, because default whitelist is `["redis"]` and test fixtures use `host='localhost'`.
- Recorder tests that use `instance=` MUST mock `boomrdbox.recorder.get_instance` and `boomrdbox.recorder.create_tunneled_redis`.
- Instance tests MUST mock `boomrdbox.instances.get_config_path` to use `tmp_path` and avoid writing to real user config.

## Git

- Use SSH (not HTTPS) for git push operations. Remote URL format: `git@github.com:<org>/<repo>.git`
- Use Conventional Commits (feat, fix, refactor, test, docs, chore); the `commit` skill has the full workflow.

## Documentation language

- `README.md` is written in Russian; agent instructions and skills are written in English.
