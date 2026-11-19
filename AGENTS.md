# redis-stream-player (boomrdbox)

`boomrdbox` records Redis stream messages to msgpack files and replays them. Python package,
src layout, console script `boomrdbox = "boomrdbox.cli:main"`. Reference material (architecture,
config contracts, file format, operations, tracker labels): [DOCS.md](DOCS.md). User guide in
Russian: [README.md](README.md).

## Layout

- `src/boomrdbox/` — package: `cli.py` (dispatch), `_config.py` (hydra-zen store), `recorder.py`,
  `player.py`, `tools.py` (convert/truncate/info), `tui.py` (Textual truncate and setup forms),
  `io.py` (Redis factory, SSH tunnel, msgpack reader/writer), `instances.py` (persistent TOML),
  `models.py` (domain types and config dataclasses).
- `tests/` — pytest suite; `tests/test_integration.py` holds `@pytest.mark.integration` tests.
- `integration/` — Docker end-to-end script `integration_test.sh`, `fill_redis.py`, `truncate_cfg.yaml`.
- `docker-compose.yaml` — dev Redis on host port 6389, RedisInsight on 5540.
- `pyproject.toml` — dependencies and all tool config (ruff, mypy, pytest, vulture, import-linter).

## Environment

```bash
uv sync --dev          # Python >= 3.12 (.python-version), dev group holds every check tool
```

## Checks

No git hooks or hook runner. Run these directly from the repo root after any code change and do
not report completion until they pass:

```bash
uv run ruff format --force-exclude .     # format first
uv run ruff check --force-exclude .      # add --fix for safe autofixes; inspect the diff
uv run lint-imports                      # layer contracts in pyproject.toml
uv run mypy .                            # strict; covers src/, tests/, integration/
uv run vulture                           # dead code in src/boomrdbox, min_confidence 80
uv run pytest                            # -n auto, coverage gate --cov-fail-under=80
```

- Partial runs fail the coverage gate: `uv run pytest tests/test_foo.py --no-cov`,
  `uv run pytest -k name --no-cov`.
- `uv run pytest -m integration` needs `docker compose up -d redis` (localhost:6389); without
  Redis those tests skip, and a skip is not a pass. Plain `uv run pytest` includes them; when
  Redis is up they FLUSHDB DB 0 and 1 on localhost:6389.
- `bash integration/integration_test.sh [--keep-redis]` — Docker end-to-end: fill, record,
  truncate, play, verify. Starts Compose itself; flushes DB 0/1, reinstalls the built wheel into
  `.venv`, and runs `docker compose down` unless `--keep-redis`. Its play step needs
  `boomrdbox setup play-hosts add localhost` in the real user config first.
- Never stash or discard unrelated changes to make checks pass; fix the failure or report the
  blocker, running checks in a separate worktree if needed.
- Package build: `uv build` (hatchling; `pyproject.toml` names `README.md` as readme).

## Architecture boundaries

Layered imports, enforced by import-linter (`[tool.importlinter]` in `pyproject.toml`):

```
boomrdbox.cli                                              <- entry point
boomrdbox._config | .recorder | .player | .tools | .tui     <- application
boomrdbox.io | .instances                                  <- infrastructure
boomrdbox.models                                           <- domain (no imports of other layers)
```

Adding a module:
1. Place it in a layer.
2. Update both contracts in `pyproject.toml`: the "Layered architecture" layers and the
   "Domain isolation" `forbidden_modules`.
3. Add `[tool.ruff.lint.per-file-ignores]` entries only when intentional (e.g. `PLC0415` for lazy imports).

## Hard constraints

- `play` writes to Redis and is gated: `Player.run()` calls `validate_play_target(redis.host,
  load_allowed_play_hosts())` and raises `UnsafePlayTargetError` for hosts outside the whitelist
  (default `["redis"]`). Never weaken or bypass the gate.
- Read instances (`setup add`, `record instance=<name>`) are read-only. `play` never uses them.
- `setup` uses argparse; every other subcommand is a Hydra task configured by hydra-zen.
- Config precedence: CLI overrides > `REDIS_*` env vars > preset defaults. Preserve it.
- Recording file format is persisted data: keep readers compatible with existing files (see DOCS.md).
- Preserve CLI output, exit codes (1 on fatal error, 130 on interrupt) and public imports.

## Test mocking rules

- Player tests calling `Player.run()` MUST mock `boomrdbox.player.load_allowed_play_hosts`
  (return `["localhost"]`); fixtures use `host='localhost'`.
- Recorder tests using `instance=` MUST mock `boomrdbox.recorder.get_instance` and
  `boomrdbox.recorder.create_tunneled_redis`.
- Instance tests MUST mock `boomrdbox.instances.get_config_path` to `tmp_path`; never write the
  real user config.
- A new gate on `Player.run()` or `Recorder.run()` requires mocking it in existing tests.
- Integration tests share `xdist_group("redis_integration")`; keep new live-Redis tests in it.

## Ruff `select = ["ALL"]` pitfalls

- `PLC0415` (lazy import) is ignored for `cli.py`, `tools.py`, `tui.py`, `tests/**` only.
- `PLR0915` (>50 statements): extract helpers; `player.py` is already excepted.
- `tests/**` already ignores `S101`, `S105`, `S106`, `D`, `ANN`, `PT019`, `PLC0415`.
- `D102` missing public-method docstrings: add docstrings rather than suppress.

## Language standards

- Python >= 3.12 syntax; modules use `from __future__ import annotations`; keep that consistent.
- Strict mypy: explicit types, no blanket `Any` or `ignore_missing_imports`; overrides stay
  scoped to the listed untyped modules (`msgpack`, `pyarrow`, `hydra_zen`, `textual`). Add stubs to the dev group.
- Ruff lint fixes before formatting; formatting does not sort imports (lint rules do). Use
  `--force-exclude` when passing explicit paths. Safe fixes only; inspect the diff.
- Logging via Loguru; validated config via Pydantic dataclasses; tests via pytest.
- Catch specific exceptions at a boundary that can handle them; chain the cause (`raise ... from exc`).
- Dependencies: `uv add`/`uv remove` in the right group; plain `uv lock` (not `--upgrade`)
  to refresh the lock; inspect both `pyproject.toml` and `uv.lock` diffs.
- Cleanup: trace consumers before removing code; a vulture or grep hit is a lead, not proof.

## Release and delivery

- Issues, specs and plans live in Linear: team `SIP`, project `redis-stream-player`. Search
  before creating. Lifecycle and labels: DOCS.md "Tracker".
- When asked for a plan, plan only; do not implement until asked.
- Deliver via one task branch and one GitHub PR against `main` in
  `BuckanovNikita/redis-stream-player`. Put the issue id (`SIP-NN`) in the branch name and PR
  title; list every included issue in the PR description.
- Agents commit, push the task branch and open the PR; humans merge. Never push `main` and never
  deliver by merging locally.
- This repository is public and Linear is private: cite issues by `SIP-NN` only; never paste
  Linear URLs, issue text or comments into commits, PRs or files. Put the PR link in the issue.
- Use SSH remotes (`git@github.com:<org>/<repo>.git`).
- Checks above pass on the branch before the PR is marked ready for review.
- The required independent review (see Working agreement) comes from someone who did not write
  the change, after integrated verification and before the PR is marked ready for review; the
  human merge is a separate step.

## Known pitfalls

- Recording appends (`open("ab")`): writing to an existing output file extends it.
- Play uses `XADD id="*"`: replayed IDs are new; ordering is sorted per batch, not globally.
- `RecordReader` stops silently (warning only) at a truncated tail record.
- Default whitelist `["redis"]` means `play` against `localhost` fails until `setup play-hosts add localhost`.
- Dev Redis is on host port 6389, not 6379; the default Hydra preset uses 6379.
- Local test infrastructure is due to move from Compose to a private Redis in a project-owned
  Kubernetes namespace; see DOCS.md "Operations".

## Working agreement
- At the start of work, state the worktree path, the current branch and its relation to the default branch; say when remote refs were not fetched.
- Inspect existing and staged changes before editing. Preserve unrelated work and local overrides; stage only task-owned paths and inspect the staged diff.
- Report what each executed check establishes and what remains unverified; distinguish mocked checks, live outcomes and historical evidence. Clean up task-owned processes and temporary resources.
- Tests must exercise product code, never project tooling.
- Changes to behavior, interfaces, dependencies, security or operational policy need a fresh independent review after integration and verification. Small wording edits may be self-reviewed.
- Split large work into independent delegated tasks with explicit write ownership and acceptance evidence; delegated agents do not delegate further.
- Use Conventional Commits. Commit automatic formatting separately from logic. Local commits within the task scope need no approval.
- README.md is Russian; AGENTS.md and DOCS.md are English. Keep observed counts, timings and deployment status out of both files.
- Keep machine-specific paths, endpoints and settings in local configuration, not in these files.
