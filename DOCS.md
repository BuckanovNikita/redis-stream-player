# boomrdbox reference

Read on demand. Rules and checks live in [AGENTS.md](AGENTS.md); the Russian user guide is
[README.md](README.md).

## Architecture and data flow

- `cli.main()` dispatches on `argv[1]`: `setup` goes to argparse (`_setup_command`); `record`,
  `play`, `convert`, `truncate` and `info` pop the subcommand and run a hydra-zen task with
  `config_path=None`. Errors are caught by `_run_safe`: exit 1 on fatal error, 130 on interrupt.
- **record**: `Recorder` resolves `instance=<name>` via `instances.get_instance` (optionally
  opening an `ssh -L` tunnel with `io.create_tunneled_redis`, command logged), then loops
  `XREAD block=1000` over the configured streams starting at `$` (or `0-0` with
  `from_beginning=true`). Each message goes through `RecordWriter.write`. Stops on SIGINT/SIGTERM,
  `max_duration` (s) or `max_size_mb`. `rotate_key` starts a daemon thread that SUBSCRIBEs to that
  channel; any message sets a flag and the writer rotates to `<stem>_<YYYYmmddTHHMMSS><ext>` (UTC).
  The subscriber reconnects on `RedisError`. A status line is logged every 60 s.
- **play**: `Player.run()` checks the play-host whitelist first, connects and pings, then a
  producer thread reads the file, drops `exclude_streams`, applies `rename_streams`, sorts each
  batch (`batch_size`, default 1000) by message ID and enqueues it on a queue bounded by
  `prefetch`. The consumer replays with delays `min(delta_ms/1000/speed, max_delay)` between
  consecutive IDs, pipelines `XADD <stream> * <fields>` (non-transactional) and flushes before each
  sleep. Field values that are not `str`/`bytes` are stringified.
- **convert**: reads all records into memory and writes a pyarrow table, Parquet or CSV. Columns:
  `stream_name`, `message_id`, `timestamp_ms`, then the union of field keys sorted, missing values
  null. No output file when the input has no records.
- **truncate**: keeps records with `from_id <= id <= to_id` (each bound optional; `from_id > to_id`
  is an error). `auto_start=true` sets `from_id` to the latest first-message ID across streams;
  an explicit `from_id` overrides it. `interactive=true` opens the Textual timeline (`tui.py`).
- **info**: scans the file and prints per-stream statistics.
- **setup**: `add|remove|list` read instances; `play-hosts add|remove|list` the whitelist;
  `add --interactive` opens a Textual form.

## Domain terms

- **Recording** — msgpack file of stream messages produced by `record`.
- **Message ID** — Redis `<ms>-<seq>`; `MessageID.parse` also accepts `<ms>` (seq 0); ordered by (ms, seq).
- **Stream record** — `StreamRecord(stream_name, message_id, fields)`.
- **Read instance** — named, read-only Redis connection with optional SSH tunnel, used by `record`.
- **Play-host whitelist** — hostnames `play` may write to; default `["redis"]` (the Docker service
  name in the devcontainer network).
- **Rotation** — closing the current recording and continuing in a timestamp-suffixed file.

## Decisions

No numbered decision records exist. Binding choices:

- Layered architecture enforced by import-linter; `models` is a pure domain layer.
- `play` is gated by a host whitelist; read instances are never used for writing.
- `setup` uses argparse; data commands use Hydra with hydra-zen programmatic config (no YAML config dir).
- Recordings are headerless, append-only, crash-safe streams (flush per record, fsync on rotate/close).
- Replay assigns new IDs (`*`) and reproduces original inter-message timing scaled by `speed`.

## Interfaces and contracts

### Recording file format

A plain concatenation of msgpack arrays `[stream_name: str, message_id: "ms-seq", fields: map]`,
packed with `use_bin_type=True`, no header or version. Readers unpack with `raw=False`; a
malformed element (not a 3-item list) is skipped with a warning; a truncated tail ends reading
with a warning. The writer opens in append mode.

### Hydra config keys (defaults)

| Command | Keys |
|---|---|
| record | `redis` group (default `local`), `streams` group (default `sensors`), `output=recording.msgpack`, `instance=null`, `from_beginning=false`, `rotate_key=null`, `batch_size=100`, `max_duration=null` (>0), `max_size_mb=null` (>0), `verbose=false` |
| play | `redis` group (default `local`), `input=recording.msgpack`, `speed=1.0`, `max_delay=60.0`, `batch_size=1000`, `prefetch=4`, `exclude_streams=[]`, `rename_streams={}`, `verbose=false` |
| convert | `input=recording.msgpack`, `output=recording.parquet`, `format=parquet` (`parquet`/`csv`), `verbose=false` |
| truncate | `input=recording.msgpack`, `output=truncated.msgpack`, `from_id`, `to_id`, `auto_start=false`, `interactive=false`, `verbose=false` |
| info | `input=recording.msgpack`, `verbose=false` |

- `redis` presets: `local` (host default `localhost`) and `prod` (host default `prod-redis`), both
  reading `REDIS_HOST`, `REDIS_PORT` (default 6379), `REDIS_DB` (0), `REDIS_PASSWORD` (null).
  `RedisConf` validates port 1–65535, db >= 0.
- `streams` presets: `sensors` (`sensor:imu`, `sensor:gps`, `sensor:camera`) and `events`
  (`events:user`, `events:system`, `events:audit`). Inline: `'streams.streams=[{key: mystream}]'`;
  `io.parse_stream_configs` accepts strings or `{key: ...}` items.
- Precedence: CLI overrides > environment variables > preset defaults.

### Persistent user config

`platformdirs.user_config_dir("boomrdbox")/config.toml` (Linux `~/.config/boomrdbox/config.toml`),
written by a small built-in TOML serializer and read with `tomllib`:

```toml
[play]
allowed_hosts = ["redis"]

[instances.<name>.redis]        # host, port, db, optional password
[instances.<name>.ssh_tunnel]   # ssh_host, ssh_port, ssh_user, optional ssh_key_file, ssh_password
```

Missing or malformed `[play].allowed_hosts` falls back to `["redis"]`.

### setup CLI

`boomrdbox setup add <name> --host H [--port 6379] [--db 0] [--password P] [--ssh-host H]
[--ssh-port 22] [--ssh-user U] [--ssh-key FILE] [--interactive]`; `setup remove <name>`;
`setup list`; `setup play-hosts add|remove <hostname>`; `setup play-hosts list` (also the default).
Removing an unknown play host exits 1.

### Version bounds

Python >= 3.12; `redis>=5,<6`, `msgpack>=1,<2`, `hydra-core>=1.3,<2`, `hydra-zen>=0.13`,
`omegaconf>=2.3`, `pydantic>=2,<3`, `pyarrow>=15`, `textual>=1,<2`, `platformdirs>=4`,
`tqdm>=4.66`, `loguru>=0.7`. Build
backend hatchling; wheel packages `src/boomrdbox`.

## Operations

- Dev Redis: `docker compose up -d` (Redis `6389:6379`, RedisInsight 5540). Record against it
  with `redis.port=6389`. Play needs the target host in the whitelist.
- `integration/integration_test.sh` uses `localhost:6389`, DB 0 for recording and DB 1 for
  playback, container `redis-stream-player-redis-1`, a temp workdir and
  `integration/truncate_cfg.yaml` (placeholders substituted at runtime). `--keep-redis` leaves
  Redis running for inspection.
- Recovery: an interrupted recording is readable up to the last complete record; truncate or
  convert it as usual. A failed pipeline command during play is logged per command and replay
  continues.
- Infrastructure migration (pending): the repo does not use the shared k8s-infra service stands,
  but its Compose Redis must be replaced by a private Redis deployed in a project-owned namespace
  (distinct prefix such as `<project>-it-<id>`, never `default`, `kube-*` or platform namespaces),
  with its own manifests, `ResourceQuota`, `LimitRange` and owned teardown; use the default
  storage class or `emptyDir`; reach it from the host with `kubectl port-forward`. Namespaces are
  swept 48 h after creation or after a PC/Docker Desktop restart, so tooling must recreate them.
  Keep real Redis integration and CLI coverage against the new environment; absent-service skips
  do not count. The end state has no Compose dependency. Platform rules: the k8s-infra
  repository's AGENTS.md and the `k8s-infra` skill. Done when verification passes against it,
  the owner is told, and the sweeper report shows its namespaces with a quota (a missing quota
  means incomplete); then remove this bullet and the AGENTS.md pitfall.

## Tracker

Linear team `SIP`, project `redis-stream-player`; GitHub PRs deliver.

- Ticket = one issue: outcome, scope, plan where needed, acceptance checklist, selected
  verification. Feature = parent issue holding the spec, tickets as sub-issues. Blocking = Linear
  blocked-by relation. Discussion = issue comments.
- States: Triage, Backlog, Todo, In Progress, In Review, Done, Canceled, Duplicate. Move to In
  Progress on start, In Review when implementation and verification are ready. Done only after the
  PR is merged to `main` and acceptance is met; an agent finding an In Review issue with a merged
  PR checks acceptance and moves it to Done. Record executed checks and outcomes in the issue.
- Start dependent work only once its prerequisite is verified on the base branch; dependencies
  still order merges. Confirm a GitHub check, branch protection or Linear automation exists
  before relying on it. PRs are deliveries, not a request surface.
- One readiness label and one category label per issue; labels never override blockers,
  verification or the human merge. Use existing labels, no lowercase duplicates:

| Role | Linear label | Meaning |
|---|---|---|
| needs-triage | `needs-triage` | Maintainer must evaluate |
| needs-info | `needs-info` | Waiting on reporter |
| ready-for-agent | `ready-for-agent` | Fully specified, ready for an AFK agent |
| ready-for-human | `ready-for-human` | Needs human implementation |
| wontfix | `wontfix` | Not actioned; set state Canceled |
| bug | `Bug` | Something is broken |
| enhancement (new) | `Feature` | Behavior the product lacks |
| enhancement (existing) | `Improvement` | Existing behavior made better |

## History

The full pre-compression documents are in git at commit `40abd97`; read one with
`git show 40abd97:<path>`. Removed paths: `INFRA_MIGRATION.md`,
`docs/agents/issue-tracker.md`, `docs/agents/triage-labels.md`, `.pre-commit-config.yaml`
(also the previous `AGENTS.md` content, rewritten here).
