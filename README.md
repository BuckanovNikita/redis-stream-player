# redis-stream-player

Record and replay Redis stream data. Capture live stream messages to a compact msgpack file, then play them back with accurate timing, speed control, and timestamp adjustment.

## Features

- **Record** — capture messages from multiple Redis streams into a single msgpack file
- **Play** — replay recorded messages with original timing, adjustable speed, and pipelined writes
- **Convert** — export recordings to Parquet or CSV for analysis
- **Truncate** — slice recordings by message ID range
- **Info** — display per-stream statistics from a recording file

## Installation

```bash
pip install redis-stream-player
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install redis-stream-player
```

## Quick start

### Record

Capture messages from configured streams:

```bash
redis-stream-record output=recording.msgpack
```

Record from the beginning of each stream:

```bash
redis-stream-record output=recording.msgpack from_beginning=true
```

Set recording limits:

```bash
redis-stream-record max_duration=60 max_size_mb=100
```

### Play

Replay a recording at original speed:

```bash
redis-stream-play input=recording.msgpack
```

Replay at 2x speed with a 10-second max inter-message delay:

```bash
redis-stream-play input=recording.msgpack speed=2.0 max_delay=10
```

### Convert

Export to Parquet or CSV:

```bash
redis-stream-convert input=recording.msgpack output=data.parquet format=parquet
redis-stream-convert input=recording.msgpack output=data.csv format=csv
```

### Truncate

Slice a recording by message ID range:

```bash
redis-stream-truncate input=recording.msgpack output=slice.msgpack \
    from_id=1709312000000-0 to_id=1709312010000-0
```

Auto-detect the start point where all streams are active:

```bash
redis-stream-truncate input=recording.msgpack output=trimmed.msgpack auto_start=true
```

### Info

Show recording statistics:

```bash
redis-stream-info input=recording.msgpack
```

## Configuration

This project uses [Hydra](https://hydra.cc/) for configuration. Default configs live in `src/redis_stream_player/conf/`.

### Redis connection

Override the Redis connection via config groups or CLI:

```bash
redis-stream-record redis=prod                # use conf/redis/prod.yaml
redis-stream-record redis.host=10.0.0.5 redis.port=6380
```

Default Redis config (`conf/redis/local.yaml`):

```yaml
host: localhost
port: 6379
db: 0
password: null
```

### Streams

Switch stream groups or define streams inline:

```bash
redis-stream-record streams=events            # use conf/streams/events.yaml
redis-stream-record 'streams.streams=[{key: mystream}]'
```

Streams support optional timestamp adjustment during playback:

```yaml
streams:
  - key: sensor:imu
    timestamp_field: receive_ts
    timestamp_mode: bypass      # keep original (default)
  - key: sensor:camera
    timestamp_field: ts_nano
    timestamp_mode: shift       # adjust to current wall-clock time
```

## Docker Compose dev setup

A `docker-compose.yaml` is included for local development:

```bash
docker compose up -d    # starts Redis on port 6389 + RedisInsight on port 5540
```

Connect the tools to the dev Redis:

```bash
redis-stream-record redis.port=6389
```

## Development

```bash
# Clone and install
git clone git@github.com:<org>/redis-stream-player.git
cd redis-stream-player
uv sync --dev

# Run checks
uv run pytest                          # tests
uv run ruff check .                    # lint
uv run mypy .                          # type check
uv run pre-commit run --all-files      # all hooks
```
