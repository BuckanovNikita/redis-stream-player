"""Fill Redis with synthetic sensor stream data for integration testing."""

from __future__ import annotations

import argparse
import math
import time

import redis


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill Redis with test stream data")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6389)
    parser.add_argument("--db", type=int, default=0)
    parser.add_argument("--duration-sec", type=int, default=120)
    args = parser.parse_args()

    client = redis.Redis(host=args.host, port=args.port, db=args.db)
    client.ping()

    base_ms = int(time.time() * 1000)
    duration_ms = args.duration_sec * 1000

    streams: dict[str, dict[str, int]] = {
        "sensor:imu": {"interval_ms": 10, "count": 0},
        "sensor:gps": {"interval_ms": 1000, "count": 0},
        "sensor:camera": {"interval_ms": 33, "count": 0},
    }

    first_ids: dict[str, str] = {}
    last_ids: dict[str, str] = {}
    total = 0
    pipe = client.pipeline(transaction=False)
    pipe_size = 0
    batch_limit = 500

    for stream_name, meta in streams.items():
        interval = meta["interval_ms"]
        offset = 0
        while offset < duration_ms:
            msg_id = f"{base_ms + offset}-0"

            if stream_name not in first_ids:
                first_ids[stream_name] = msg_id

            if stream_name == "sensor:imu":
                fields = {
                    "x": str(math.sin(offset / 1000.0)),
                    "y": str(math.cos(offset / 1000.0)),
                    "z": str(9.81 + math.sin(offset / 500.0) * 0.1),
                    "receive_ts": str((base_ms + offset) * 1_000_000),
                }
            elif stream_name == "sensor:gps":
                fields = {
                    "lat": str(55.7558 + offset * 0.000001),
                    "lon": str(37.6173 + offset * 0.000001),
                }
            else:
                fields = {
                    "frame": str(meta["count"]),
                    "ts_nano": str((base_ms + offset) * 1_000_000),
                    "width": "1920",
                    "height": "1080",
                }

            pipe.xadd(stream_name, fields, id=msg_id)
            pipe_size += 1
            meta["count"] += 1
            last_ids[stream_name] = msg_id

            if pipe_size >= batch_limit:
                pipe.execute()
                pipe = client.pipeline(transaction=False)
                pipe_size = 0

            offset += interval

    if pipe_size > 0:
        pipe.execute()

    for stream_name, meta in streams.items():
        total += meta["count"]
        print(
            f"STREAM_RANGE {stream_name}"
            f" {first_ids[stream_name]}"
            f" {last_ids[stream_name]}"
            f" {meta['count']}"
        )

    print(f"TOTAL_MESSAGES {total}")
    print(f"BASE_TIMESTAMP_MS {base_ms}")


if __name__ == "__main__":
    main()
