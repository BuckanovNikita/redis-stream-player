"""Application layer: Redis stream player."""

from __future__ import annotations

import logging
import signal
import threading
import time
from typing import TYPE_CHECKING, Any

from tqdm import tqdm

from redis_stream_player.io import (
    RecordReader,
    create_redis,
    parse_stream_configs,
)
from redis_stream_player.models import (
    MessageID,
    PlayConf,
    StreamConfig,
    StreamRecord,
    TimestampMode,
)

if TYPE_CHECKING:
    from types import FrameType

    import redis as redis_lib

logger = logging.getLogger(__name__)

_NS_PER_MS = 1_000_000


class Player:
    """Replays recorded Redis stream messages."""

    def __init__(self, conf: PlayConf) -> None:
        """Initialize with player configuration."""
        if conf.speed <= 0:
            msg = f"speed must be positive, got {conf.speed}"
            raise ValueError(msg)

        self._conf = conf
        self._stream_configs = parse_stream_configs(list(conf.streams.streams))
        self._config_map: dict[str, StreamConfig] = {
            sc.key: sc for sc in self._stream_configs
        }
        self._running = False
        self._stop_event = threading.Event()
        self._client: redis_lib.Redis[bytes] | None = None
        self._progress: tqdm[Any] | None = None
        logger.debug(
            "Player config: input=%s, speed=%s, max_delay=%s, batch_size=%s",
            conf.input,
            conf.speed,
            conf.max_delay,
            conf.batch_size,
        )

    def _handle_signal(self, _signum: int, _frame: FrameType | None) -> None:
        if not self._running:
            raise KeyboardInterrupt
        logger.info("Shutdown signal received, stopping...")
        self._running = False
        self._stop_event.set()

    def _adjust_timestamp(
        self,
        config: StreamConfig,
        fields: dict[str, object],
        original_msg_id: MessageID,
    ) -> dict[str, object]:
        """Adjust timestamp field if configured for shift mode."""
        if config.timestamp_mode != TimestampMode.SHIFT:
            return fields
        if config.timestamp_field is None:
            return fields

        ts_field = config.timestamp_field
        if ts_field not in fields:
            logger.debug(
                "Timestamp field %r missing in stream %s",
                ts_field,
                config.key,
            )
            return fields

        raw_val = fields[ts_field]
        try:
            original_ts_ns = int(str(raw_val))
        except (ValueError, TypeError):
            logger.warning(
                "Non-numeric timestamp %r in stream %s",
                raw_val,
                config.key,
            )
            return fields

        original_offset = original_msg_id.ms * _NS_PER_MS - original_ts_ns
        now_ns = time.time_ns()
        adjusted_ts_ns = now_ns - original_offset

        adjusted_fields = dict(fields)
        adjusted_fields[ts_field] = str(adjusted_ts_ns)
        return adjusted_fields

    def run(self) -> None:
        """Run the player, replaying all records from the file."""
        self._running = True
        self._stop_event.clear()

        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._client = create_redis(self._conf.redis)
        self._client.ping()
        logger.info(
            "Connected to Redis at %s:%s",
            self._conf.redis.host,
            self._conf.redis.port,
        )
        reader = RecordReader(self._conf.input)
        logger.info(
            "Playing streams: %s",
            ", ".join(sc.key for sc in self._stream_configs),
        )

        batch_size = self._conf.batch_size

        try:
            file_size = reader.file_size
        except OSError:
            file_size = 0

        self._progress = tqdm(
            total=file_size if file_size > 0 else None,
            desc="Playing",
            unit="B",
            unit_scale=True,
            disable=not self._conf.verbose,
        )

        prev_msg_id: MessageID | None = None
        prev_mono: float | None = None
        replayed = 0

        try:
            batch: list[StreamRecord] = []
            for record in reader:
                if not self._running:
                    break
                batch.append(record)

                if len(batch) >= batch_size:
                    self._replay_batch(
                        batch,
                        prev_msg_id,
                        prev_mono,
                    )
                    if batch:
                        prev_msg_id = batch[-1].message_id
                        prev_mono = time.monotonic()
                    replayed += len(batch)
                    batch = []

            if batch and self._running:
                self._replay_batch(
                    batch,
                    prev_msg_id,
                    prev_mono,
                )
                replayed += len(batch)

        finally:
            self._progress.close()
            self._progress = None
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)
            self._running = False

        logger.info("Playback complete: %d messages replayed", replayed)

    def _prepare_fields(
        self,
        record: StreamRecord,
    ) -> dict[str, str | bytes]:
        """Prepare XADD fields from a record, applying timestamp adjustments."""
        config = self._config_map.get(record.stream_name)
        fields = record.fields
        if config is not None:
            fields = self._adjust_timestamp(config, fields, record.message_id)

        xadd_fields: dict[str, str | bytes] = {}
        for k, v in fields.items():
            xadd_fields[str(k)] = str(v) if not isinstance(v, bytes) else v
        return xadd_fields

    def _flush_pipeline(
        self,
        pipe: redis_lib.client.Pipeline[bytes],
        pipe_meta: list[tuple[str, list[str]]],
    ) -> None:
        """Execute a pipeline and log any per-command failures."""
        results = pipe.execute(raise_on_error=False)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                stream_name, field_keys = pipe_meta[i]
                logger.error(
                    "Failed to XADD to %s (fields: %s): %s",
                    stream_name,
                    field_keys,
                    result,
                )

    def _replay_batch(
        self,
        batch: list[StreamRecord],
        prev_msg_id: MessageID | None,
        prev_mono: float | None,
    ) -> None:
        """Sort batch by MessageID and replay with timing."""
        assert self._client is not None  # noqa: S101
        client = self._client
        assert self._progress is not None  # noqa: S101
        progress = self._progress
        speed = self._conf.speed
        max_delay = self._conf.max_delay

        batch.sort(key=lambda r: r.message_id)
        logger.debug("Replaying batch of %d records", len(batch))

        local_prev_id = prev_msg_id
        local_prev_mono = prev_mono

        pipe = client.pipeline(transaction=False)
        pipe_count = 0
        pipe_meta: list[tuple[str, list[str]]] = []

        for record in batch:
            if not self._running:
                break

            if local_prev_id is not None and local_prev_mono is not None:
                delta_ms = record.message_id.ms - local_prev_id.ms
                if delta_ms > 0:
                    delay = min((delta_ms / 1000.0) / speed, max_delay)
                    logger.debug(
                        "Delay %.3fs (delta_ms=%d, speed=%.1f)",
                        delay,
                        delta_ms,
                        speed,
                    )
                    if delay > 0:
                        # Flush pending commands before sleeping
                        if pipe_count > 0:
                            self._flush_pipeline(pipe, pipe_meta)
                            progress.update(pipe_count)
                            pipe = client.pipeline(transaction=False)
                            pipe_count = 0
                            pipe_meta = []

                        if self._stop_event.wait(timeout=delay):
                            break

            xadd_fields = self._prepare_fields(record)
            pipe.xadd(record.stream_name, xadd_fields, id="*")
            pipe_meta.append((record.stream_name, list(xadd_fields.keys())))
            pipe_count += 1

            local_prev_id = record.message_id
            local_prev_mono = time.monotonic()

        # Flush remaining commands
        if pipe_count > 0:
            self._flush_pipeline(pipe, pipe_meta)
            progress.update(pipe_count)
