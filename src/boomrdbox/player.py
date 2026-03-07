"""Application layer: Redis stream player."""

from __future__ import annotations

import queue
import signal
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgpack
from loguru import logger
from tqdm import tqdm

from boomrdbox.io import (
    RecordReader,
    create_redis,
    parse_stream_configs,
)
from boomrdbox.models import (
    MessageID,
    PlayConf,
    StreamConfig,
    StreamRecord,
    TimestampMode,
)

if TYPE_CHECKING:
    from types import FrameType

    import redis as redis_lib

_NS_PER_MS = 1_000_000


def _format_ms(ms: int) -> str:
    total_seconds = ms // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


@dataclass(frozen=True)
class _BatchMeta:
    """Metadata about stream time boundaries for progress tracking."""

    first_ms: int
    last_ms: int


@dataclass(frozen=True)
class _PreparedRecord:
    """A record with pre-computed XADD fields ready for replay."""

    stream_name: str
    message_id: MessageID
    xadd_fields: dict[str, str | bytes]


class Player:
    """Replays recorded Redis stream messages."""

    def __init__(self, conf: PlayConf) -> None:
        """Initialize with player configuration."""
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
            "Player config: input=%s, speed=%s, max_delay=%s,"
            " batch_size=%s, prefetch=%s",
            conf.input,
            conf.speed,
            conf.max_delay,
            conf.batch_size,
            conf.prefetch,
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
            if isinstance(raw_val, (int, str)):
                original_ts_ns = int(raw_val)
            else:
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

        fields[ts_field] = str(adjusted_ts_ns)
        return fields

    def _prepare_fields(
        self,
        record: StreamRecord,
    ) -> dict[str, str | bytes]:
        """Prepare XADD fields from a record, applying timestamp adjustments."""
        config = self._config_map.get(record.stream_name)
        fields = dict(record.fields)
        if config is not None:
            fields = self._adjust_timestamp(config, fields, record.message_id)

        xadd_fields: dict[str, str | bytes] = {}
        for k, v in fields.items():
            if isinstance(v, (str, bytes)):
                xadd_fields[k if isinstance(k, str) else str(k)] = v
            else:
                xadd_fields[k if isinstance(k, str) else str(k)] = str(v)
        return xadd_fields

    def _producer_loop(
        self,
        q: queue.Queue[tuple[list[_PreparedRecord], _BatchMeta] | None],
        reader: RecordReader,
    ) -> None:
        """Read records from file, prepare batches, and enqueue them."""
        batch_size = self._conf.batch_size
        try:
            batch: list[StreamRecord] = []
            first_ms: int | None = None
            last_ms = 0
            for record in reader:
                if self._stop_event.is_set():
                    break
                batch.append(record)

                if len(batch) >= batch_size:
                    batch.sort(key=lambda r: r.message_id)
                    prepared = [
                        _PreparedRecord(
                            stream_name=r.stream_name,
                            message_id=r.message_id,
                            xadd_fields=self._prepare_fields(r),
                        )
                        for r in batch
                    ]
                    if first_ms is None:
                        first_ms = batch[0].message_id.ms
                    last_ms = max(last_ms, batch[-1].message_id.ms)
                    q.put((prepared, _BatchMeta(first_ms=first_ms, last_ms=last_ms)))
                    batch = []

            if batch and not self._stop_event.is_set():
                batch.sort(key=lambda r: r.message_id)
                prepared = [
                    _PreparedRecord(
                        stream_name=r.stream_name,
                        message_id=r.message_id,
                        xadd_fields=self._prepare_fields(r),
                    )
                    for r in batch
                ]
                if first_ms is None:
                    first_ms = batch[0].message_id.ms
                last_ms = max(last_ms, batch[-1].message_id.ms)
                q.put((prepared, _BatchMeta(first_ms=first_ms, last_ms=last_ms)))
        except (msgpack.UnpackValueError, OSError, KeyError, TypeError):
            logger.exception("Producer thread error")
        finally:
            q.put(None)

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

        self._progress = tqdm(
            total=0,
            desc="Playing",
            bar_format="{desc}: {postfix} [{bar:30}] {percentage:3.0f}%",
            disable=not self._conf.verbose,
        )
        self._progress.set_postfix_str("00:00:00 / --:--:--")

        q: queue.Queue[tuple[list[_PreparedRecord], _BatchMeta] | None] = queue.Queue(
            maxsize=self._conf.prefetch,
        )
        producer = threading.Thread(
            target=self._producer_loop,
            args=(q, reader),
            daemon=True,
        )
        producer.start()

        prev_msg_id: MessageID | None = None
        prev_mono: float | None = None
        replayed = 0

        try:
            while self._running:
                try:
                    item = q.get(timeout=0.5)
                except queue.Empty:
                    continue

                if item is None:
                    break

                batch, meta = item
                self._replay_batch(batch, prev_msg_id, prev_mono)
                if batch:
                    prev_msg_id = batch[-1].message_id
                    prev_mono = time.monotonic()
                replayed += len(batch)

                elapsed_ms = batch[-1].message_id.ms - meta.first_ms if batch else 0
                total_ms = meta.last_ms - meta.first_ms
                self._progress.n = elapsed_ms
                self._progress.total = max(total_ms, 1)
                self._progress.set_postfix_str(
                    f"{_format_ms(elapsed_ms)} / {_format_ms(total_ms)}"
                )
                self._progress.refresh()

        finally:
            self._stop_event.set()
            producer.join(timeout=5.0)
            self._progress.close()
            self._progress = None
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)
            self._running = False

        logger.info("Playback complete: %d messages replayed", replayed)

    def _flush_pipeline(
        self,
        pipe: redis_lib.client.Pipeline[bytes],
        pipe_meta: list[tuple[str, dict[str, str | bytes]]],
    ) -> None:
        """Execute a pipeline and log any per-command failures."""
        results = pipe.execute(raise_on_error=False)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                stream_name, xadd_fields = pipe_meta[i]
                logger.error(
                    "Failed to XADD to %s (fields: %s): %s",
                    stream_name,
                    list(xadd_fields.keys()),
                    result,
                )

    def _replay_batch(
        self,
        batch: list[_PreparedRecord],
        prev_msg_id: MessageID | None,
        prev_mono: float | None,
    ) -> None:
        """Replay a batch of prepared records with timing."""
        assert self._client is not None  # noqa: S101
        client = self._client
        speed = self._conf.speed
        max_delay = self._conf.max_delay

        logger.debug("Replaying batch of %d records", len(batch))

        local_prev_id = prev_msg_id
        local_prev_mono = prev_mono

        pipe = client.pipeline(transaction=False)
        pipe_count = 0
        pipe_meta: list[tuple[str, dict[str, str | bytes]]] = []

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
                            pipe = client.pipeline(transaction=False)
                            pipe_count = 0
                            pipe_meta = []

                        if self._stop_event.wait(timeout=delay):
                            break

            pipe.xadd(record.stream_name, record.xadd_fields, id="*")
            pipe_meta.append((record.stream_name, record.xadd_fields))
            pipe_count += 1

            local_prev_id = record.message_id
            local_prev_mono = time.monotonic()

        # Flush remaining commands
        if pipe_count > 0:
            self._flush_pipeline(pipe, pipe_meta)
