"""Application layer: Redis stream recorder."""

from __future__ import annotations

import signal
import threading
import time
from typing import TYPE_CHECKING, Any

import redis as redis_lib
from loguru import logger
from tqdm import tqdm

from boomrdbox.io import (
    RecordWriter,
    create_redis,
    parse_stream_configs,
)
from boomrdbox.models import MessageID, RecordConf, StreamRecord

if TYPE_CHECKING:
    from types import FrameType


class Recorder:
    """Records Redis stream messages to a msgpack file."""

    def __init__(self, conf: RecordConf) -> None:
        """Initialize with recorder configuration."""
        self._conf = conf
        self._stream_configs = parse_stream_configs(list(conf.streams.streams))
        self._running = False
        self._stop_event = threading.Event()
        self._rotate_flag = threading.Event()
        self._redis: redis_lib.Redis[bytes] | None = None
        self._sub_thread: threading.Thread | None = None
        self._client: redis_lib.Redis[bytes] | None = None
        self._progress: tqdm[Any] | None = None
        logger.debug(
            "Recorder: output=%s, batch=%s, duration=%s, size_mb=%s",
            conf.output,
            conf.batch_size,
            conf.max_duration,
            conf.max_size_mb,
        )

    def _handle_signal(self, _signum: int, _frame: FrameType | None) -> None:
        if not self._running:
            raise KeyboardInterrupt
        logger.info("Shutdown signal received, stopping...")
        self._running = False
        self._stop_event.set()

    def _subscribe_rotation(self, client: redis_lib.Redis[bytes]) -> None:
        """Watch for rotation signals via Redis SUBSCRIBE."""
        rotate_key = self._conf.rotate_key
        if not rotate_key:
            return

        def _listener() -> None:
            while self._running:
                try:
                    pubsub = client.pubsub()
                    pubsub.subscribe(rotate_key)
                    for message in pubsub.listen():  # type: ignore[no-untyped-call]
                        if not self._running:
                            break
                        if message["type"] == "message":
                            self._rotate_flag.set()
                except redis_lib.exceptions.RedisError:
                    if not self._running:
                        break
                    logger.warning("SUBSCRIBE connection lost, reconnecting...")
                    self._stop_event.wait(timeout=1.0)

        self._sub_thread = threading.Thread(
            target=_listener,
            daemon=True,
            name="rotation-listener",
        )
        self._sub_thread.start()
        logger.debug("Rotation subscription thread started on key=%s", rotate_key)

    def run(self) -> None:
        """Run the recording loop until signal or limits reached."""
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
        self._redis = self._client

        self._subscribe_rotation(self._client)

        stream_keys = {sc.key: sc for sc in self._stream_configs}
        if not stream_keys:
            logger.error("No streams configured")
            return

        logger.info("Recording streams: %s", ", ".join(stream_keys))

        last_ids: dict[str, str] = {}
        for key in stream_keys:
            last_ids[key] = "0-0" if self._conf.from_beginning else "$"

        start_time = time.monotonic()

        with RecordWriter(self._conf.output) as writer:
            self._progress = tqdm(
                desc="Recording",
                unit=" msgs",
                disable=not self._conf.verbose,
            )

            try:
                self._record_loop(
                    writer,
                    stream_keys,
                    last_ids,
                    start_time,
                )
            finally:
                self._progress.close()
                self._progress = None
                signal.signal(signal.SIGINT, original_sigint)
                signal.signal(signal.SIGTERM, original_sigterm)
                self._running = False
                self._stop_event.set()

        total_elapsed = time.monotonic() - start_time
        logger.info(
            "Recording complete: %d messages in %.1fs, file: %s",
            writer.count,
            total_elapsed,
            writer.path,
        )

    def _check_limits(
        self,
        writer: RecordWriter,
        start_time: float,
    ) -> bool:
        """Check whether recording limits have been reached.

        Returns True if recording should stop.
        """
        max_duration = self._conf.max_duration
        if max_duration is not None:
            elapsed = time.monotonic() - start_time
            if elapsed >= max_duration:
                logger.info("Max duration %.1fs reached", max_duration)
                return True

        max_size_bytes = (
            int(self._conf.max_size_mb * 1024 * 1024)
            if self._conf.max_size_mb is not None
            else None
        )
        if max_size_bytes is not None and writer.size >= max_size_bytes:
            logger.info("Max file size reached, stopping")
            return True

        return False

    def _maybe_rotate(self, writer: RecordWriter) -> None:
        """Rotate the output file if a rotation signal was received."""
        if self._rotate_flag.is_set():
            self._rotate_flag.clear()
            new_path = writer.rotate()
            logger.info("Rotated to %s", new_path)

    def _record_loop(
        self,
        writer: RecordWriter,
        stream_keys: dict[str, Any],
        last_ids: dict[str, str],
        start_time: float,
    ) -> None:
        """Core XREAD recording loop."""
        assert self._client is not None  # noqa: S101
        client = self._client
        assert self._progress is not None  # noqa: S101
        progress = self._progress
        warned_streams: set[str] = set()

        while self._running:
            if self._check_limits(writer, start_time):
                break

            self._maybe_rotate(writer)

            streams_arg = {k: last_ids[k] for k in stream_keys if k in last_ids}
            if not streams_arg:
                break

            try:
                result = client.xread(
                    streams=streams_arg,
                    count=self._conf.batch_size,
                    block=1000,
                )
            except (redis_lib.exceptions.RedisError, OSError):
                if not self._running:
                    break
                logger.exception("XREAD error")
                self._stop_event.wait(timeout=1.0)
                continue

            if result is None:
                continue

            logger.debug(
                "XREAD returned %d stream(s): %s",
                len(result),
                ", ".join(
                    f"{s[0].decode() if isinstance(s[0], bytes) else s[0]}={len(s[1])}"
                    for s in result
                ),
            )

            batch_count = self._process_xread_result(
                result,
                stream_keys,
                warned_streams,
                last_ids,
                writer,
            )

            if batch_count > 0:
                progress.update(batch_count)

    def _process_xread_result(
        self,
        result: list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]],
        stream_keys: dict[str, Any],
        warned_streams: set[str],
        last_ids: dict[str, str],
        writer: RecordWriter,
    ) -> int:
        """Process XREAD result and write records."""
        batch_count = 0
        for stream_data in result:
            stream_name_bytes, messages = stream_data
            try:
                stream_name = (
                    stream_name_bytes.decode()
                    if isinstance(stream_name_bytes, bytes)
                    else str(stream_name_bytes)
                )
            except (UnicodeDecodeError, AttributeError):
                logger.exception("Failed to decode stream name: %r", stream_name_bytes)
                continue

            if stream_name not in stream_keys:
                if stream_name not in warned_streams:
                    logger.warning("Unexpected stream %s", stream_name)
                    warned_streams.add(stream_name)
                continue

            for msg_id_bytes, fields_raw in messages:
                try:
                    msg_id_str = (
                        msg_id_bytes.decode()
                        if isinstance(msg_id_bytes, bytes)
                        else str(msg_id_bytes)
                    )
                except (UnicodeDecodeError, AttributeError):
                    logger.exception("Failed to decode message ID: %r", msg_id_bytes)
                    continue

                fields: dict[str, object] = {}
                try:
                    for k, v in fields_raw.items():
                        key_str = k.decode() if isinstance(k, bytes) else str(k)
                        val = v.decode() if isinstance(v, bytes) else v
                        fields[key_str] = val
                except (UnicodeDecodeError, AttributeError):
                    logger.exception(
                        "Failed to decode fields for %s/%s",
                        stream_name,
                        msg_id_str,
                    )
                    continue

                mid = MessageID.parse(msg_id_str)
                record = StreamRecord(
                    stream_name=stream_name,
                    message_id=mid,
                    fields=fields,
                )
                writer.write(record)
                batch_count += 1
                last_ids[stream_name] = msg_id_str

        return batch_count
