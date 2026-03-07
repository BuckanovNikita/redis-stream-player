"""Application layer: Converter, Truncator, and Info tools."""

from __future__ import annotations

import sys
from typing import Any

from loguru import logger
from tqdm import tqdm

from redis_stream_player.io import RecordReader, RecordWriter
from redis_stream_player.models import (
    ConvertConf,
    InfoConf,
    MessageID,
    TruncateConf,
)


class Converter:
    """Convert msgpack recording to Parquet or CSV."""

    def __init__(self, conf: ConvertConf) -> None:
        """Initialize with converter configuration."""
        self._conf = conf

    def run(self) -> None:
        """Read msgpack and write to the configured output format."""
        logger.info(
            "Converting %s to %s -> %s",
            self._conf.input,
            self._conf.format,
            self._conf.output,
        )
        reader = RecordReader(self._conf.input)
        fmt = self._conf.format.lower()

        if fmt not in ("parquet", "csv"):
            msg = f"Unsupported format: {fmt!r}, use 'parquet' or 'csv'"
            raise ValueError(msg)

        try:
            file_size = reader.file_size
        except OSError:
            file_size = 0

        progress = tqdm(
            total=file_size if file_size > 0 else None,
            desc="Converting",
            unit="B",
            unit_scale=True,
            disable=not self._conf.verbose,
        )

        rows: list[dict[str, Any]] = []
        all_field_keys: set[str] = set()

        for record in reader:
            row: dict[str, Any] = {
                "stream_name": record.stream_name,
                "message_id": str(record.message_id),
                "timestamp_ms": record.message_id.ms,
            }
            for k, v in record.fields.items():
                row[k] = v
                all_field_keys.add(k)
            rows.append(row)
            progress.update(1)

        progress.close()
        logger.debug("Read %d records for conversion", len(rows))

        if not rows:
            logger.info("No records found, output file not created")
            return

        import pyarrow as pa
        import pyarrow.csv as pcsv
        import pyarrow.parquet as pq

        columns: dict[str, list[Any]] = {
            "stream_name": [],
            "message_id": [],
            "timestamp_ms": [],
        }
        for key in sorted(all_field_keys):
            columns[key] = []

        for row in rows:
            columns["stream_name"].append(row["stream_name"])
            columns["message_id"].append(row["message_id"])
            columns["timestamp_ms"].append(row["timestamp_ms"])
            for key in sorted(all_field_keys):
                columns[key].append(row.get(key))

        table = pa.table(columns)

        if fmt == "parquet":
            pq.write_table(table, self._conf.output)
        else:
            pcsv.write_csv(table, self._conf.output)

        logger.info(
            "Converted %d records to %s: %s",
            len(rows),
            fmt,
            self._conf.output,
        )


class Truncator:
    """Slice a recording by message IDs."""

    def __init__(self, conf: TruncateConf) -> None:
        """Initialize with truncator configuration."""
        self._conf = conf

    def run(self) -> None:
        """Read input, filter by ID range, write to output."""
        logger.info(
            "Truncating %s -> %s (from_id=%s, to_id=%s)",
            self._conf.input,
            self._conf.output,
            self._conf.from_id,
            self._conf.to_id,
        )
        reader = RecordReader(self._conf.input)

        from_id: MessageID | None = None
        to_id: MessageID | None = None

        if self._conf.auto_start:
            from_id = self._find_auto_start(reader)
            if from_id is not None:
                logger.info("Auto-start ID: %s", from_id)

        if self._conf.from_id is not None:
            from_id = MessageID.parse(self._conf.from_id)

        if self._conf.to_id is not None:
            to_id = MessageID.parse(self._conf.to_id)

        if from_id is not None and to_id is not None and from_id > to_id:
            msg = f"from_id ({from_id}) > to_id ({to_id})"
            raise ValueError(msg)

        try:
            file_size = reader.file_size
        except OSError:
            file_size = 0

        progress = tqdm(
            total=file_size if file_size > 0 else None,
            desc="Truncating",
            unit="B",
            unit_scale=True,
            disable=not self._conf.verbose,
        )

        count = 0
        skipped = 0
        with RecordWriter(self._conf.output) as writer:
            for record in reader:
                if from_id is not None and record.message_id < from_id:
                    skipped += 1
                    progress.update(1)
                    continue
                if to_id is not None and record.message_id > to_id:
                    skipped += 1
                    progress.update(1)
                    continue
                writer.write(record)
                count += 1
                progress.update(1)

        progress.close()
        logger.debug("Skipped %d records outside range", skipped)
        logger.info(
            "Truncated: %d records written to %s",
            count,
            self._conf.output,
        )

    @staticmethod
    def _find_auto_start(
        reader: RecordReader,
    ) -> MessageID | None:
        """Find the latest first-message-ID across all streams.

        This is the point where all streams are active.
        """
        first_ids: dict[str, MessageID] = {}
        for record in reader:
            if record.stream_name not in first_ids:
                first_ids[record.stream_name] = record.message_id

        if not first_ids:
            return None

        return max(first_ids.values())


class Info:
    """Show per-stream statistics from a recording file."""

    def __init__(self, conf: InfoConf) -> None:
        """Initialize with info configuration."""
        self._conf = conf

    def run(self) -> None:
        """Scan the file and print per-stream statistics."""
        logger.info("Inspecting file: %s", self._conf.input)
        reader = RecordReader(self._conf.input)

        try:
            file_size = reader.file_size
        except OSError:
            file_size = 0

        progress = tqdm(
            total=file_size if file_size > 0 else None,
            desc="Scanning",
            unit="B",
            unit_scale=True,
            disable=not self._conf.verbose,
        )

        stats: dict[str, _StreamStats] = {}

        for record in reader:
            name = record.stream_name
            if name not in stats:
                stats[name] = _StreamStats(
                    first_id=record.message_id,
                    last_id=record.message_id,
                    count=0,
                )
            s = stats[name]
            s.last_id = record.message_id
            s.count += 1
            progress.update(1)

        progress.close()

        if not stats:
            sys.stdout.write("No records found.\n")
            return

        sys.stdout.write(f"\nFile: {self._conf.input} ({file_size:,} bytes)\n")
        sys.stdout.write(f"Streams: {len(stats)}\n\n")

        total_messages = 0
        for name, s in sorted(stats.items()):
            duration_ms = s.last_id.ms - s.first_id.ms
            duration_s = duration_ms / 1000.0
            sys.stdout.write(f"  {name}:\n")
            sys.stdout.write(f"    Messages: {s.count:,}\n")
            sys.stdout.write(f"    First ID: {s.first_id}\n")
            sys.stdout.write(f"    Last ID:  {s.last_id}\n")
            sys.stdout.write(f"    Duration: {duration_s:.1f}s\n")
            total_messages += s.count

        sys.stdout.write(f"\nTotal messages: {total_messages:,}\n")


class _StreamStats:
    """Mutable accumulator for per-stream statistics."""

    __slots__ = ("count", "first_id", "last_id")

    def __init__(
        self,
        first_id: MessageID,
        last_id: MessageID,
        count: int,
    ) -> None:
        self.first_id = first_id
        self.last_id = last_id
        self.count = count
