"""Tests for tools (Converter, Truncator, Info)."""

import pytest

from boomrdbox.io import RecordReader
from boomrdbox.models import (
    ConvertConf,
    InfoConf,
    MessageID,
    TruncateConf,
)
from boomrdbox.tools import Converter, Info, Truncator


class TestConverter:
    def test_convert_to_parquet(self, sample_msgpack, tmp_path):
        output = tmp_path / "output.parquet"
        conf = ConvertConf(
            input=str(sample_msgpack),
            output=str(output),
            format="parquet",
        )
        Converter(conf).run()
        assert output.exists()
        assert output.stat().st_size > 0

    def test_convert_to_csv(self, sample_msgpack, tmp_path):
        output = tmp_path / "output.csv"
        conf = ConvertConf(
            input=str(sample_msgpack),
            output=str(output),
            format="csv",
        )
        Converter(conf).run()
        assert output.exists()
        content = output.read_text()
        assert "stream_name" in content
        assert "sensor:imu" in content

    def test_convert_empty_file(self, tmp_path):
        input_file = tmp_path / "empty.msgpack"
        input_file.touch()
        output = tmp_path / "output.parquet"
        conf = ConvertConf(input=str(input_file), output=str(output))
        Converter(conf).run()
        assert not output.exists()

    def test_invalid_format(self, sample_msgpack, tmp_path):
        conf = ConvertConf(
            input=str(sample_msgpack),
            output=str(tmp_path / "out.json"),
            format="json",
        )
        with pytest.raises(ValueError, match="Unsupported format"):
            Converter(conf).run()


class TestTruncator:
    def test_truncate_by_range(self, sample_msgpack, tmp_path):
        output = tmp_path / "truncated.msgpack"
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(output),
            from_id="1709312000100-0",
            to_id="1709312000300-0",
        )
        Truncator(conf).run()

        records = list(RecordReader(output))
        assert len(records) == 3
        assert records[0].message_id == MessageID(1709312000100, 0)
        assert records[-1].message_id == MessageID(1709312000300, 0)

    def test_truncate_from_only(self, sample_msgpack, tmp_path):
        output = tmp_path / "truncated.msgpack"
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(output),
            from_id="1709312000300-0",
        )
        Truncator(conf).run()

        records = list(RecordReader(output))
        assert len(records) == 2

    def test_truncate_to_only(self, sample_msgpack, tmp_path):
        output = tmp_path / "truncated.msgpack"
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(output),
            to_id="1709312000100-0",
        )
        Truncator(conf).run()

        records = list(RecordReader(output))
        assert len(records) == 2

    def test_auto_start(self, sample_msgpack, tmp_path):
        output = tmp_path / "truncated.msgpack"
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(output),
            auto_start=True,
        )
        Truncator(conf).run()

        records = list(RecordReader(output))
        # Auto-start: latest first msg across streams
        # imu@0, gps@100, camera@300 → auto_start = 300
        assert all(r.message_id >= MessageID(1709312000300, 0) for r in records)

    def test_invalid_range(self, sample_msgpack, tmp_path):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(tmp_path / "out.msgpack"),
            from_id="1709312000300-0",
            to_id="1709312000100-0",
        )
        with pytest.raises(ValueError, match=r"from_id.*> to_id"):
            Truncator(conf).run()

    def test_empty_file(self, tmp_path):
        input_file = tmp_path / "empty.msgpack"
        input_file.touch()
        output = tmp_path / "out.msgpack"
        conf = TruncateConf(input=str(input_file), output=str(output))
        Truncator(conf).run()
        assert list(RecordReader(output)) == []


class TestInfo:
    def test_info_output(self, sample_msgpack, capsys):
        conf = InfoConf(input=str(sample_msgpack))
        Info(conf).run()

        captured = capsys.readouterr()
        assert "sensor:imu" in captured.out
        assert "sensor:gps" in captured.out
        assert "sensor:camera" in captured.out
        assert "Messages:" in captured.out

    def test_info_empty_file(self, tmp_path, capsys):
        fpath = tmp_path / "empty.msgpack"
        fpath.touch()
        conf = InfoConf(input=str(fpath))
        Info(conf).run()

        captured = capsys.readouterr()
        assert "No records found" in captured.out
