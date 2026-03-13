"""Tests for TUI module: pure functions and Textual app."""

from pathlib import Path

import pytest

from boomrdbox.models import TruncateConf
from boomrdbox.tui import (
    StreamScanResult,
    TimeInputDialog,
    TimelineMetadata,
    TimelineScreen,
    TruncateApp,
    estimate_selected_messages,
    ms_to_hms,
    parse_hms,
    scan_timeline,
)


class TestMsToHms:
    def test_zero(self):
        assert ms_to_hms(0) == "00:00:00.000"

    def test_one_hour(self):
        assert ms_to_hms(3_600_000) == "01:00:00.000"

    def test_mixed(self):
        ms = 1 * 3_600_000 + 23 * 60_000 + 45 * 1000 + 678
        assert ms_to_hms(ms) == "01:23:45.678"

    def test_negative_clamps_to_zero(self):
        assert ms_to_hms(-500) == "00:00:00.000"

    def test_milliseconds_only(self):
        assert ms_to_hms(123) == "00:00:00.123"

    def test_large_value(self):
        ms = 99 * 3_600_000 + 59 * 60_000 + 59 * 1000 + 999
        assert ms_to_hms(ms) == "99:59:59.999"


class TestParseHms:
    def test_full_format(self):
        assert parse_hms("01:23:45.678") == 1 * 3_600_000 + 23 * 60_000 + 45_678

    def test_no_millis(self):
        assert parse_hms("01:23:45") == 1 * 3_600_000 + 23 * 60_000 + 45_000

    def test_mm_ss(self):
        assert parse_hms("05:30") == 5 * 60_000 + 30_000

    def test_mm_ss_millis(self):
        assert parse_hms("05:30.500") == 5 * 60_000 + 30_500

    def test_seconds_only(self):
        assert parse_hms("90") == 90_000

    def test_seconds_with_millis(self):
        assert parse_hms("90.1") == 90_100

    def test_zero(self):
        assert parse_hms("00:00:00.000") == 0

    def test_empty_string(self):
        assert parse_hms("") is None

    def test_invalid_format(self):
        assert parse_hms("abc") is None

    def test_too_many_parts(self):
        assert parse_hms("1:2:3:4") is None

    def test_whitespace_trimmed(self):
        assert parse_hms("  01:00:00  ") == 3_600_000

    def test_short_millis_padded(self):
        assert parse_hms("0:0:0.1") == 100

    def test_long_millis_truncated(self):
        assert parse_hms("0:0:0.1234") == 123

    def test_roundtrip(self):
        original_ms = 1 * 3_600_000 + 23 * 60_000 + 45_678
        assert parse_hms(ms_to_hms(original_ms)) == original_ms


class TestEstimateSelectedMessages:
    @pytest.fixture
    def metadata(self):
        return TimelineMetadata(
            file_path="test.msgpack",
            file_size=1000,
            streams=[
                StreamScanResult(
                    name="sensor:imu",
                    count=1000,
                    first_ms=0,
                    last_ms=10000,
                ),
                StreamScanResult(
                    name="sensor:gps",
                    count=500,
                    first_ms=0,
                    last_ms=10000,
                ),
            ],
            global_first_ms=0,
            global_last_ms=10000,
            total_messages=1500,
        )

    def test_full_range(self, metadata):
        result = estimate_selected_messages(metadata, 0, 10000)
        assert result == 1500

    def test_half_range(self, metadata):
        result = estimate_selected_messages(metadata, 0, 5000)
        assert result == 750

    def test_empty_range(self, metadata):
        result = estimate_selected_messages(metadata, 5000, 5000)
        assert result == 0

    def test_outside_range(self, metadata):
        result = estimate_selected_messages(metadata, 20000, 30000)
        assert result == 0

    def test_partial_overlap(self, metadata):
        result = estimate_selected_messages(metadata, 2000, 8000)
        assert result == 900

    def test_zero_span_stream_inside(self):
        meta = TimelineMetadata(
            file_path="t.msgpack",
            file_size=100,
            streams=[
                StreamScanResult(name="a", count=10, first_ms=500, last_ms=500),
            ],
            global_first_ms=0,
            global_last_ms=1000,
            total_messages=10,
        )
        assert estimate_selected_messages(meta, 0, 1000) == 10

    def test_zero_span_stream_outside(self):
        meta = TimelineMetadata(
            file_path="t.msgpack",
            file_size=100,
            streams=[
                StreamScanResult(name="a", count=10, first_ms=500, last_ms=500),
            ],
            global_first_ms=0,
            global_last_ms=1000,
            total_messages=10,
        )
        assert estimate_selected_messages(meta, 0, 100) == 0


class TestScanTimeline:
    def test_scan_sample(self, sample_msgpack):
        meta = scan_timeline(str(sample_msgpack))
        assert meta.total_messages == 5
        assert len(meta.streams) == 3
        assert meta.global_first_ms == 1709312000000
        assert meta.global_last_ms == 1709312000500
        assert meta.file_size > 0

        names = [s.name for s in meta.streams]
        assert "sensor:imu" in names
        assert "sensor:gps" in names
        assert "sensor:camera" in names

        imu = next(s for s in meta.streams if s.name == "sensor:imu")
        assert imu.count == 3
        assert imu.first_ms == 1709312000000
        assert imu.last_ms == 1709312000500

    def test_scan_empty_file(self, tmp_path):
        empty = tmp_path / "empty.msgpack"
        empty.touch()
        meta = scan_timeline(str(empty))
        assert meta.total_messages == 0
        assert meta.streams == []

    def test_scan_nonexistent_file(self, tmp_path):
        meta = scan_timeline(str(tmp_path / "missing.msgpack"))
        assert meta.total_messages == 0


@pytest.mark.asyncio
class TestTuiApp:
    async def test_scan_transitions_to_timeline(self, sample_msgpack):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(sample_msgpack.parent / "out.msgpack"),
        )
        conf.interactive = True

        app = TruncateApp(conf)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()
            assert isinstance(app.screen, TimelineScreen)

    async def test_keyboard_navigation(self, sample_msgpack):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(sample_msgpack.parent / "out.msgpack"),
        )
        conf.interactive = True

        app = TruncateApp(conf)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, TimelineScreen)

            initial_left = screen.left_ms
            initial_right = screen.right_ms

            await pilot.press("ctrl+right")
            assert screen.left_ms == min(
                initial_left + 1000,
                initial_right,
            )

            await pilot.press("tab")
            assert screen.active_point == "right"

            await pilot.press("ctrl+left")
            assert screen.right_ms == max(
                initial_right - 1000,
                screen.left_ms,
            )

    async def test_quit_without_action(self, sample_msgpack):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(sample_msgpack.parent / "out.msgpack"),
        )
        conf.interactive = True
        output_path = Path(conf.output)

        app = TruncateApp(conf)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("q")

        assert not output_path.exists()  # noqa: ASYNC240

    async def test_confirm_sets_range_on_conf(self, sample_msgpack):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(sample_msgpack.parent / "out.msgpack"),
        )
        conf.interactive = True
        assert conf.from_id is None
        assert conf.to_id is None

        app = TruncateApp(conf)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()

        assert conf.from_id == "1709312000000-0"
        assert conf.to_id == "1709312000500-0"

    async def test_time_input_dialog(self, sample_msgpack):
        conf = TruncateConf(
            input=str(sample_msgpack),
            output=str(sample_msgpack.parent / "out.msgpack"),
        )
        conf.interactive = True

        app = TruncateApp(conf)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, TimelineScreen)

            await pilot.press("t")
            await pilot.pause()
            assert isinstance(app.screen, TimeInputDialog)

            await pilot.press("escape")
            await pilot.pause()
            assert isinstance(app.screen, TimelineScreen)
