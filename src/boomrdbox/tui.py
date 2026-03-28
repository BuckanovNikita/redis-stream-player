"""Interactive TUI for truncating recordings and setup wizard."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Center, Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Input,
    Label,
    ProgressBar,
    Static,
)

if TYPE_CHECKING:
    from boomrdbox.models import TruncateConf


@dataclass
class StreamScanResult:
    """Per-stream scan statistics."""

    name: str
    count: int
    first_ms: int
    last_ms: int


@dataclass
class TimelineMetadata:
    """Aggregated scan results across all streams."""

    file_path: str
    file_size: int
    streams: list[StreamScanResult] = field(default_factory=list)
    global_first_ms: int = 0
    global_last_ms: int = 0
    total_messages: int = 0


def ms_to_hms(ms_offset: int) -> str:
    """Format millisecond offset as ``HH:MM:SS.mmm``."""
    ms_offset = max(ms_offset, 0)
    total_seconds, millis = divmod(ms_offset, 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def parse_hms(text: str) -> int | None:
    """Parse ``HH:MM:SS``, ``HH:MM:SS.mmm``, ``MM:SS``, or ``SS`` into ms.

    Returns None if the format is invalid.
    """
    text = text.strip()
    if not text:
        return None

    millis = 0
    if "." in text:
        main_part, frac_part = text.rsplit(".", maxsplit=1)
        try:
            millis = int(frac_part.ljust(3, "0")[:3])
        except ValueError:
            return None
    else:
        main_part = text

    parts = main_part.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None

    if len(nums) == 3:
        hours, minutes, seconds = nums
    elif len(nums) == 2:
        hours = 0
        minutes, seconds = nums
    elif len(nums) == 1:
        hours = 0
        minutes = 0
        seconds = nums[0]
    else:
        return None

    if any(v < 0 for v in (hours, minutes, seconds, millis)):
        return None

    return (hours * 3600 + minutes * 60 + seconds) * 1000 + millis


def estimate_selected_messages(
    metadata: TimelineMetadata,
    left_ms: int,
    right_ms: int,
) -> int:
    """Estimate message count in [left_ms, right_ms] via linear interpolation."""
    total = 0
    for stream in metadata.streams:
        span = stream.last_ms - stream.first_ms
        if span <= 0 or stream.count <= 0:
            if stream.first_ms >= left_ms and stream.last_ms <= right_ms:
                total += stream.count
            continue
        clamp_left = max(left_ms, stream.first_ms)
        clamp_right = min(right_ms, stream.last_ms)
        if clamp_left >= clamp_right:
            continue
        fraction = (clamp_right - clamp_left) / span
        total += max(1, round(stream.count * fraction))
    return total


def scan_timeline(path: str) -> TimelineMetadata:
    """Scan a msgpack recording and return timeline metadata."""
    from boomrdbox.io import RecordReader

    reader = RecordReader(path)
    try:
        file_size = reader.file_size
    except OSError:
        file_size = 0

    stats: dict[str, StreamScanResult] = {}
    for record in reader:
        name = record.stream_name
        ms = record.message_id.ms
        if name not in stats:
            stats[name] = StreamScanResult(
                name=name,
                count=0,
                first_ms=ms,
                last_ms=ms,
            )
        s = stats[name]
        s.last_ms = ms
        s.count += 1

    streams = sorted(stats.values(), key=lambda s: s.name)
    total_messages = sum(s.count for s in streams)
    global_first = min((s.first_ms for s in streams), default=0)
    global_last = max((s.last_ms for s in streams), default=0)

    return TimelineMetadata(
        file_path=path,
        file_size=file_size,
        streams=streams,
        global_first_ms=global_first,
        global_last_ms=global_last,
        total_messages=total_messages,
    )


_STEP_NORMAL = 60_000
_STEP_COARSE = 600_000
_STEP_FINE = 1_000

_SELECTED_CHAR = "\u2593"
_EXCLUDED_CHAR = "\u2591"


class TimelineBar(Static):
    """Visual timeline bar showing selected range."""

    left_ms: reactive[int] = reactive(0)
    right_ms: reactive[int] = reactive(0)
    global_first_ms: reactive[int] = reactive(0)
    global_last_ms: reactive[int] = reactive(0)
    active_point: reactive[str] = reactive("left")

    def render(self) -> str:
        """Render the timeline bar."""
        width = max(self.size.width - 2, 10)
        span = self.global_last_ms - self.global_first_ms
        if span <= 0:
            return _SELECTED_CHAR * width

        left_frac = (self.left_ms - self.global_first_ms) / span
        right_frac = (self.right_ms - self.global_first_ms) / span
        left_pos = int(left_frac * width)
        right_pos = int(right_frac * width)
        left_pos = max(0, min(left_pos, width - 1))
        right_pos = max(left_pos, min(right_pos, width))

        bar = (
            _EXCLUDED_CHAR * left_pos
            + _SELECTED_CHAR * max(1, right_pos - left_pos)
            + _EXCLUDED_CHAR * (width - right_pos)
        )
        return bar[:width]

    def watch_left_ms(self) -> None:
        """React to left_ms changes."""
        self.refresh()

    def watch_right_ms(self) -> None:
        """React to right_ms changes."""
        self.refresh()


class ConfirmDialog(ModalScreen[bool]):
    """Modal confirmation before truncation."""

    BINDINGS: ClassVar[list[Binding | tuple[str, str] | tuple[str, str, str]]] = [
        Binding("y", "confirm", "Да"),
        Binding("n,escape", "cancel", "Нет"),
    ]

    def __init__(self, message: str) -> None:
        super().__init__()
        self._message = message

    def compose(self) -> ComposeResult:
        """Build the confirmation dialog layout."""
        with Vertical(id="confirm-dialog"):
            yield Label(self._message, id="confirm-message")
            yield Label("[Y] Да  [N/Esc] Нет", id="confirm-hint")

    def action_confirm(self) -> None:
        """User confirmed."""
        self.dismiss(result=True)

    def action_cancel(self) -> None:
        """User cancelled."""
        self.dismiss(result=False)


class TimeInputDialog(ModalScreen[int | None]):
    """Modal dialog for manual time entry."""

    def __init__(self, label: str, current_hms: str) -> None:
        super().__init__()
        self._label = label
        self._current_hms = current_hms

    def compose(self) -> ComposeResult:
        """Build the time input dialog layout."""
        with Vertical(id="time-input-dialog"):
            yield Label(
                f"{self._label} (HH:MM:SS.mmm / MM:SS / SS):",
                id="time-input-label",
            )
            yield Input(
                value=self._current_hms,
                placeholder="00:00:00.000",
                id="time-input-field",
            )
            yield Label("[Enter] Применить  [Esc] Отмена", id="time-input-hint")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter in the input field."""
        parsed = parse_hms(event.value)
        if parsed is not None:
            self.dismiss(parsed)
        else:
            self.notify("Неверный формат времени", severity="error")

    BINDINGS: ClassVar[list[Binding | tuple[str, str] | tuple[str, str, str]]] = [
        Binding("escape", "cancel_input", "Отмена"),
    ]

    def action_cancel_input(self) -> None:
        """Cancel the dialog."""
        self.dismiss(None)


class ScanScreen(Screen[TimelineMetadata]):
    """Progress screen while scanning the recording file."""

    def __init__(self, file_path: str) -> None:
        super().__init__()
        self._file_path = file_path

    def compose(self) -> ComposeResult:
        """Build the scan screen layout."""
        with Vertical(id="scan-container"):
            yield Label(f"Сканирование: {self._file_path}")
            yield ProgressBar(id="scan-progress", show_eta=False)

    def on_mount(self) -> None:
        """Start scanning when screen mounts."""
        self._do_scan()

    @work(thread=True)
    def _do_scan(self) -> None:
        metadata = scan_timeline(self._file_path)
        self.app.call_from_thread(self.dismiss, metadata)


class TimelineScreen(Screen[tuple[int, int] | None]):
    """Main interactive timeline screen."""

    BINDINGS: ClassVar[list[Binding | tuple[str, str] | tuple[str, str, str]]] = [
        Binding("left", "move(-1, 'normal')", "←1м"),
        Binding("right", "move(1, 'normal')", "→1м"),
        Binding("shift+left", "move(-1, 'coarse')", "←10м"),
        Binding("shift+right", "move(1, 'coarse')", "→10м"),
        Binding("ctrl+left", "move(-1, 'fine')", "←1с"),
        Binding("ctrl+right", "move(1, 'fine')", "→1с"),
        Binding("tab", "toggle_point", "Переключить"),
        Binding("t", "input_time", "Ввести время"),
        Binding("home", "jump_home", "Начало"),
        Binding("end", "jump_end", "Конец"),
        Binding("enter", "confirm", "Готово"),
        Binding("q,escape", "quit_tui", "Выход"),
    ]

    left_ms: reactive[int] = reactive(0)
    right_ms: reactive[int] = reactive(0)
    active_point: reactive[str] = reactive("left")

    def __init__(self, metadata: TimelineMetadata) -> None:
        super().__init__()
        self._metadata = metadata

    def compose(self) -> ComposeResult:
        """Build the timeline screen layout."""
        meta = self._metadata
        duration = ms_to_hms(meta.global_last_ms - meta.global_first_ms)
        with VerticalScroll(id="timeline-container"):
            yield Label(
                f"Файл: {meta.file_path} ({meta.file_size:,} байт)",
                id="file-info",
            )
            yield Label(f"Длительность: {duration}", id="duration-info")
            yield Label(
                f"Сообщений: {meta.total_messages:,}",
                id="messages-info",
            )

            table: DataTable[str] = DataTable(id="streams-table")
            table.add_columns("Стрим", "Сообщ.", "Начало", "Конец")
            base = meta.global_first_ms
            for stream in meta.streams:
                table.add_row(
                    stream.name,
                    f"{stream.count:,}",
                    ms_to_hms(stream.first_ms - base),
                    ms_to_hms(stream.last_ms - base),
                )
            yield table

            with Center():
                yield TimelineBar(id="timeline-bar")

            with Horizontal(id="markers-row"):
                yield Label("", id="left-marker")
                yield Label("", id="right-marker")

            yield Label("", id="selection-info")

        yield Footer()

    def on_mount(self) -> None:
        """Initialize reactive properties after mount."""
        self.left_ms = self._metadata.global_first_ms
        self.right_ms = self._metadata.global_last_ms
        self._update_display()

    def _update_display(self) -> None:
        base = self._metadata.global_first_ms
        left_label = self.query_one("#left-marker", Label)
        right_label = self.query_one("#right-marker", Label)
        selection_label = self.query_one("#selection-info", Label)
        bar = self.query_one("#timeline-bar", TimelineBar)

        active_indicator_l = " ◄" if self.active_point == "left" else ""
        active_indicator_r = " ◄" if self.active_point == "right" else ""

        left_label.update(f"  L: {ms_to_hms(self.left_ms - base)}{active_indicator_l}")
        right_label.update(
            f"  R: {ms_to_hms(self.right_ms - base)}{active_indicator_r}"
        )

        selected_duration = ms_to_hms(self.right_ms - self.left_ms)
        est_messages = estimate_selected_messages(
            self._metadata,
            self.left_ms,
            self.right_ms,
        )
        selection_label.update(
            f"  Выбрано: {selected_duration}  ~{est_messages:,} сообщ."
        )

        bar.left_ms = self.left_ms
        bar.right_ms = self.right_ms
        bar.global_first_ms = self._metadata.global_first_ms
        bar.global_last_ms = self._metadata.global_last_ms
        bar.active_point = self.active_point

    def _clamp(self, value: int) -> int:
        return max(
            self._metadata.global_first_ms,
            min(value, self._metadata.global_last_ms),
        )

    def action_move(self, direction: int, mode: str) -> None:
        """Move the active marker by the given step."""
        step_map = {
            "normal": _STEP_NORMAL,
            "coarse": _STEP_COARSE,
            "fine": _STEP_FINE,
        }
        step = step_map.get(mode, _STEP_NORMAL) * direction

        if self.active_point == "left":
            new_val = self._clamp(self.left_ms + step)
            self.left_ms = min(new_val, self.right_ms)
        else:
            new_val = self._clamp(self.right_ms + step)
            self.right_ms = max(new_val, self.left_ms)
        self._update_display()

    def action_toggle_point(self) -> None:
        """Switch active marker between left and right."""
        self.active_point = "right" if self.active_point == "left" else "left"
        self._update_display()

    def action_jump_home(self) -> None:
        """Jump active marker to the recording boundary."""
        if self.active_point == "left":
            self.left_ms = self._metadata.global_first_ms
        else:
            self.right_ms = self.left_ms
        self._update_display()

    def action_jump_end(self) -> None:
        """Jump active marker to the recording boundary."""
        if self.active_point == "right":
            self.right_ms = self._metadata.global_last_ms
        else:
            self.left_ms = self.right_ms
        self._update_display()

    def action_input_time(self) -> None:
        """Open manual time entry dialog for the active marker."""
        base = self._metadata.global_first_ms
        if self.active_point == "left":
            current_ms = self.left_ms - base
            label = "L (левая граница)"
        else:
            current_ms = self.right_ms - base
            label = "R (правая граница)"
        self.app.push_screen(
            TimeInputDialog(label, ms_to_hms(current_ms)),
            self._on_time_input,
        )

    def _on_time_input(self, result: int | None) -> None:
        if result is None:
            return
        absolute_ms = self._metadata.global_first_ms + result
        if self.active_point == "left":
            clamped = self._clamp(absolute_ms)
            self.left_ms = min(clamped, self.right_ms)
        else:
            clamped = self._clamp(absolute_ms)
            self.right_ms = max(clamped, self.left_ms)
        self._update_display()

    def action_confirm(self) -> None:
        """Show confirmation dialog."""
        base = self._metadata.global_first_ms
        est = estimate_selected_messages(
            self._metadata,
            self.left_ms,
            self.right_ms,
        )
        msg = (
            f"Обрезать запись?\n\n"
            f"Диапазон: {ms_to_hms(self.left_ms - base)}"
            f" — {ms_to_hms(self.right_ms - base)}\n"
            f"~{est:,} сообщений"
        )
        self.app.push_screen(ConfirmDialog(msg), self._on_confirm)

    def _on_confirm(self, confirmed: bool | None) -> None:
        if confirmed:
            self.dismiss((self.left_ms, self.right_ms))

    def action_quit_tui(self) -> None:
        """Exit without action."""
        self.dismiss(None)


class TruncateApp(App[None]):
    """Textual app for interactive recording truncation."""

    CSS = """
    #scan-container {
        align: center middle;
        height: 100%;
    }
    #timeline-container {
        padding: 1 2;
    }
    #timeline-bar {
        width: 100%;
        height: 1;
        margin: 1 0;
    }
    #markers-row {
        height: 1;
    }
    #left-marker {
        width: 1fr;
    }
    #right-marker {
        width: 1fr;
    }
    #selection-info {
        margin: 1 0 0 0;
    }
    #streams-table {
        height: auto;
        max-height: 10;
        margin: 1 0;
    }
    #confirm-dialog {
        align: center middle;
        width: 60;
        height: auto;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }
    #confirm-message {
        margin: 1 0;
    }
    #confirm-hint {
        margin: 1 0 0 0;
        text-align: center;
    }
    ConfirmDialog {
        align: center middle;
    }
    #time-input-dialog {
        align: center middle;
        width: 50;
        height: auto;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }
    #time-input-hint {
        margin: 1 0 0 0;
        text-align: center;
    }
    TimeInputDialog {
        align: center middle;
    }
    """

    def __init__(self, conf: TruncateConf) -> None:
        super().__init__()
        self._conf = conf
        self._metadata: TimelineMetadata | None = None

    def on_mount(self) -> None:
        """Push the scan screen on startup."""
        self.push_screen(ScanScreen(self._conf.input), self._on_scan_done)

    def _on_scan_done(self, metadata: TimelineMetadata | None) -> None:
        if metadata is None or not metadata.streams:
            self.notify("Файл пуст или не содержит записей.", severity="error")
            self.exit()
            return
        self._metadata = metadata
        self.push_screen(TimelineScreen(metadata), self._on_timeline_done)

    def _on_timeline_done(self, result: tuple[int, int] | None) -> None:
        if result is None:
            self.exit()
            return
        left_ms, right_ms = result
        self._conf.from_id = f"{left_ms}-0"
        self._conf.to_id = f"{right_ms}-0"
        self.exit()


class SetupApp(App[None]):
    """Interactive TUI for adding a read instance."""

    BINDINGS: ClassVar[list[Binding | tuple[str, str] | tuple[str, str, str]]] = [
        Binding("escape", "quit", "Выход"),
    ]

    CSS = """
    #setup-form {
        padding: 1 2;
        max-width: 80;
    }
    .setup-label {
        margin: 1 0 0 0;
    }
    #save-btn {
        margin: 2 0 0 0;
    }
    """

    def __init__(self, instance_name: str) -> None:
        super().__init__()
        self._instance_name = instance_name

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(f"Настройка инстанса: {self._instance_name}", classes="setup-label"),
            Label("Redis Host:", classes="setup-label"),
            Input(placeholder="10.0.0.5", id="host"),
            Label("Redis Port:", classes="setup-label"),
            Input(placeholder="6379", value="6379", id="port"),
            Label("Redis DB:", classes="setup-label"),
            Input(placeholder="0", value="0", id="db"),
            Label("Redis Password (опционально):", classes="setup-label"),
            Input(placeholder="", password=True, id="password"),
            Label("SSH Host (опционально):", classes="setup-label"),
            Input(placeholder="bastion.corp.net", id="ssh-host"),
            Label("SSH Port:", classes="setup-label"),
            Input(placeholder="22", value="22", id="ssh-port"),
            Label("SSH User:", classes="setup-label"),
            Input(placeholder="deploy", id="ssh-user"),
            Label("SSH Key File (опционально):", classes="setup-label"),
            Input(placeholder="~/.ssh/id_rsa", id="ssh-key"),
            Button("Сохранить", id="save-btn", variant="primary"),
            id="setup-form",
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "save-btn":
            return
        self._save_instance()

    def _save_instance(self) -> None:
        from boomrdbox.instances import add_instance
        from boomrdbox.models import ReadInstanceConf, RedisConf, SshTunnelConf

        host_input = self.query_one("#host", Input)
        if not host_input.value.strip():
            self.notify("Host обязателен", severity="error")
            return

        port_val = int(self.query_one("#port", Input).value or "6379")
        db_val = int(self.query_one("#db", Input).value or "0")
        password_val = self.query_one("#password", Input).value or None

        ssh_host = self.query_one("#ssh-host", Input).value.strip()
        ssh_tunnel: SshTunnelConf | None = None
        if ssh_host:
            ssh_tunnel = SshTunnelConf(
                ssh_host=ssh_host,
                ssh_port=int(self.query_one("#ssh-port", Input).value or "22"),
                ssh_user=self.query_one("#ssh-user", Input).value.strip(),
                ssh_key_file=self.query_one("#ssh-key", Input).value.strip() or None,
            )

        conf = ReadInstanceConf(
            name=self._instance_name,
            redis=RedisConf(
                host=host_input.value.strip(),
                port=port_val,
                db=db_val,
                password=password_val,
            ),
            ssh_tunnel=ssh_tunnel,
        )
        add_instance(conf)
        self.notify(f"Инстанс {self._instance_name!r} сохранён")
        self.exit()
