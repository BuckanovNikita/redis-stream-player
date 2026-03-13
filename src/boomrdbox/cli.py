"""CLI entry point: ``boomrdbox <subcommand>``."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable

from hydra_zen import zen
from loguru import logger
from omegaconf import DictConfig, OmegaConf

from boomrdbox._config import store
from boomrdbox.models import (
    ConvertConf,
    InfoConf,
    PlayConf,
    RecordConf,
    TruncateConf,
)


def _run_safe(func: Callable[[], None]) -> None:
    """Run a function with top-level error handling."""
    try:
        func()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(130)
    except Exception:  # noqa: BLE001 — top-level safety net
        logger.exception("Fatal error")
        sys.exit(1)


def _setup_logging(*, verbose: bool) -> None:
    """Configure loguru level based on verbosity."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=level)


def _record_task(zen_cfg: DictConfig) -> None:
    """Record Redis stream messages to a msgpack file."""
    from boomrdbox.recorder import Recorder

    schema = OmegaConf.structured(RecordConf)
    merged = OmegaConf.merge(schema, zen_cfg)
    conf = cast("RecordConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Recorder(conf).run)


def _play_task(zen_cfg: DictConfig) -> None:
    """Play back recorded Redis stream messages."""
    from boomrdbox.player import Player

    schema = OmegaConf.structured(PlayConf)
    merged = OmegaConf.merge(schema, zen_cfg)
    conf = cast("PlayConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Player(conf).run)


def _convert_task(zen_cfg: DictConfig) -> None:
    """Convert a msgpack recording to Parquet or CSV."""
    from boomrdbox.tools import Converter

    schema = OmegaConf.structured(ConvertConf)
    merged = OmegaConf.merge(schema, zen_cfg)
    conf = cast("ConvertConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Converter(conf).run)


def _truncate_task(zen_cfg: DictConfig) -> None:
    """Truncate a recording by message ID range."""
    schema = OmegaConf.structured(TruncateConf)
    merged = OmegaConf.merge(schema, zen_cfg)
    conf = cast("TruncateConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)

    if conf.interactive:
        from boomrdbox.tui import TruncateApp

        logger.remove()
        TruncateApp(conf).run()
        if conf.from_id is None and conf.to_id is None:
            return
        _setup_logging(verbose=conf.verbose)

    from boomrdbox.tools import Truncator

    _run_safe(Truncator(conf).run)


def _info_task(zen_cfg: DictConfig) -> None:
    """Show statistics about a recording file."""
    from boomrdbox.tools import Info

    schema = OmegaConf.structured(InfoConf)
    merged = OmegaConf.merge(schema, zen_cfg)
    conf = cast("InfoConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Info(conf).run)


_SUBCOMMANDS: dict[str, tuple[Callable[[DictConfig], None], str]] = {
    "record": (_record_task, "record"),
    "play": (_play_task, "play"),
    "convert": (_convert_task, "convert"),
    "truncate": (_truncate_task, "truncate"),
    "info": (_info_task, "info"),
}

_USAGE = """\
Usage: boomrdbox <command> [hydra overrides]

Commands:
  record     Record Redis stream messages to a msgpack file
  play       Play back recorded Redis stream messages
  convert    Convert a msgpack recording to Parquet or CSV
  truncate   Truncate a recording by message ID range
  info       Show statistics about a recording file
"""


def main() -> None:
    """Dispatch to the correct subcommand via hydra-zen."""
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        sys.stdout.write(_USAGE)
        sys.exit(0)

    subcmd = sys.argv[1]
    if subcmd not in _SUBCOMMANDS:
        sys.stderr.write(f"Unknown command: {subcmd!r}\n\n")
        sys.stderr.write(_USAGE)
        sys.exit(1)

    task_fn, config_name = _SUBCOMMANDS[subcmd]

    # Remove the subcommand so Hydra only sees overrides
    sys.argv.pop(1)

    store.add_to_hydra_store()
    zen(task_fn).hydra_main(
        config_name=config_name,
        config_path=None,
        version_base=None,
    )
