"""CLI entry points using Hydra for configuration."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable

import hydra
from omegaconf import DictConfig, OmegaConf

from redis_stream_player.models import (
    ConvertConf,
    InfoConf,
    PlayConf,
    RecordConf,
    TruncateConf,
)

logger = logging.getLogger(__name__)


def _run_safe(func: Callable[[], None]) -> None:
    """Run a function with top-level error handling."""
    try:
        func()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(130)
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)


def _setup_logging(*, verbose: bool) -> None:
    """Configure logging level based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@hydra.main(
    config_path="conf",
    config_name="record",
    version_base=None,
)
def record_main(cfg: DictConfig) -> None:
    """Record Redis stream messages to a msgpack file."""
    from redis_stream_player.recorder import Recorder

    schema = OmegaConf.structured(RecordConf)
    merged = OmegaConf.merge(schema, cfg)
    conf = cast("RecordConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Recorder(conf).run)


@hydra.main(
    config_path="conf",
    config_name="play",
    version_base=None,
)
def play_main(cfg: DictConfig) -> None:
    """Play back recorded Redis stream messages."""
    from redis_stream_player.player import Player

    schema = OmegaConf.structured(PlayConf)
    merged = OmegaConf.merge(schema, cfg)
    conf = cast("PlayConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Player(conf).run)


@hydra.main(
    config_path="conf",
    config_name="convert",
    version_base=None,
)
def convert_main(cfg: DictConfig) -> None:
    """Convert a msgpack recording to Parquet or CSV."""
    from redis_stream_player.tools import Converter

    schema = OmegaConf.structured(ConvertConf)
    merged = OmegaConf.merge(schema, cfg)
    conf = cast("ConvertConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Converter(conf).run)


@hydra.main(
    config_path="conf",
    config_name="truncate",
    version_base=None,
)
def truncate_main(cfg: DictConfig) -> None:
    """Truncate a recording by message ID range."""
    from redis_stream_player.tools import Truncator

    schema = OmegaConf.structured(TruncateConf)
    merged = OmegaConf.merge(schema, cfg)
    conf = cast("TruncateConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Truncator(conf).run)


@hydra.main(
    config_path="conf",
    config_name="info",
    version_base=None,
)
def info_main(cfg: DictConfig) -> None:
    """Show statistics about a recording file."""
    from redis_stream_player.tools import Info

    schema = OmegaConf.structured(InfoConf)
    merged = OmegaConf.merge(schema, cfg)
    conf = cast("InfoConf", OmegaConf.to_object(merged))
    _setup_logging(verbose=conf.verbose)
    _run_safe(Info(conf).run)
