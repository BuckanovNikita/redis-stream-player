"""CLI entry point: ``boomrdbox <subcommand>``."""

from __future__ import annotations

import argparse
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
  setup      Manage read instances and play-host whitelist
"""


def _build_setup_parser() -> argparse.ArgumentParser:
    """Build argparse parser for the setup subcommand."""
    parser = argparse.ArgumentParser(
        prog="boomrdbox setup",
        description="Manage read instances and play-host whitelist",
    )
    sub = parser.add_subparsers(dest="action")

    add_p = sub.add_parser("add", help="Add or update a read instance")
    add_p.add_argument("name", help="Instance name")
    add_p.add_argument("--host", required=True, help="Redis host")
    add_p.add_argument("--port", type=int, default=6379, help="Redis port")
    add_p.add_argument("--db", type=int, default=0, help="Redis DB index")
    add_p.add_argument("--password", default=None, help="Redis password")
    add_p.add_argument("--ssh-host", default=None, help="SSH tunnel host")
    add_p.add_argument("--ssh-port", type=int, default=22, help="SSH port")
    add_p.add_argument("--ssh-user", default=None, help="SSH username")
    add_p.add_argument("--ssh-key", default=None, help="SSH private key file")
    add_p.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive TUI mode",
    )

    rm_p = sub.add_parser("remove", help="Remove a read instance")
    rm_p.add_argument("name", help="Instance name to remove")

    sub.add_parser("list", help="List all read instances")

    ph = sub.add_parser("play-hosts", help="Manage play-host whitelist")
    ph_sub = ph.add_subparsers(dest="ph_action")
    ph_add = ph_sub.add_parser("add", help="Add a hostname to whitelist")
    ph_add.add_argument("hostname", help="Hostname to allow")
    ph_rm = ph_sub.add_parser("remove", help="Remove a hostname from whitelist")
    ph_rm.add_argument("hostname", help="Hostname to remove")
    ph_sub.add_parser("list", help="List allowed hostnames")

    return parser


def _setup_add(ns: argparse.Namespace) -> None:
    """Handle 'boomrdbox setup add'."""
    from boomrdbox.instances import add_instance
    from boomrdbox.models import ReadInstanceConf, RedisConf, SshTunnelConf

    if ns.interactive:
        from boomrdbox.tui import SetupApp

        app = SetupApp(ns.name)
        app.run()
        return

    ssh_tunnel: SshTunnelConf | None = None
    if ns.ssh_host is not None:
        ssh_tunnel = SshTunnelConf(
            ssh_host=ns.ssh_host,
            ssh_port=ns.ssh_port,
            ssh_user=ns.ssh_user or "",
            ssh_key_file=ns.ssh_key,
        )

    conf = ReadInstanceConf(
        name=ns.name,
        redis=RedisConf(
            host=ns.host,
            port=ns.port,
            db=ns.db,
            password=ns.password,
        ),
        ssh_tunnel=ssh_tunnel,
    )
    add_instance(conf)
    logger.info(f"Instance {ns.name!r} saved")


def _setup_remove(ns: argparse.Namespace) -> None:
    """Handle 'boomrdbox setup remove'."""
    from boomrdbox.instances import get_instance, remove_instance

    if get_instance(ns.name) is None:
        logger.error(
            f"Instance {ns.name!r} not found."
            " Run 'boomrdbox setup list' to see configured instances.",
        )
        sys.exit(1)
    remove_instance(ns.name)
    logger.info(f"Instance {ns.name!r} removed")


def _setup_list() -> None:
    """Handle 'boomrdbox setup list'."""
    from boomrdbox.instances import load_instances

    instances = load_instances()
    if not instances:
        logger.info("No read instances configured")
        return
    for name, inst in instances.items():
        ssh_info = ""
        if inst.ssh_tunnel is not None and inst.ssh_tunnel.ssh_host:
            tunnel = inst.ssh_tunnel
            ssh_info = (
                f" via ssh://{tunnel.ssh_user}@{tunnel.ssh_host}:{tunnel.ssh_port}"
            )
        logger.info(
            f"  {name}: {inst.redis.host}:{inst.redis.port}"
            f" db={inst.redis.db}{ssh_info}",
        )


def _setup_play_hosts(ns: argparse.Namespace) -> None:
    """Handle 'boomrdbox setup play-hosts' subcommands."""
    from boomrdbox.instances import load_allowed_play_hosts, save_allowed_play_hosts

    if ns.ph_action == "list" or ns.ph_action is None:
        hosts = load_allowed_play_hosts()
        logger.info(f"Allowed play hosts: {hosts}")
    elif ns.ph_action == "add":
        hosts = load_allowed_play_hosts()
        if ns.hostname not in hosts:
            hosts.append(ns.hostname)
            save_allowed_play_hosts(hosts)
        logger.info(f"Host {ns.hostname!r} added to whitelist")
    elif ns.ph_action == "remove":
        hosts = load_allowed_play_hosts()
        if ns.hostname in hosts:
            hosts.remove(ns.hostname)
            save_allowed_play_hosts(hosts)
            logger.info(f"Host {ns.hostname!r} removed from whitelist")
        else:
            logger.error(f"Host {ns.hostname!r} not in whitelist")
            sys.exit(1)


def _setup_command(argv: list[str]) -> None:
    """Handle the setup subcommand for managing read instances."""
    _setup_logging(verbose=False)
    parser = _build_setup_parser()
    ns = parser.parse_args(argv)

    if ns.action is None:
        parser.print_help()
        return

    if ns.action == "add":
        _setup_add(ns)
    elif ns.action == "remove":
        _setup_remove(ns)
    elif ns.action == "list":
        _setup_list()
    elif ns.action == "play-hosts":
        _setup_play_hosts(ns)


def main() -> None:
    """Dispatch to the correct subcommand via hydra-zen."""
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        sys.stdout.write(_USAGE)
        sys.exit(0)

    subcmd = sys.argv[1]

    if subcmd == "setup":
        _setup_command(sys.argv[2:])
        return

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
