"""Infrastructure layer: persistent TOML config for read instances and play hosts."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import platformdirs

from boomrdbox.models import ReadInstanceConf, RedisConf, SshTunnelConf

_DEFAULT_PLAY_HOSTS: list[str] = ["redis"]


def _toml_value(v: object) -> str:
    """Serialize a single value to TOML literal."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(v, list):
        items = ", ".join(_toml_value(i) for i in v)
        return f"[{items}]"
    msg = f"Unsupported TOML value type: {type(v)}"
    raise TypeError(msg)


def _dumps_toml(data: dict[str, Any], prefix: str = "") -> str:
    """Serialize a nested dict to TOML string.

    Recursively handles arbitrarily nested tables as used by boomrdbox config.
    """
    lines: list[str] = []
    tables: list[tuple[str, dict[str, Any]]] = []

    for key, val in data.items():
        if isinstance(val, dict):
            tables.append((key, val))
        else:
            lines.append(f"{key} = {_toml_value(val)}")

    for table_key, table_val in tables:
        full_key = f"{prefix}{table_key}" if prefix else table_key
        nested: list[tuple[str, dict[str, Any]]] = []
        scalar_lines: list[str] = []
        for k, v in table_val.items():
            if isinstance(v, dict):
                nested.append((k, v))
            else:
                scalar_lines.append(f"{k} = {_toml_value(v)}")
        if scalar_lines:
            lines.append(f"\n[{full_key}]")
            lines.extend(scalar_lines)
        for nk, nv in nested:
            sub = _dumps_toml(nv, prefix=f"{full_key}.{nk}.")
            lines.append(sub)

    result = "\n".join(lines)
    if not prefix:
        result += "\n"
    return result


def get_config_path() -> Path:
    """Return the path to the persistent config file."""
    config_dir = Path(platformdirs.user_config_dir("boomrdbox"))
    return config_dir / "config.toml"


def load_config() -> dict[str, Any]:
    """Read the TOML config file, returning an empty dict if missing."""
    path = get_config_path()
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def save_config(data: dict[str, Any]) -> None:
    """Write config data to the TOML file, creating dirs if needed."""
    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dumps_toml(data), encoding="utf-8")


def _parse_instance(name: str, raw: dict[str, Any]) -> ReadInstanceConf:
    """Parse a single instance section from TOML data."""
    redis_raw = raw.get("redis", {})
    redis_conf = RedisConf(
        host=redis_raw.get("host", "localhost"),
        port=redis_raw.get("port", 6379),
        db=redis_raw.get("db", 0),
        password=redis_raw.get("password"),
    )
    ssh_tunnel: SshTunnelConf | None = None
    ssh_raw = raw.get("ssh_tunnel")
    if ssh_raw is not None:
        ssh_tunnel = SshTunnelConf(
            ssh_host=ssh_raw.get("ssh_host", ""),
            ssh_port=ssh_raw.get("ssh_port", 22),
            ssh_user=ssh_raw.get("ssh_user", ""),
            ssh_key_file=ssh_raw.get("ssh_key_file"),
            ssh_password=ssh_raw.get("ssh_password"),
        )
    return ReadInstanceConf(name=name, redis=redis_conf, ssh_tunnel=ssh_tunnel)


def _instance_to_dict(conf: ReadInstanceConf) -> dict[str, Any]:
    """Serialize a ReadInstanceConf to a TOML-compatible dict."""
    result: dict[str, Any] = {
        "redis": {
            "host": conf.redis.host,
            "port": conf.redis.port,
            "db": conf.redis.db,
        },
    }
    if conf.redis.password is not None:
        result["redis"]["password"] = conf.redis.password
    if conf.ssh_tunnel is not None:
        ssh: dict[str, Any] = {
            "ssh_host": conf.ssh_tunnel.ssh_host,
            "ssh_port": conf.ssh_tunnel.ssh_port,
            "ssh_user": conf.ssh_tunnel.ssh_user,
        }
        if conf.ssh_tunnel.ssh_key_file is not None:
            ssh["ssh_key_file"] = conf.ssh_tunnel.ssh_key_file
        if conf.ssh_tunnel.ssh_password is not None:
            ssh["ssh_password"] = conf.ssh_tunnel.ssh_password
        result["ssh_tunnel"] = ssh
    return result


def load_instances() -> dict[str, ReadInstanceConf]:
    """Load all named read instances from the config file."""
    data = load_config()
    instances_raw = data.get("instances", {})
    if not isinstance(instances_raw, dict):
        return {}
    return {
        name: _parse_instance(name, section)
        for name, section in instances_raw.items()
        if isinstance(section, dict)
    }


def get_instance(name: str) -> ReadInstanceConf | None:
    """Fetch a single read instance by name, or None if not found."""
    return load_instances().get(name)


def add_instance(conf: ReadInstanceConf) -> None:
    """Add or update a named read instance in the config file."""
    data = load_config()
    if "instances" not in data:
        data["instances"] = {}
    data["instances"][conf.name] = _instance_to_dict(conf)
    save_config(data)


def remove_instance(name: str) -> None:
    """Remove a named read instance from the config file."""
    data = load_config()
    instances = data.get("instances", {})
    if isinstance(instances, dict) and name in instances:
        del instances[name]
        save_config(data)


def load_allowed_play_hosts() -> list[str]:
    """Load the play command hostname whitelist, defaulting to ['redis']."""
    data = load_config()
    play_section = data.get("play", {})
    if not isinstance(play_section, dict):
        return list(_DEFAULT_PLAY_HOSTS)
    hosts = play_section.get("allowed_hosts")
    if not isinstance(hosts, list):
        return list(_DEFAULT_PLAY_HOSTS)
    return [str(h) for h in hosts]


def save_allowed_play_hosts(hosts: list[str]) -> None:
    """Save the play command hostname whitelist to the config file."""
    data = load_config()
    if "play" not in data:
        data["play"] = {}
    data["play"]["allowed_hosts"] = hosts
    save_config(data)
