"""Tests for persistent instance config storage."""

from pathlib import Path
from unittest.mock import patch

from boomrdbox.instances import (
    add_instance,
    get_instance,
    load_allowed_play_hosts,
    load_instances,
    remove_instance,
    save_allowed_play_hosts,
)
from boomrdbox.models import ReadInstanceConf, RedisConf, SshTunnelConf


def _patch_config_path(tmp_path: Path):
    return patch(
        "boomrdbox.instances.get_config_path",
        return_value=tmp_path / "config.toml",
    )


class TestLoadInstances:
    def test_empty_when_no_file(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            assert load_instances() == {}

    def test_add_and_load(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            conf = ReadInstanceConf(
                name="staging",
                redis=RedisConf(host="10.0.0.5", port=6380, db=2),
            )
            add_instance(conf)
            instances = load_instances()
            assert "staging" in instances
            loaded = instances["staging"]
            assert loaded.redis.host == "10.0.0.5"
            assert loaded.redis.port == 6380
            assert loaded.redis.db == 2
            assert loaded.ssh_tunnel is None

    def test_add_with_ssh(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            conf = ReadInstanceConf(
                name="prod",
                redis=RedisConf(host="redis-internal", port=6379),
                ssh_tunnel=SshTunnelConf(
                    ssh_host="bastion.corp.net",
                    ssh_port=2222,
                    ssh_user="deploy",
                    ssh_key_file="~/.ssh/id_ed25519",
                ),
            )
            add_instance(conf)
            loaded = get_instance("prod")
            assert loaded is not None
            assert loaded.ssh_tunnel is not None
            assert loaded.ssh_tunnel.ssh_host == "bastion.corp.net"
            assert loaded.ssh_tunnel.ssh_port == 2222
            assert loaded.ssh_tunnel.ssh_user == "deploy"
            assert loaded.ssh_tunnel.ssh_key_file == "~/.ssh/id_ed25519"

    def test_remove(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            add_instance(
                ReadInstanceConf(name="tmp", redis=RedisConf(host="x")),
            )
            assert get_instance("tmp") is not None
            remove_instance("tmp")
            assert get_instance("tmp") is None

    def test_overwrite_existing(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            add_instance(
                ReadInstanceConf(name="s", redis=RedisConf(host="old")),
            )
            add_instance(
                ReadInstanceConf(name="s", redis=RedisConf(host="new")),
            )
            loaded = get_instance("s")
            assert loaded is not None
            assert loaded.redis.host == "new"

    def test_get_not_found(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            assert get_instance("nonexistent") is None

    def test_remove_nonexistent_is_noop(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            remove_instance("nope")


class TestAllowedPlayHosts:
    def test_default(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            assert load_allowed_play_hosts() == ["redis"]

    def test_save_and_load(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            save_allowed_play_hosts(["redis", "redis-dev"])
            assert load_allowed_play_hosts() == ["redis", "redis-dev"]

    def test_hosts_persist_alongside_instances(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            save_allowed_play_hosts(["my-redis"])
            add_instance(
                ReadInstanceConf(name="foo", redis=RedisConf(host="10.0.0.1")),
            )
            assert load_allowed_play_hosts() == ["my-redis"]
            assert get_instance("foo") is not None

    def test_add_instance_with_password(self, tmp_path: Path) -> None:
        with _patch_config_path(tmp_path):
            conf = ReadInstanceConf(
                name="auth",
                redis=RedisConf(host="secured", password="secret123"),
            )
            add_instance(conf)
            loaded = get_instance("auth")
            assert loaded is not None
            assert loaded.redis.password == "secret123"
