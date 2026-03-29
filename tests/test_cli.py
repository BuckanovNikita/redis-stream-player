"""Tests for the CLI entry point."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from omegaconf import DictConfig

from boomrdbox.cli import (
    _build_setup_parser,
    _info_task,
    _play_task,
    _record_task,
    _run_safe,
    _setup_command,
    _setup_logging,
    _truncate_task,
    main,
)


class TestRunSafe:
    def test_keyboard_interrupt_exits_130(self):
        def raise_ki():
            raise KeyboardInterrupt

        with pytest.raises(SystemExit) as exc_info:
            _run_safe(raise_ki)
        assert exc_info.value.code == 130

    def test_exception_exits_1(self):
        def raise_exc():
            msg = "boom"
            raise RuntimeError(msg)

        with pytest.raises(SystemExit) as exc_info:
            _run_safe(raise_exc)
        assert exc_info.value.code == 1

    def test_success_no_exit(self):
        called = False

        def ok():
            nonlocal called
            called = True

        _run_safe(ok)
        assert called


class TestSetupLogging:
    def test_verbose_sets_debug(self):
        _setup_logging(verbose=True)

    def test_default_sets_info(self):
        _setup_logging(verbose=False)


class TestRecordTask:
    @patch("boomrdbox.recorder.Recorder")
    def test_dispatches_to_recorder(self, mock_cls: Any, hydra_record_cfg: DictConfig):
        _record_task(hydra_record_cfg)
        mock_cls.assert_called_once()
        mock_cls.return_value.run.assert_called_once()


class TestPlayTask:
    @patch("boomrdbox.player.Player")
    def test_dispatches_to_player(self, mock_cls: Any, hydra_play_cfg: DictConfig):
        _play_task(hydra_play_cfg)
        mock_cls.assert_called_once()
        mock_cls.return_value.run.assert_called_once()


class TestConvertTask:
    @patch("boomrdbox.tools.Converter")
    def test_dispatches_to_converter(
        self, mock_cls: Any, hydra_convert_cfg: DictConfig
    ):
        from boomrdbox.cli import _convert_task

        _convert_task(hydra_convert_cfg)
        mock_cls.assert_called_once()
        mock_cls.return_value.run.assert_called_once()


class TestInfoTask:
    @patch("boomrdbox.tools.Info")
    def test_dispatches_to_info(self, mock_cls: Any, hydra_info_cfg: DictConfig):
        _info_task(hydra_info_cfg)
        mock_cls.assert_called_once()
        mock_cls.return_value.run.assert_called_once()


class TestTruncateTask:
    @patch("boomrdbox.tools.Truncator")
    def test_non_interactive(self, mock_cls: Any, hydra_truncate_cfg: DictConfig):
        _truncate_task(hydra_truncate_cfg)
        mock_cls.assert_called_once()
        mock_cls.return_value.run.assert_called_once()

    @patch("boomrdbox.tui.TruncateApp")
    def test_interactive_no_selection(
        self, mock_app_cls: Any, hydra_truncate_cfg: DictConfig
    ):
        from omegaconf import OmegaConf

        OmegaConf.update(hydra_truncate_cfg, "interactive", value=True)
        _truncate_task(hydra_truncate_cfg)
        mock_app_cls.assert_called_once()
        mock_app_cls.return_value.run.assert_called_once()

    @patch("boomrdbox.tools.Truncator")
    @patch("boomrdbox.tui.TruncateApp")
    def test_interactive_with_selection(
        self,
        mock_app_cls: Any,
        mock_truncator_cls: Any,
        hydra_truncate_cfg: DictConfig,
    ):
        from omegaconf import OmegaConf

        OmegaConf.update(hydra_truncate_cfg, "interactive", value=True)

        def fake_run(conf_arg: Any) -> None:
            conf_arg.from_id = "100-0"
            conf_arg.to_id = "200-0"

        def app_init(conf: Any) -> MagicMock:
            app = MagicMock()
            app.run.side_effect = lambda: fake_run(conf)
            return app

        mock_app_cls.side_effect = app_init
        _truncate_task(hydra_truncate_cfg)
        mock_truncator_cls.assert_called_once()
        mock_truncator_cls.return_value.run.assert_called_once()


class TestBuildSetupParser:
    def test_parser_structure(self):
        parser = _build_setup_parser()
        assert isinstance(parser, argparse.ArgumentParser)

        ns = parser.parse_args(["add", "staging", "--host", "10.0.0.5"])
        assert ns.action == "add"
        assert ns.name == "staging"
        assert ns.host == "10.0.0.5"

    def test_parser_remove(self):
        parser = _build_setup_parser()
        ns = parser.parse_args(["remove", "staging"])
        assert ns.action == "remove"
        assert ns.name == "staging"

    def test_parser_list(self):
        parser = _build_setup_parser()
        ns = parser.parse_args(["list"])
        assert ns.action == "list"

    def test_parser_play_hosts_add(self):
        parser = _build_setup_parser()
        ns = parser.parse_args(["play-hosts", "add", "myhost"])
        assert ns.action == "play-hosts"
        assert ns.ph_action == "add"
        assert ns.hostname == "myhost"

    def test_parser_play_hosts_remove(self):
        parser = _build_setup_parser()
        ns = parser.parse_args(["play-hosts", "remove", "myhost"])
        assert ns.ph_action == "remove"

    def test_parser_play_hosts_list(self):
        parser = _build_setup_parser()
        ns = parser.parse_args(["play-hosts", "list"])
        assert ns.ph_action == "list"


class TestSetupAdd:
    @patch("boomrdbox.instances.add_instance")
    def test_add_basic(self, mock_add: Any):
        _setup_command(["add", "staging", "--host", "10.0.0.5"])
        mock_add.assert_called_once()
        conf = mock_add.call_args[0][0]
        assert conf.name == "staging"
        assert conf.redis.host == "10.0.0.5"
        assert conf.ssh_tunnel is None

    @patch("boomrdbox.instances.add_instance")
    def test_add_with_ssh(self, mock_add: Any):
        _setup_command(
            [
                "add",
                "prod",
                "--host",
                "10.0.0.5",
                "--ssh-host",
                "bastion.corp",
                "--ssh-user",
                "deploy",
                "--ssh-key",
                "/home/user/.ssh/id_rsa",
            ]
        )
        mock_add.assert_called_once()
        conf = mock_add.call_args[0][0]
        assert conf.ssh_tunnel is not None
        assert conf.ssh_tunnel.ssh_host == "bastion.corp"
        assert conf.ssh_tunnel.ssh_user == "deploy"

    @patch("boomrdbox.tui.SetupApp")
    def test_add_interactive(self, mock_app_cls: Any):
        _setup_command(["add", "staging", "--host", "x", "--interactive"])
        mock_app_cls.assert_called_once_with("staging")
        mock_app_cls.return_value.run.assert_called_once()


class TestSetupRemove:
    @patch("boomrdbox.instances.remove_instance")
    @patch("boomrdbox.instances.get_instance")
    def test_remove_existing(self, mock_get: Any, mock_remove: Any):
        mock_get.return_value = MagicMock()
        _setup_command(["remove", "staging"])
        mock_remove.assert_called_once_with("staging")

    @patch("boomrdbox.instances.get_instance", return_value=None)
    def test_remove_not_found(self, _mock_get: Any):
        with pytest.raises(SystemExit) as exc_info:
            _setup_command(["remove", "nonexistent"])
        assert exc_info.value.code == 1


class TestSetupList:
    @patch("boomrdbox.instances.load_instances", return_value={})
    def test_list_empty(self, _mock_load: Any):
        _setup_command(["list"])

    @patch("boomrdbox.instances.load_instances")
    def test_list_with_instances(self, mock_load: Any):
        from boomrdbox.models import ReadInstanceConf, RedisConf

        mock_load.return_value = {
            "staging": ReadInstanceConf(
                name="staging",
                redis=RedisConf(host="10.0.0.5", port=6379),
            ),
        }
        _setup_command(["list"])

    @patch("boomrdbox.instances.load_instances")
    def test_list_with_ssh_tunnel(self, mock_load: Any):
        from boomrdbox.models import ReadInstanceConf, RedisConf, SshTunnelConf

        mock_load.return_value = {
            "prod": ReadInstanceConf(
                name="prod",
                redis=RedisConf(host="10.0.0.5"),
                ssh_tunnel=SshTunnelConf(
                    ssh_host="bastion",
                    ssh_port=22,
                    ssh_user="deploy",
                ),
            ),
        }
        _setup_command(["list"])


class TestSetupPlayHosts:
    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis"],
    )
    def test_list_hosts(self, _mock_load: Any):
        _setup_command(["play-hosts", "list"])

    @patch("boomrdbox.instances.save_allowed_play_hosts")
    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis"],
    )
    def test_add_host(self, _mock_load: Any, mock_save: Any):
        _setup_command(["play-hosts", "add", "localhost"])
        mock_save.assert_called_once_with(["redis", "localhost"])

    @patch("boomrdbox.instances.save_allowed_play_hosts")
    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis", "localhost"],
    )
    def test_add_duplicate(self, _mock_load: Any, mock_save: Any):
        _setup_command(["play-hosts", "add", "localhost"])
        mock_save.assert_not_called()

    @patch("boomrdbox.instances.save_allowed_play_hosts")
    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis", "localhost"],
    )
    def test_remove_host(self, _mock_load: Any, mock_save: Any):
        _setup_command(["play-hosts", "remove", "localhost"])
        mock_save.assert_called_once_with(["redis"])

    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis"],
    )
    def test_remove_missing_exits(self, _mock_load: Any):
        with pytest.raises(SystemExit) as exc_info:
            _setup_command(["play-hosts", "remove", "nonexistent"])
        assert exc_info.value.code == 1

    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis"],
    )
    def test_no_action_lists(self, _mock_load: Any):
        _setup_command(["play-hosts"])


class TestSetupCommand:
    def test_no_action_prints_help(self):
        _setup_command([])

    @patch("boomrdbox.instances.add_instance")
    def test_dispatches_add(self, mock_add: Any):
        _setup_command(["add", "test", "--host", "x"])
        mock_add.assert_called_once()

    @patch("boomrdbox.instances.remove_instance")
    @patch("boomrdbox.instances.get_instance")
    def test_dispatches_remove(self, mock_get: Any, mock_rm: Any):
        mock_get.return_value = MagicMock()
        _setup_command(["remove", "test"])
        mock_rm.assert_called_once()

    @patch("boomrdbox.instances.load_instances", return_value={})
    def test_dispatches_list(self, _mock: Any):
        _setup_command(["list"])

    @patch(
        "boomrdbox.instances.load_allowed_play_hosts",
        return_value=["redis"],
    )
    def test_dispatches_play_hosts(self, _mock: Any):
        _setup_command(["play-hosts", "list"])


class TestMain:
    def test_help_flag(self):
        with (
            patch.object(sys, "argv", ["boomrdbox", "--help"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
        assert exc_info.value.code == 0

    def test_no_args(self):
        with (
            patch.object(sys, "argv", ["boomrdbox"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
        assert exc_info.value.code == 0

    def test_unknown_command(self):
        with (
            patch.object(sys, "argv", ["boomrdbox", "bogus"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
        assert exc_info.value.code == 1

    @patch("boomrdbox.cli._setup_command")
    def test_setup_dispatches(self, mock_setup: Any):
        with patch.object(sys, "argv", ["boomrdbox", "setup", "list"]):
            main()
        mock_setup.assert_called_once_with(["list"])
