from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from qlever.commands.upgrade_index import UpgradeIndexCommand


def make_args() -> MagicMock:
    args = MagicMock()
    args.name = "TestName"
    args.index_binary = "qlever-index"
    args.upgrade_index_binary = None
    args.system = "native"
    args.image = "test_image"
    args.index_container = "test_container"
    args.show = False
    return args


@patch("qlever.commands.upgrade_index.run_command")
@patch("qlever.commands.upgrade_index.binary_exists")
@patch("qlever.commands.upgrade_index.Containerize")
class TestUpgradeIndexCommand(unittest.TestCase):
    # Native system, index binary from the PATH: the upgrade binary is
    # `qlever-upgrade-index` from the PATH.
    def test_execute_native(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = True

        result = UpgradeIndexCommand().execute(args)

        self.assertTrue(result)
        mock_run_command.assert_called_once_with(
            "qlever-upgrade-index TestName"
            " 2>&1 | tee TestName.upgrade-index-log.txt",
            show_output=True,
        )

    # Index binary given with a path: the upgrade binary is taken from the
    # same directory.
    def test_execute_binary_next_to_index_binary(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        args.index_binary = "/test/path/qlever-index"
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = True

        result = UpgradeIndexCommand().execute(args)

        self.assertTrue(result)
        mock_run_command.assert_called_once_with(
            "/test/path/qlever-upgrade-index TestName"
            " 2>&1 | tee TestName.upgrade-index-log.txt",
            show_output=True,
        )

    # An explicitly given `--upgrade-index-binary` is used as is.
    def test_execute_explicit_binary(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        args.index_binary = "/test/path/qlever-index"
        args.upgrade_index_binary = "/other/path/upgrade-binary"
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = True

        result = UpgradeIndexCommand().execute(args)

        self.assertTrue(result)
        mock_run_command.assert_called_once_with(
            "/other/path/upgrade-binary TestName"
            " 2>&1 | tee TestName.upgrade-index-log.txt",
            show_output=True,
        )

    # With a container system, the command is wrapped by `Containerize`.
    def test_execute_containerized(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        args.system = "docker"
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = True
        containerized_cmd = "docker run --rm ..."
        containerize_instance = mock_containerize.return_value
        containerize_instance.containerize_command.return_value = (
            containerized_cmd
        )

        result = UpgradeIndexCommand().execute(args)

        self.assertTrue(result)
        containerize_instance.containerize_command.assert_called_once_with(
            "qlever-upgrade-index TestName"
            " 2>&1 | tee TestName.upgrade-index-log.txt",
            "docker",
            "run --rm",
            args.image,
            args.index_container,
            volumes=[("$(pwd)", "/index")],
            working_directory="/index",
        )
        mock_run_command.assert_called_once_with(
            containerized_cmd, show_output=True
        )

    # With `--show`, the command is only shown, not run.
    def test_execute_show(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        args.show = True
        mock_containerize.supported_systems.return_value = ["docker"]

        result = UpgradeIndexCommand().execute(args)

        self.assertTrue(result)
        mock_run_command.assert_not_called()

    # A missing binary fails the command before anything is run.
    def test_execute_binary_missing(
        self, mock_containerize, mock_binary_exists, mock_run_command
    ):
        args = make_args()
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = False

        result = UpgradeIndexCommand().execute(args)

        self.assertFalse(result)
        mock_run_command.assert_not_called()

    # A failing upgrade binary fails the command.
    @patch("qlever.commands.upgrade_index.log")
    def test_execute_upgrade_fails(
        self,
        mock_log,
        mock_containerize,
        mock_binary_exists,
        mock_run_command,
    ):
        args = make_args()
        mock_containerize.supported_systems.return_value = ["docker"]
        mock_binary_exists.return_value = True
        mock_run_command.side_effect = Exception("upgrade failed")

        result = UpgradeIndexCommand().execute(args)

        self.assertFalse(result)
        mock_log.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()
