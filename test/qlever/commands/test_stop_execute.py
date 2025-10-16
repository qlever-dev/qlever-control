from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from qlever.commands.stop import StopCommand


class TestStopCommand(unittest.TestCase):
    @patch("qlever.commands.stop.StatusCommand.execute")
    @patch("psutil.process_iter")
    @patch("qlever.containerize.Containerize.stop_and_remove_container")
    @patch("qlever.commands.stop.StopCommand.show")
    def test_execute_no_matching_processes_or_containers(
        self,
        mock_show,
        mock_stop_and_remove_container,
        mock_process_iter,
        mock_status_execute,
    ):
        # Setup args
        args = MagicMock()
        args.cmdline_regex = "ServerMain.* -i [^ ]*%%NAME%%"
        args.name = "TestName"
        args.no_containers = True
        args.server_container = "test_container"
        args.show = False

        # Replace the regex placeholder
        expected_regex = args.cmdline_regex.replace("%%NAME%%", args.name)

        # Mock process_iter to return no matching processes
        mock_process_iter.return_value = []

        # Instantiate the StopCommand
        sc = StopCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        mock_show.assert_called_once_with(
            f'Checking for processes matching "{expected_regex}"',
            only_show=False,
        )
        mock_process_iter.assert_called_once()
        mock_stop_and_remove_container.assert_not_called()
        mock_status_execute.assert_called_once_with(args)
        self.assertTrue(result)

    @patch("qlever.commands.stop.StatusCommand.execute")
    @patch("psutil.process_iter")
    @patch("qlever.containerize.Containerize.stop_and_remove_container")
    @patch("qlever.commands.stop.StopCommand.show")
    def test_execute_with_matching_process(
        self,
        mock_show,
        mock_stop_and_remove_container,
        mock_process_iter,
        mock_status_execute,
    ):
        # Setup args
        args = MagicMock()
        args.cmdline_regex = "ServerMain.* -i [^ ]*%%NAME%%"
        args.name = "TestName"
        args.no_containers = True
        args.server_container = "test_container"
        args.show = False

        # Replace the regex placeholder
        expected_regex = args.cmdline_regex.replace("%%NAME%%", args.name)

        # Creating mock psutil.Process objects with necessary attributes
        mock_process = MagicMock()
        # to test with real psutil.process objects use this:

        mock_process.as_dict.return_value = {
            "cmdline": ["ServerMain", "-i", "/some/path/TestName"],
            "pid": 1234,
            "username": "test_user",
        }

        mock_process_iter.return_value = [mock_process]

        # Mock process.kill to simulate successful process termination
        mock_process.kill.return_value = None

        # Instantiate the StopCommand
        sc = StopCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        mock_show.assert_called_once_with(
            f'Checking for processes matching "{expected_regex}"',
            only_show=False,
        )
        mock_process_iter.assert_called_once()
        mock_stop_and_remove_container.assert_not_called()
        mock_process.kill.assert_called_once()
        mock_status_execute.assert_not_called()
        self.assertTrue(result)

    @patch("qlever.commands.stop.StatusCommand.execute")
    @patch("psutil.process_iter")
    @patch("qlever.containerize.Containerize.stop_and_remove_container")
    @patch("qlever.commands.stop.StopCommand.show")
    def test_execute_with_containers(
        self,
        mock_show,
        mock_stop_and_remove_container,
        mock_process_iter,
        mock_status_execute,
    ):
        # Setup args
        args = MagicMock()
        args.cmdline_regex = "ServerMain.* -i [^ ]*%%NAME%%"
        args.name = "TestName"
        args.no_containers = False
        args.server_container = "test_container"
        args.show = False

        # Replace the regex placeholder
        expected_regex = args.cmdline_regex.replace("%%NAME%%", args.name)

        # Mocking container stop and removal
        mock_stop_and_remove_container.return_value = True

        # Instantiate the StopCommand
        sc = StopCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        mock_show.assert_called_once_with(
            f'Checking for processes matching "{expected_regex}" and for'
            f' Docker container with name "{args.server_container}"',
            only_show=False,
        )
        mock_process_iter.assert_not_called()
        mock_stop_and_remove_container.assert_called_once()
        mock_status_execute.assert_not_called()
        self.assertTrue(result)

    @patch("qlever.commands.stop.StatusCommand.execute")
    @patch("psutil.process_iter")
    @patch("qlever.containerize.Containerize.stop_and_remove_container")
    @patch("qlever.commands.stop.StopCommand.show")
    def test_execute_with_no_containers_and_no_matching_process(
        self,
        mock_show,
        mock_stop_and_remove_container,
        mock_process_iter,
        mock_status_execute,
    ):
        # Setup args
        args = MagicMock()
        args.cmdline_regex = "ServerMain.* -i [^ ]*%%NAME%%"
        args.name = "TestName"
        args.no_containers = False
        args.server_container = "test_container"
        args.show = False

        # Replace the regex placeholder
        expected_regex = args.cmdline_regex.replace("%%NAME%%", args.name)

        # Mock process_iter to return no matching processes
        mock_process_iter.return_value = []

        # Mock container stop and removal to return False (no container found)
        mock_stop_and_remove_container.return_value = False

        # Instantiate the StopCommand
        sc = StopCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        mock_show.assert_called_once_with(
            f'Checking for processes matching "{expected_regex}" and for'
            f' Docker container with name "{args.server_container}"',
            only_show=False,
        )
        mock_process_iter.assert_called_once()
        mock_stop_and_remove_container.assert_called()
        mock_status_execute.assert_called_once_with(args)
        self.assertTrue(result)

    @patch("qlever.commands.stop.StatusCommand.execute")
    @patch("psutil.process_iter")
    @patch("qlever.containerize.Containerize.stop_and_remove_container")
    @patch("qlever.commands.stop.StopCommand.show")
    @patch("qlever.util.show_process_info")
    def test_execute_with_error_killing_process(
        self,
        mock_show_process_info,
        mock_show,
        mock_stop_and_remove_container,
        mock_process_iter,
        mock_status_execute,
    ):
        # Setup args
        args = MagicMock()
        args.cmdline_regex = "ServerMain.* -i [^ ]*%%NAME%%"
        args.name = "TestName"
        args.no_containers = True
        args.server_container = "test_container"
        args.show = False

        # Replace the regex placeholder
        expected_regex = args.cmdline_regex.replace("%%NAME%%", args.name)

        # Creating mock psutil.Process objects with necessary attributes
        mock_process = MagicMock()
        mock_process.as_dict.return_value = {
            "cmdline": ["ServerMain", "-i", "/some/path/TestName"],
            "pid": 1234,
            "create_time": 1234567890,
            "memory_info": MagicMock(rss=1024 * 1024 * 512),
            "username": "test_user",
        }
        mock_process_iter.return_value = [mock_process]

        # Mock process.kill to raise an exception
        mock_process.kill.side_effect = Exception("Test")

        # Instantiate the StopCommand
        sc = StopCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        mock_show.assert_called_once_with(
            f'Checking for processes matching "{expected_regex}"',
            only_show=False,
        )
        mock_process_iter.assert_called_once()
        mock_stop_and_remove_container.assert_not_called()
        mock_process.kill.assert_called_once()
        mock_show_process_info.assert_called_once_with(
            mock_process, "", show_heading=True
        )
        mock_status_execute.assert_not_called()
        self.assertFalse(result)
