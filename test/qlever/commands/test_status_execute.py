import sys
import unittest
from io import StringIO
from unittest.mock import MagicMock, call, patch

import qlever.command
from qlever.commands.status import StatusCommand


def get_mock_args(only_show):
    args = MagicMock()
    args.cmdline_regex = "^(qlever-server|qlever-index)"
    args.show = only_show
    return [args, args.cmdline_regex, args.show]


class TestStatusCommand(unittest.TestCase):
    @patch("qlever.commands.status.show_process_info")
    @patch("psutil.process_iter")
    # testing execute for 2 processes. Just the second one is a qlever process.
    # Mocking the process_iter and show_process_info method and testing
    # if the methods are called correctly.
    def test_execute_processes_found(
        self, mock_process_iter, mock_show_process_info
    ):
        # Mocking the input for the execute function
        [args, args.cmdline_regex, args.show] = get_mock_args(False)

        # Creating mock psutil.Process objects with necessary attributes
        mock_process1 = MagicMock()
        mock_process1.as_dict.return_value = {"test": [1]}
        # to test with real psutil.process objects use this:
        """mock_process1.as_dict.return_value = {
            'cmdline': ['cmdline1'],
            'pid': 1,
            'username': 'user1',
            'create_time': datetime.now().timestamp(),
            'memory_info': MagicMock(rss=512 * 1024 * 1024)  # 512 MB
        }"""

        mock_process2 = MagicMock()
        mock_process2.as_dict.return_value = {"test": [2]}
        # to test with real psutil.process objects use this:
        """mock_process2.as_dict.return_value = {
            'cmdline': ['cmdline2'],
            'pid': 2,
            'username': 'user2',
            'create_time': datetime.now().timestamp(),
            'memory_info': MagicMock(rss=1024 * 1024 * 1024)  # 1 GB
        }"""

        mock_process3 = MagicMock()
        mock_process3.as_dict.return_value = {"test": [3]}

        # Mock the return value of process_iter
        # to be a list of these mocked process objects
        mock_process_iter.return_value = [
            mock_process1,
            mock_process2,
            mock_process3,
        ]

        # Simulate show_process_info returning False for the first
        # True for the second and False for the third process
        mock_show_process_info.side_effect = [False, True, False]

        sc = StatusCommand()

        # Execute the function
        result = sc.execute(args)

        # Assert that process_iter was called once
        mock_process_iter.assert_called_once()

        # Assert that show_process_info was called 3times
        # in correct order with the correct arguments
        expected_calls = [
            call(mock_process1, args.cmdline_regex, show_heading=True),
            call(mock_process2, args.cmdline_regex, show_heading=True),
            call(mock_process3, args.cmdline_regex, show_heading=False),
        ]
        mock_show_process_info.assert_has_calls(
            expected_calls, any_order=False
        )
        self.assertTrue(result)

    @patch("qlever.util.show_process_info")
    @patch("psutil.process_iter")
    def test_execute_no_processes_found(
        self, mock_process_iter, mock_show_process_info
    ):
        # Mocking the input for the execute function
        [args, args.cmdline_regex, args.show] = get_mock_args(False)

        # Mock process_iter to return an empty list,
        # simulating that no matching processes are found
        mock_process_iter.return_value = []

        # Capture the string-output
        captured_output = StringIO()
        sys.stdout = captured_output

        # Instantiate the StatusCommand
        status_command = StatusCommand()

        # Execute the function
        result = status_command.execute(args)

        # Reset redirect
        sys.stdout = sys.__stdout__

        # Assert that process_iter was called once
        mock_process_iter.assert_called_once()

        # Assert that show_process_info was never called
        # since there are no processes
        mock_show_process_info.assert_not_called()

        self.assertTrue(result)

        # Verify the correct output was printed
        self.assertIn("No processes found", captured_output.getvalue())

    @patch.object(qlever.command.QleverCommand, "show")
    def test_execute_show_action_description(self, mock_show):
        # Mocking the input for the execute function
        [args, args.cmdline_regex, args.show] = get_mock_args(True)

        # Execute the function
        result = StatusCommand().execute(args)

        # Assert that verifies that show was called with the correct parameters
        mock_show.assert_any_call(
            f"Show all processes on this machine where "
            f"the command line matches {args.cmdline_regex}"
            f" using Python's psutil library",
            only_show=args.show,
        )

        self.assertTrue(result)
