from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from qlever.commands.cache_stats import CacheStatsCommand


class TestCacheStatsCommand(unittest.TestCase):
    def setUp(self):
        self.command = CacheStatsCommand()

    @patch("qlever.commands.cache_stats.subprocess.check_output")
    @patch("qlever.commands.cache_stats.json.loads")
    @patch("qlever.commands.cache_stats.log")
    # Test execute of cache stats command for basic case with successful
    # execution
    def test_execute_successful_basic_cache_stats(
        self, mock_log, mock_json_loads, mock_check_output
    ):
        # Mock arguments for basic cache stats
        args = MagicMock()
        args.server_url = None
        args.host_name = "localhorst"
        args.port = 1234
        args.show = False
        args.detailed = False

        # Mock `subprocess.check_output` and `json.loads` as encoded bytes
        mock_check_output.side_effect = [
            # Mock cache_stats
            b'{"cache-size-pinned": 1e9, "cache-size-unpinned": 3e9}',
            # Mock cache_settings
            b'{"cache-max-size": "10 GB"}',
        ]
        # mock cache_stats_dict and cache_settings_dict as a dictionary
        mock_json_loads.side_effect = [
            {"cache-size-pinned": 1e9, "cache-size-unpinned": 3e9},
            {"cache-max-size": "10 GB"},
        ]

        # Execute the command
        result = self.command.execute(args)

        # Assertions
        expected_stats_call = (
            f"curl -s {args.host_name}:{args.port} "
            f'--data-urlencode "cmd=cache-stats"'
        )
        expected_settings_call = (
            f"curl -s {args.host_name}:{args.port} "
            f'--data-urlencode "cmd=get-settings"'
        )

        mock_check_output.assert_any_call(expected_stats_call, shell=True)
        mock_check_output.assert_any_call(expected_settings_call, shell=True)

        # Verify the correct information logs
        mock_log.info.assert_any_call(
            "Pinned queries     :   1.0 GB of  10.0 GB  [10.0%]"
        )
        mock_log.info.assert_any_call(
            "Non-pinned queries :   3.0 GB of  10.0 GB  [30.0%]"
        )
        mock_log.info.assert_any_call(
            "FREE               :   6.0 GB of  10.0 GB  [60.0%]"
        )

        self.assertTrue(result)

    @patch("qlever.commands.cache_stats.subprocess.check_output")
    @patch("qlever.commands.cache_stats.json.loads")
    @patch("qlever.commands.cache_stats.log")
    # Test for show_dict_as_table function. Reached if 'args.detailed = True'.
    def test_execute_detailed_cache_stats(
        self, mock_log, mock_json_loads, mock_check_output
    ):
        # Mock arguments for detailed cache stats
        args = MagicMock()
        args.server_url = "http://testlocalhost:1234"
        args.show = False
        args.detailed = True

        # Mock the responses from `subprocess.check_output` and `json.loads`
        mock_check_output.side_effect = [
            b'{"cache-size-pinned": 2e9, "cache-size-unpinned": 1e9, "test-stat": 500}',
            b'{"cache-max-size": "10 GB", "test-setting": 1000}',
        ]
        # CAREFUL: if value is float you will get an error in re.match
        mock_json_loads.side_effect = [
            {
                "cache-size-pinned": int(2e9),
                "cache-size-unpinned": int(1e9),
                "test-stat": 500,
            },
            {"cache-max-size": "10 GB", "test-setting": 1000},
        ]

        # Execute the command
        result = self.command.execute(args)

        # Assertions
        expected_stats_call = (
            f"curl -s {args.server_url} " f'--data-urlencode "cmd=cache-stats"'
        )
        expected_settings_call = (
            f"curl -s {args.server_url} "
            f'--data-urlencode "cmd=get-settings"'
        )

        mock_check_output.assert_any_call(expected_stats_call, shell=True)
        mock_check_output.assert_any_call(expected_settings_call, shell=True)

        # Verify that detailed stats and settings were logged as a table
        mock_log.info.assert_any_call("cache-max-size : 10 GB")
        mock_log.info.assert_any_call("cache-size-pinned   : 2,000,000,000")
        mock_log.info.assert_any_call("cache-size-unpinned : 1,000,000,000")
        mock_log.info.assert_any_call("test-stat           : 500")
        mock_log.info.assert_any_call("test-setting   : 1,000")

        self.assertTrue(result)

    @patch("qlever.commands.cache_stats.subprocess.check_output")
    @patch("qlever.commands.cache_stats.log")
    # Checking if correct error message is given for unsuccessful try/except
    # block.
    def test_execute_failed_cache_stats(self, mock_log, mock_check_output):
        # Mock arguments for basic cache stats
        args = MagicMock()
        args.server_url = "http://testlocalhost:1234"
        args.show = False
        args.detailed = False

        # Simulate a command execution failure
        mock_check_output.side_effect = Exception("Mocked command failure")

        # Execute the command
        result = self.command.execute(args)

        # Assertions to verify that error was logged
        mock_log.error.assert_called_once_with(
            "Failed to get cache stats and settings: Mocked command failure"
        )

        self.assertFalse(result)

    @patch("qlever.commands.cache_stats.subprocess.check_output")
    @patch("qlever.commands.cache_stats.json.loads")
    @patch("qlever.commands.cache_stats.log")
    # Checking if correct error message is given for invalid cache_size
    def test_execute_invalid_cache_size_format(
        self, mock_log, mock_json_loads, mock_check_output
    ):
        # Mock arguments for basic cache stats
        args = MagicMock()
        args.server_url = None
        args.port = 1234
        args.show = False
        args.detailed = False

        # Mock the responses with invalid cache size format
        mock_check_output.side_effect = [
            b'{"pinned-size": 2e9, "non-pinned-size": 1e9}',
            # Mock cache stats with invalid cache settings
            b'{"cache-max-size": "1000 MB"}',
        ]
        mock_json_loads.side_effect = [
            {"pinned-size": 2e9, "non-pinned-size": 1e9},
            {"cache-max-size": "1000 MB"},
        ]

        # Execute the command
        result = self.command.execute(args)

        # Assertions to verify that error was logged
        mock_log.error.assert_called_once_with(
            "Cache size 1000 MB is not in GB, QLever should return "
            "bytes instead"
        )

        self.assertFalse(result)

    @patch("qlever.commands.cache_stats.subprocess.check_output")
    @patch("qlever.commands.cache_stats.json.loads")
    @patch("qlever.commands.cache_stats.log")
    # Checking if correct log message is given for empty cache_size
    def test_execute_empty_cache_size(
        self, mock_log, mock_json_loads, mock_check_output
    ):
        # Mock arguments for basic cache stats
        args = MagicMock()
        args.server_url = None
        args.port = 1234
        args.show = False
        args.detailed = False

        # Mock the responses with empty cache size
        mock_check_output.side_effect = [
            b'{"cache-size-pinned": 0, "cache-size-unpinned": 0}',
            b'{"cache-max-size": "10 GB"}',
        ]
        mock_json_loads.side_effect = [
            {"cache-size-pinned": 0, "cache-size-unpinned": 0},
            {"cache-max-size": "10 GB"},
        ]

        # Execute the command
        result = self.command.execute(args)

        # Assertions to verify that log.info was called correctly
        mock_log.info.assert_called_once_with(
            "Cache is empty, all 10.0 GB available"
        )

        self.assertTrue(result)
