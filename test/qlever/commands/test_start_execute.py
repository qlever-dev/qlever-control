import unittest
from unittest.mock import MagicMock, call, patch

import qlever.commands.start
import qlever.util
from qlever.commands.start import StartCommand


# Tests if the construction of the command line works if every "if" is taken
def test_construct_command_with_if():
    # Setup args
    args = MagicMock()
    args.server_binary = "/test/path/server_binary"
    args.name = "TestName"
    args.num_threads = 2
    args.port = 1234
    args.memory_for_queries = "8G"
    args.cache_max_size = "2G"
    args.cache_max_size_single_entry = "124M"
    args.cache_max_num_entries = 1000
    args.timeout = True
    args.persist_updates = False
    args.access_token = True
    args.only_pso_and_pos_permutations = True
    args.use_patterns = False
    args.use_text_index = "yes"

    # Execute the function
    result = qlever.commands.start.construct_command(args)

    start_command = (
        f"{args.server_binary}"
        f" -i {args.name}"
        f" -j {args.num_threads}"
        f" -p {args.port}"
        f" -m {args.memory_for_queries}"
        f" -c {args.cache_max_size}"
        f" -e {args.cache_max_size_single_entry}"
        f" -k {args.cache_max_num_entries}"
        f" -s {args.timeout}"
        f" -a {args.access_token}"
        " --only-pso-and-pos-permutations"
        " --no-patterns"
        " -t"
        f" > {args.name}.server-log.txt 2>&1"
    )
    assert result == start_command


# Tests if the construction of the command line works if no "if" is taken
def test_construct_command_without_if():
    # Setup args
    args = MagicMock()
    args.server_binary = "/test/path/server_binary"
    args.name = "TestName"
    args.num_threads = 2
    args.port = 1234
    args.memory_for_queries = "8G"
    args.cache_max_size = "2G"
    args.cache_max_size_single_entry = "124M"
    args.cache_max_num_entries = 1000
    args.timeout = False
    args.persist_updates = False
    args.access_token = False
    args.only_pso_and_pos_permutations = False
    args.use_patterns = True
    args.use_text_index = "no"

    # Execute the function
    result = qlever.commands.start.construct_command(args)

    start_command = (
        f"{args.server_binary}"
        f" -i {args.name}"
        f" -j {args.num_threads}"
        f" -p {args.port}"
        f" -m {args.memory_for_queries}"
        f" -c {args.cache_max_size}"
        f" -e {args.cache_max_size_single_entry}"
        f" -k {args.cache_max_num_entries}"
        f" > {args.name}.server-log.txt 2>&1"
    )
    assert result == start_command


# Tests `wrap_command_in_container`.
@patch("qlever.commands.start.Containerize.containerize_command")
def test_wrap_command_in_container(mock_containerize_command):
    # Setup args
    args = MagicMock()
    args.name = "TestName"
    args.server_container = f"qlever.server.{args.name}"
    args.port = 1234
    args.system = "native"
    args.image = None

    # Mock wrap_command_in_container
    mock_containerize_command.return_value = "Test_Container_Command"

    # start_cmd before construct_command(args)
    start_cmd = "Test_start_cmd"
    # Execute the function
    result = qlever.commands.start.wrap_command_in_container(args, start_cmd)

    # check wrap_command_in_container was called once with correct parameters
    mock_containerize_command.assert_called_once_with(
        start_cmd,
        args.system,
        "run -d --restart=unless-stopped",
        args.image,
        args.server_container,
        volumes=[("$(pwd)", "/index")],
        ports=[(args.port, args.port)],
        working_directory="/index",
    )
    # check start command was successfully returned
    start_command = "Test_Container_Command"
    assert result == start_command


# Tests the check_binary help function for the case of success of the
# run_cmd in the try/except block
@patch("qlever.util.run_command")
def test_check_binary_success(mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.server_binary = "/test/path/server_binary"
    # mock run_cmd as successful
    mock_run_cmd.return_value = "Command works"

    # Execute the function
    result = qlever.util.binary_exists(args.server_binary, "server-binary")
    # check if run_cmd was called once with
    mock_run_cmd.assert_called_once_with(f"{args.server_binary} --help")
    assert result


# Tests the check_binary help function for the case of exception for the
# run_cmd in the try/except block
@patch("qlever.util.run_command")
@patch("qlever.util.log")
def test_check_binary_exception(mock_log, mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.server_binary = "false_binary"

    # Simulate an exception when run_command is called
    mock_run_cmd.side_effect = Exception("Mocked command failure")

    # Execute the function
    result = qlever.util.binary_exists(args.server_binary, "server-binary")

    # check if run_cmd was called once with
    mock_run_cmd.assert_called_once_with(f"{args.server_binary} --help")
    # Verify that the error message was logged
    mock_log.error.assert_called_once_with(
        'Running "false_binary" failed, set `--server-binary` to a different'
        " binary or set `--system to a container system`"
    )
    # Check that the info log contains the exception message
    mock_log.info.assert_any_call(
        "The error message was: Mocked command failure"
    )
    assert not result


# Tests the set_index_description help function for the case of success
# of the run_cmd in the try/except block
@patch("qlever.commands.start.run_command")
@patch("qlever.commands.start.log")
def test_set_index_description_success(mock_log, mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.access_token = True
    args.port = 1234
    args.description = "TestDescription"
    access_arg = f'--data-urlencode "access-token={args.access_token}"'

    # Execute the function
    qlever.commands.start.set_index_description(
        access_arg, args.port, args.description
    )
    # Asserts
    curl_cmd = (
        f"curl -Gs http://localhost:{args.port}/api"
        f' --data-urlencode "index-description={args.description}"'
        f" {access_arg} > /dev/null"
    )
    # Verify that the debug message was logged
    mock_log.debug.assert_called_once_with(curl_cmd)
    # check if run_cmd was called once with correct parameters
    mock_run_cmd.assert_called_once_with(curl_cmd)


# Tests the set_index_description help function for the case of exception
# for the run_cmd in the try/except block
@patch("qlever.commands.start.run_command")
@patch("qlever.commands.start.log")
def test_set_index_description_exception(mock_log, mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.access_token = True
    args.port = 1234
    args.description = "ErrorDescription"
    access_arg = f'--data-urlencode "access-token={args.access_token}"'

    # Simulate an exception when run_command is called
    mock_run_cmd.side_effect = Exception("Mocked command failure")

    # Execute the function
    qlever.commands.start.set_index_description(
        access_arg, args.port, args.description
    )

    # Asserts
    curl_cmd = (
        f"curl -Gs http://localhost:{args.port}/api"
        f' --data-urlencode "index-description={args.description}"'
        f" {access_arg} > /dev/null"
    )
    # Verify that the debug message was logged
    mock_log.debug.assert_called_once_with(curl_cmd)
    # check if run_cmd was called once with correct parameters
    mock_run_cmd.assert_called_once_with(curl_cmd)
    # Verify that the error message was logged
    mock_log.error.assert_called_once_with(
        "Setting the index description failed (Mocked command failure)"
    )


# Tests the set_text_description help function for the case of success
# of the run_cmd in the try/except block
@patch("qlever.commands.start.run_command")
@patch("qlever.commands.start.log")
def test_set_text_description_success(mock_log, mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.access_token = True
    args.port = 1234
    args.description = "TestDescription"
    access_arg = f'--data-urlencode "access-token={args.access_token}"'

    # Execute the function
    qlever.commands.start.set_text_description(
        access_arg, args.port, args.description
    )
    # Asserts
    curl_cmd = (
        f"curl -Gs http://localhost:{args.port}/api"
        f' --data-urlencode "text-description={args.description}"'
        f" {access_arg} > /dev/null"
    )
    # Verify that the debug message was logged
    mock_log.debug.assert_called_once_with(curl_cmd)
    # check if run_cmd was called once with correct parameters
    mock_run_cmd.assert_called_once_with(curl_cmd)


# Tests the set_text_description help function for the case of exception
# for the run_cmd in the try/except block
@patch("qlever.commands.start.run_command")
@patch("qlever.commands.start.log")
def test_set_text_description_exception(mock_log, mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.access_token = True
    args.port = 1234
    args.description = "ErrorDescription"
    access_arg = f'--data-urlencode "access-token={args.access_token}"'

    # Simulate an exception when run_command is called
    mock_run_cmd.side_effect = Exception("Mocked command failure")

    # Execute the function
    qlever.commands.start.set_text_description(
        access_arg, args.port, args.description
    )

    # Asserts
    curl_cmd = (
        f"curl -Gs http://localhost:{args.port}/api"
        f' --data-urlencode "text-description={args.description}"'
        f" {access_arg} > /dev/null"
    )
    # Verify that the debug message was logged
    mock_log.debug.assert_called_once_with(curl_cmd)
    # check if run_cmd was called once with correct parameters
    mock_run_cmd.assert_called_once_with(curl_cmd)
    # Verify that the error message was logged
    mock_log.error.assert_called_once_with(
        "Setting the text description failed (Mocked command failure)"
    )


class TestStartCommand(unittest.TestCase):
    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.commands.stop.StopCommand.execute", return_value=True)
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize")
    # Tests if killing existing server and restarting a new one works.
    # Also checks the start_command for all the extra options enabled.
    def test_execute_kills_existing_server_on_same_port(
        self,
        mock_containerize,
        mock_popen,
        mock_is_qlever_server_alive,
        mock_start_run_command,
        mock_util_run_command,
        mock_stop,
        mock_cache_stats_command,
    ):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = True
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.num_threads = 2
        args.memory_for_queries = "8G"
        args.cache_max_size = "2G"
        args.cache_max_size_single_entry = "124M"
        args.cache_max_num_entries = 1000
        args.system = "native"
        args.show = False
        args.no_warmup = True
        args.run_in_foreground = False
        args.timeout = True
        args.persist_updates = False
        args.access_token = True
        args.only_pso_and_pos_permutations = True
        args.use_patterns = False
        args.use_text_index = "yes"

        # Mock CacheStatsCommand
        mock_cache_stats_command.return_value = None

        # Mock Containerize
        mock_containerize.return_value = None

        # Mock server is not alive initially, then alive after starting
        mock_is_qlever_server_alive.side_effect = [False, True]

        # Mock Popen
        mock_popen.return_value = MagicMock()

        # Instantiate the StartCommand
        sc = StartCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        # Ensure the StopCommand was called
        mock_stop.assert_called_once()
        # Server status should be checked
        mock_is_qlever_server_alive.assert_called()

        # Ensure the server was started
        run_call_1 = f"{args.server_binary} --help"
        start_command = (
            f"{args.server_binary}"
            f" -i {args.name}"
            f" -j {args.num_threads}"
            f" -p {args.port}"
            f" -m {args.memory_for_queries}"
            f" -c {args.cache_max_size}"
            f" -e {args.cache_max_size_single_entry}"
            f" -k {args.cache_max_num_entries}"
            f" -s {args.timeout}"
            f" -a {args.access_token}"
            " --only-pso-and-pos-permutations"
            " --no-patterns"
            " -t"
            f" > {args.name}.server-log.txt 2>&1"
        )
        run_call_2 = f"nohup {start_command} &"
        # Assert that run_command was called exactly twice with the

        mock_util_run_command.assert_has_calls([call(run_call_1)])
        mock_start_run_command.assert_has_calls(
            [call(run_call_2, use_popen=False)]
        )
        # Ensure execution was successful
        self.assertTrue(result)

    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("qlever.commands.start.Containerize")
    def test_execute_fails_due_to_existing_server(
        self, mock_containerize, mock_is_qlever_server_alive, mock_run_command
    ):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = False
        args.port = "localhorst"
        args.port = 1234
        args.cmdline_regex = f"^ServerMain.* -p {args.port}"
        args.no_containers = True
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.num_threads = 2
        args.memory_for_queries = "8G"
        args.cache_max_size = "2G"
        args.cache_max_size_single_entry = "124M"
        args.cache_max_num_entries = 1000
        args.system = "native"
        args.show = False

        # Mock the QLever server as already running
        mock_is_qlever_server_alive.return_value = True

        # Mock Containerize
        mock_containerize.return_value = None

        # Instantiate the StartCommand
        sc = StartCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        # Ensure the server status was checked
        endpoint_url = f"http://{args.host_name}:{args.port}"
        mock_is_qlever_server_alive.assert_called_once_with(endpoint_url)
        # Check that `run_command` was called only for the `--help` check,
        # but not the actual start command
        mock_run_command.assert_called_once_with(
            f"{args.server_binary} --help"
        )
        # The function should return False if the server is already running
        self.assertFalse(result)

    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize")
    @patch("time.sleep")
    def test_execute_successful_server_start(
        self,
        mock_sleep,
        mock_containerize,
        mock_popen,
        mock_is_qlever_server_alive,
        mock_start_run_command,
        mock_util_run_command,
        mock_cache_stats_command,
    ):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = False
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.num_threads = 2
        args.memory_for_queries = "8G"
        args.cache_max_size = "2G"
        args.cache_max_size_single_entry = "124M"
        args.cache_max_num_entries = 1000
        args.system = "native"
        args.show = False
        args.no_warmup = True

        # Mock server is not alive initially, then alive after starting
        mock_is_qlever_server_alive.side_effect = [False, True]

        # Mock Popen
        mock_popen.return_value = MagicMock()

        # Mock CacheStatsCommand
        mock_cache_stats_command.return_value = None

        # Mock Containerize
        mock_containerize.return_value = None

        # Mock sleep
        mock_sleep.return_value = None

        # Instantiate the StartCommand
        sc = StartCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        # Server status should be checked
        mock_is_qlever_server_alive.assert_called()
        # Ensure the server was started
        self.assertTrue(mock_util_run_command.called)
        self.assertTrue(mock_start_run_command.called)
        # Ensure execution was successful
        self.assertTrue(result)

    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("subprocess.run")
    @patch("qlever.commands.start.Containerize")
    def test_execute_server_with_warmup(
        self,
        mock_containerize,
        mock_run,
        mock_popen,
        mock_is_qlever_server_alive,
        mock_start_run_command,
        mock_util_run_command,
        mock_cache_stats_command,
    ):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = False
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.num_threads = 2
        args.memory_for_queries = "8G"
        args.cache_max_size = "2G"
        args.cache_max_size_single_entry = "124M"
        args.cache_max_num_entries = 1000
        args.system = "native"
        args.show = False
        args.warmup_cmd = "test_warmup_command"
        args.no_warmup = False

        # Mock Popen
        mock_popen.return_value = MagicMock()

        # Mock CacheStatsCommand
        mock_cache_stats_command.return_value = None

        # Mock Containerize
        mock_containerize.return_value = None

        # Mock that no server is currently running
        mock_is_qlever_server_alive.side_effect = [False, True]

        # Instantiate the StartCommand
        sc = StartCommand()

        # Execute the function
        result = sc.execute(args)

        # Check that Popen was called
        mock_popen.assert_called_once_with(
            f"exec tail -f {args.name}.server-log.txt", shell=True
        )

        # Check warmup was called
        mock_run.assert_called_once_with(
            args.warmup_cmd, shell=True, check=True
        )

        # Assertions
        # Ensure the server status was checked
        mock_is_qlever_server_alive.assert_called()
        # Ensure the server was started
        mock_util_run_command.assert_called()
        mock_start_run_command.assert_called()
        # Execution should succeed
        self.assertTrue(result)

    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.commands.stop.StopCommand.execute", return_value=True)
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize.supported_systems")
    @patch("qlever.commands.start.wrap_command_in_container")
    @patch("qlever.commands.start.construct_command")
    def test_execute_containerize_and_description(
        self,
        mock_construct_cl,
        mock_run_containerize,
        mock_containerize,
        mock_popen,
        mock_is_qlever_server_alive,
        mock_run_command,
        mock_stop,
        mock_cache_stats_command,
    ):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = True
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.num_threads = 2
        args.memory_for_queries = "8G"
        args.cache_max_size = "2G"
        args.cache_max_size_single_entry = "124M"
        args.cache_max_num_entries = 1000
        args.system = "test1"
        args.show = False
        args.description = "TestDescription"
        args.text_description = "TestTextDescription"
        args.access_token = "TestToken"
        args.run_in_foreground = False

        # Mock server is not alive initially, then alive after starting
        mock_is_qlever_server_alive.side_effect = [False, True]

        # Mock Popen
        mock_popen.return_value = MagicMock()

        # Mock construct_command
        mock_construct_cl.return_value = "TestStart"

        # Mock construct_command
        mock_run_containerize.return_value = "TestStart2"

        # mock StopCommand
        mock_stop.return_value = True

        # Mock CacheStatsCommand
        mock_cache_stats_command.return_value = None

        # Mock Containerize
        mock_containerize.return_value = ["test1", "test2"]

        # Instantiate the StartCommand
        sc = StartCommand()

        # Execute the function
        result = sc.execute(args)

        # Assertions
        # check if wrap_command_in_container is called once
        mock_run_containerize.assert_called_once_with(args, "TestStart")

        # Calls for run command
        run_call_1 = f"{args.system} rm -f {args.server_container}"
        run_call_2 = "TestStart2"
        access_arg = f'--data-urlencode "access-token={args.access_token}"'
        run_call_3 = (
            f"curl -Gs http://localhost:{args.port}/api"
            f' --data-urlencode "index-description={args.description}"'
            f" {access_arg} > /dev/null"
        )
        run_call_4 = (
            f"curl -Gs http://localhost:{args.port}/api"
            f' --data-urlencode "text-description='
            f'{args.text_description}"'
            f" {access_arg} > /dev/null"
        )
        # Assert that run_command was called exactly 4 times with the
        # correct arguments in order
        mock_run_command.assert_has_calls(
            [
                call(run_call_1),
                call(run_call_2, use_popen=False),
                call(run_call_3),
                call(run_call_4),
            ],
            any_order=False,
        )
        # Server status should be checked
        mock_is_qlever_server_alive.assert_called()
        # Ensure execution was successful
        self.assertTrue(result)

    # check if execute returns False for args.show = True
    @patch("qlever.commands.start.construct_command")
    def test_execute_show(self, mock_construct_cmd_line):
        # Setup args
        args = MagicMock()
        args.kill_existing_with_same_port = False
        args.system = None
        args.show = True
        # Mock construct_command
        mock_construct_cmd_line.return_value = "Test_start_cmd"

        # Execute the function and check if return is False
        self.assertTrue(StartCommand().execute(args))
