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
    args.rebuild_index_strategy = "automatic:10000:1000000:0.1"
    args.rebuild_keep_previous_index_dirs = "most-recent-only"
    args.set_runtime_parameters = [
        "default-query-timeout=300s",
        "rebuild-max-concurrent-permutation-pairs=1",
    ]
    args.access_token = True
    args.description = "Test description"
    args.text_description = "Text description"
    args.only_pso_and_pos_permutations = True
    args.use_patterns = "no"
    args.use_text_index = "yes"
    args.enable_metrics = False
    args.resource_usage_log = "no"
    args.preload_materialized_views = ["view-1", "view-2"]

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
        " --index-description 'Test description'"
        " --text-description 'Text description'"
        " --rebuild-index-strategy automatic:10000:1000000:0.1"
        " --rebuild-keep-previous-index-dirs most-recent-only"
        " --set-runtime-parameter default-query-timeout=300s"
        " --set-runtime-parameter rebuild-max-concurrent-permutation-pairs=1"
        " --only-pso-and-pos-permutations"
        " --no-patterns"
        " -t"
        " --no-resource-usage-log"
        " --preload-materialized-views view-1 view-2"
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
    args.rebuild_index_strategy = "manual"
    args.rebuild_keep_previous_index_dirs = "original-and-most-recent"
    args.set_runtime_parameters = None
    args.access_token = False
    args.description = None
    args.text_description = None
    args.only_pso_and_pos_permutations = False
    args.use_patterns = True
    args.use_text_index = "no"
    args.enable_metrics = False
    args.resource_usage_log = "yes"
    args.resource_usage_interval = 2
    args.preload_materialized_views = None

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


# Tests that a non-default sampling interval is passed to the binary
def test_construct_command_non_default_resource_usage_interval():
    args = MagicMock()
    args.description = None
    args.text_description = None
    args.resource_usage_log = "yes"
    args.resource_usage_interval = 5

    result = qlever.commands.start.construct_command(args)

    assert " --resource-usage-interval-s 5" in result
    assert "--no-resource-usage-log" not in result


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
    args.run_in_foreground = False
    args.restart_policy = "unless-stopped"

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
        "run --restart=unless-stopped -d",
        args.image,
        args.server_container,
        volumes=[("$(pwd)", "/index")],
        ports=[(args.port, args.port)],
        working_directory="/index",
        seccomp_profile=args.seccomp_profile,
    )
    # check start command was successfully returned
    start_command = "Test_Container_Command"
    assert result == start_command


# Tests `wrap_command_in_systemd_unit`: the restart policy is mapped to the
# `Restart=` property of the unit and the log is appended by the unit.
def test_wrap_command_in_systemd_unit():
    args = MagicMock()
    args.name = "TestName"
    args.restart_policy = "unless-stopped"
    args.server_log_mode = "rotate"

    result = qlever.commands.start.wrap_command_in_systemd_unit(
        args, "Test_start_cmd"
    )
    assert (
        "systemd-run --user --unit qlever.server.TestName"
        ' --working-directory "$(pwd)"' in result
    )
    assert " -p Restart=always -p RestartSec=5" in result
    assert (
        ' -p StandardOutput=append:"$(pwd)"/TestName.server-log.txt' in result
    )
    assert result.endswith(" -p StandardError=inherit Test_start_cmd")

    args.restart_policy = "on-failure"
    args.server_log_mode = "no-log"
    result = qlever.commands.start.wrap_command_in_systemd_unit(
        args, "Test_start_cmd"
    )
    assert " -p Restart=on-failure " in result
    assert " -p StandardOutput=null " in result


# For a server run as a systemd unit, the command line has no shell redirect
# (the unit writes the log), and the liveness check asks systemd.
@patch("qlever.commands.start.systemd_unit_is_active")
def test_construct_command_and_liveness_check_systemd(mock_is_active):
    args = MagicMock()
    args.name = "TestName"
    args.system = "native"
    args.description = None
    args.text_description = None
    args.timeout = False
    args.access_token = False
    args.persist_updates = False
    args.rebuild_index_strategy = "manual"
    args.rebuild_keep_previous_index_dirs = "original-and-most-recent"
    args.set_runtime_parameters = None
    args.only_pso_and_pos_permutations = False
    args.use_patterns = "yes"
    args.use_text_index = "no"
    args.enable_metrics = False
    args.metrics_log = "yes"
    args.resource_usage_log = "yes"
    args.resource_usage_interval = 2
    args.preload_materialized_views = None

    result = qlever.commands.start.construct_command(args, use_systemd=True)
    assert "server-log.txt" not in result
    assert not result.endswith("2>&1")

    mock_is_active.return_value = True
    is_still_running = qlever.commands.start.make_server_liveness_check(
        args, None, None, use_systemd=True
    )
    assert is_still_running()
    mock_is_active.assert_called_once_with("qlever.server.TestName")


# Tests `check_systemd_for_restarts`: systemd on Linux with lingering is
# "ok", without lingering "no-linger", everything else "no-systemd".
@patch("qlever.commands.start.systemd_linger_status")
@patch("qlever.commands.start.shutil.which")
@patch("qlever.commands.start.platform.system")
def test_check_systemd_for_restarts(mock_system, mock_which, mock_linger):
    check = qlever.commands.start.check_systemd_for_restarts
    mock_system.return_value = "Linux"
    mock_which.return_value = "/usr/bin/systemd-run"
    mock_linger.return_value = "yes"
    assert check() == "ok"
    mock_linger.return_value = "no"
    assert check() == "no-linger"
    mock_linger.return_value = None
    assert check() == "no-systemd"
    mock_linger.return_value = "yes"
    mock_which.return_value = None
    assert check() == "no-systemd"
    mock_which.return_value = "/usr/bin/systemd-run"
    mock_system.return_value = "Darwin"
    assert check() == "no-systemd"


# Tests the check_binary help function for the case of success of the
# run_cmd in the try/except block
@patch("qlever.util.run_command")
def test_check_binary_success(mock_run_cmd):
    # Setup args
    args = MagicMock()
    args.server_binary = "/test/path/server_binary"
    args.system = "native"
    # mock run_cmd as successful
    mock_run_cmd.return_value = "Command works"

    # Execute the function
    result = qlever.util.binary_exists(
        args.server_binary, "server-binary", args
    )
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
    args.system = "native"

    # Simulate an exception when run_command is called
    mock_run_cmd.side_effect = Exception("Mocked command failure")

    # Execute the function
    result = qlever.util.binary_exists(
        args.server_binary, "server-binary", args
    )

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


# Tests `server_supports_description_options`: decided by the `--help` output
# of the server binary; a binary that cannot be run counts as supporting them.
@patch("qlever.commands.start.run_command")
def test_server_supports_description_options(mock_run_cmd):
    args = MagicMock()
    args.server_binary = "/test/path/server_binary"
    args.system = "native"
    help_cmd = f"{args.server_binary} --help"

    mock_run_cmd.return_value = "... --index-description arg ..."
    assert qlever.commands.start.server_supports_description_options(args)
    mock_run_cmd.assert_called_once_with(help_cmd, return_output=True)

    mock_run_cmd.return_value = "... --text arg ..."
    assert not qlever.commands.start.server_supports_description_options(args)

    mock_run_cmd.side_effect = Exception("Mocked command failure")
    assert qlever.commands.start.server_supports_description_options(args)


class TestStartCommand(unittest.TestCase):
    @staticmethod
    def _mock_log_file(mock_path_cls, name):
        mock_log_file = mock_path_cls.return_value
        mock_log_file.exists.return_value = True
        mock_log_file.__str__ = MagicMock(
            return_value=f"{name}.server-log.txt"
        )

    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.commands.stop.StopCommand.execute", return_value=True)
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize")
    @patch("qlever.commands.start.Path")
    # Tests if killing existing server and restarting a new one works.
    # Also checks the start_command for all the extra options enabled.
    def test_execute_kills_existing_server_on_same_port(
        self,
        mock_path_cls,
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
        args.restart_policy = "no"
        args.description = None
        args.text_description = None
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
        args.rebuild_index_strategy = "manual"
        args.rebuild_keep_previous_index_dirs = "original-and-most-recent"
        args.access_token = True
        args.only_pso_and_pos_permutations = True
        args.use_patterns = "no"
        args.use_text_index = "yes"
        args.enable_metrics = False
        args.resource_usage_log = "yes"
        args.resource_usage_interval = 2
        args.preload_materialized_views = None

        # Configure Path mock so the log file wait loop is skipped
        self._mock_log_file(mock_path_cls, args.name)

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
        run_call_2 = f"nohup {start_command} & echo $!"
        # Assert that run_command was called exactly twice with the

        mock_util_run_command.assert_has_calls([call(run_call_1)])
        mock_start_run_command.assert_has_calls(
            [call(run_call_2, return_output=True)]
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
        args.restart_policy = "no"
        args.description = None
        args.text_description = None
        args.kill_existing_with_same_port = False
        args.port = "localhorst"
        args.port = 1234
        args.cmdline_regex = f"^qlever-server.* -p {args.port}"
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

    # With a server binary that does not know the description options, the
    # descriptions are dropped from the command line with a warning.
    @patch("qlever.commands.start.log")
    @patch("qlever.commands.start.server_supports_description_options")
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("qlever.commands.start.Containerize")
    def test_execute_warns_about_old_server_binary(
        self,
        mock_containerize,
        mock_is_qlever_server_alive,
        mock_run_command,
        mock_supports_description_options,
        mock_log,
    ):
        args = MagicMock()
        args.description = "TestDescription"
        args.text_description = None
        args.kill_existing_with_same_port = False
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.system = "native"
        args.show = False
        mock_supports_description_options.return_value = False
        # Stop right after the warning: the server is already running.
        mock_is_qlever_server_alive.return_value = True
        mock_containerize.supported_systems.return_value = []

        self.assertFalse(StartCommand().execute(args))

        mock_supports_description_options.assert_called_once_with(args)
        mock_log.warning.assert_called_once()
        self.assertIn(
            "descriptions from the Qleverfile are NOT set",
            mock_log.warning.call_args.args[0],
        )
        self.assertIsNone(args.description)
        self.assertIsNone(args.text_description)

    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize")
    @patch("time.sleep")
    @patch("qlever.commands.start.Path")
    def test_execute_successful_server_start(
        self,
        mock_path_cls,
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
        args.restart_policy = "no"
        args.description = None
        args.text_description = None
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

        # Configure Path mock so the log file wait loop is skipped
        self._mock_log_file(mock_path_cls, args.name)

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
    @patch("qlever.commands.start.Path")
    def test_execute_server_with_warmup(
        self,
        mock_path_cls,
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
        args.restart_policy = "no"
        args.description = None
        args.text_description = None
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

        # Configure Path mock so the log file wait loop is skipped
        self._mock_log_file(mock_path_cls, args.name)

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
            f"exec tail -n +1 -f {args.name}.server-log.txt", shell=True
        )

        # Check warmup was called
        mock_run.assert_any_call(args.warmup_cmd, shell=True, check=True)

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
    @patch("qlever.commands.start.binary_exists")
    @patch("qlever.commands.start.Path")
    def test_execute_containerize(
        self,
        mock_path_cls,
        mock_binary_exists,
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
        args.description = None
        args.text_description = None
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

        mock_binary_exists.return_value = True

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
        # Assert that run_command was called exactly twice with the
        # correct arguments in order (the descriptions are part of the
        # server command line, not set via the API after the start)
        mock_run_command.assert_has_calls(
            [call(run_call_1), call(run_call_2, use_popen=False)],
            any_order=False,
        )
        self.assertEqual(mock_run_command.call_count, 2)
        # Server status should be checked
        mock_is_qlever_server_alive.assert_called()
        # Ensure execution was successful
        self.assertTrue(result)

    # With a usable systemd and the default restart policy, the server is
    # started as a systemd unit (after removing a leftover unit of the same
    # name), and the start succeeds once the server answers.
    @patch("qlever.commands.start.CacheStatsCommand.execute")
    @patch("qlever.util.run_command")
    @patch("qlever.commands.start.run_command")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("subprocess.Popen")
    @patch("qlever.commands.start.Containerize")
    @patch("time.sleep")
    @patch("qlever.commands.start.Path")
    @patch("qlever.commands.start.systemd_unit_is_active", return_value=True)
    @patch("qlever.commands.start.stop_systemd_unit", return_value=True)
    @patch("qlever.commands.start.check_systemd_for_restarts")
    def test_execute_starts_systemd_unit(
        self,
        mock_check,
        mock_stop_systemd_unit,
        mock_unit_is_active,
        mock_path_cls,
        mock_sleep,
        mock_containerize,
        mock_popen,
        mock_is_qlever_server_alive,
        mock_start_run_command,
        mock_util_run_command,
        mock_cache_stats_command,
    ):
        args = MagicMock()
        args.restart_policy = None
        args.description = None
        args.text_description = None
        args.kill_existing_with_same_port = False
        args.port = 1234
        args.server_binary = "/test/path/server_binary"
        args.name = "TestName"
        args.system = "native"
        args.run_in_foreground = False
        args.show = False
        args.no_warmup = True
        self._mock_log_file(mock_path_cls, args.name)
        mock_check.return_value = "ok"
        mock_is_qlever_server_alive.side_effect = [False, True]
        mock_containerize.supported_systems.return_value = []
        mock_popen.return_value = MagicMock()

        self.assertTrue(StartCommand().execute(args))

        # The default policy `unless-stopped` becomes `Restart=always`.
        start_cmd = mock_start_run_command.call_args.args[0]
        self.assertIn(
            "systemd-run --user --unit qlever.server.TestName", start_cmd
        )
        self.assertIn(" -p Restart=always ", start_cmd)
        self.assertIn(" /test/path/server_binary -i TestName", start_cmd)
        mock_stop_systemd_unit.assert_called_once_with(
            "qlever.server.TestName"
        )

    # Without a usable systemd, an explicit restart policy is an error, the
    # default falls back to `nohup` with a note, and missing lingering gives
    # a warning. In the latter two cases, the start continues (and stops at
    # the already running server here).
    @patch("qlever.commands.start.StatusCommand.execute")
    @patch("qlever.commands.start.is_qlever_server_alive")
    @patch("qlever.commands.start.binary_exists")
    @patch("qlever.commands.start.Containerize")
    @patch("qlever.commands.start.check_systemd_for_restarts")
    @patch("qlever.commands.start.log")
    def test_execute_restart_policy_without_systemd(
        self,
        mock_log,
        mock_check,
        mock_containerize,
        mock_binary_exists,
        mock_is_alive,
        mock_status,
    ):
        def make_args(restart_policy):
            args = MagicMock()
            args.kill_existing_with_same_port = False
            args.system = "native"
            args.run_in_foreground = False
            args.show = False
            args.restart_policy = restart_policy
            args.name = "TestName"
            args.port = 1234
            args.description = None
            args.text_description = None
            return args

        mock_containerize.supported_systems.return_value = []
        mock_binary_exists.return_value = True
        mock_is_alive.return_value = True

        mock_check.return_value = "no-systemd"
        self.assertFalse(StartCommand().execute(make_args("always")))
        mock_log.error.assert_called_once()
        self.assertIn("need systemd", mock_log.error.call_args.args[0])
        mock_is_alive.assert_not_called()

        mock_log.reset_mock()
        args = make_args(None)
        self.assertFalse(StartCommand().execute(args))
        self.assertEqual(args.restart_policy, "unless-stopped")
        self.assertIn(
            "starting without them", mock_log.info.call_args_list[0].args[0]
        )
        mock_is_alive.assert_called_once()

        mock_log.reset_mock()
        mock_check.return_value = "no-linger"
        self.assertFalse(StartCommand().execute(make_args(None)))
        mock_log.warning.assert_called_once()
        self.assertIn(
            "loginctl enable-linger", mock_log.warning.call_args.args[0]
        )

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


class TestMergeRuntimeParameters(unittest.TestCase):
    def test_command_line_takes_precedence_per_name(self):
        from qlever.commands.start import merge_runtime_parameters

        self.assertEqual(
            merge_runtime_parameters(
                ["a=1", "b=2"],
                ["b=3", "c=4"],
            ),
            ["a=1", "b=3", "c=4"],
        )

    def test_empty_lists(self):
        from qlever.commands.start import merge_runtime_parameters

        self.assertEqual(merge_runtime_parameters([], []), [])
        self.assertEqual(merge_runtime_parameters(["a=1"], []), ["a=1"])
        self.assertEqual(merge_runtime_parameters([], ["a=1"]), ["a=1"])

    def test_qleverfile_parameters_are_read_and_merged(self):
        import tempfile
        from types import SimpleNamespace

        from qlever.commands.start import (
            get_runtime_parameters_from_qleverfile,
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".qleverfile"
        ) as file:
            file.write(
                "[data]\nNAME = test\n\n[server]\n"
                "SET_RUNTIME_PARAMETERS = a=1 b=2\n"
            )
            file.flush()
            args = SimpleNamespace(qleverfile=file.name)
            self.assertEqual(
                get_runtime_parameters_from_qleverfile(args),
                ["a=1", "b=2"],
            )

    def test_no_qleverfile_yields_empty_list(self):
        from types import SimpleNamespace

        from qlever.commands.start import (
            get_runtime_parameters_from_qleverfile,
        )

        args = SimpleNamespace(qleverfile="/nonexistent/Qleverfile")
        self.assertEqual(get_runtime_parameters_from_qleverfile(args), [])


# Tests that `--server-log-mode append` appends to the server log instead
# of truncating it, and that `no-log` writes no log at all.
def test_construct_command_server_log_mode_append_and_no_log():
    args = MagicMock()
    args.name = "TestName"
    args.server_log_mode = "append"
    args.run_in_foreground = False
    args.timeout = False
    args.access_token = False
    args.description = None
    args.text_description = None
    args.persist_updates = False
    args.rebuild_index_strategy = "manual"
    args.rebuild_keep_previous_index_dirs = "original-and-most-recent"
    args.set_runtime_parameters = []
    args.only_pso_and_pos_permutations = False
    args.use_patterns = "yes"
    args.use_text_index = "no"
    args.enable_metrics = False
    args.metrics_log = "yes"
    args.resource_usage_log = "yes"
    args.resource_usage_interval = 2
    args.preload_materialized_views = None

    result = qlever.commands.start.construct_command(args)
    assert result.endswith(f" >> {args.name}.server-log.txt 2>&1")

    # `no-log` in the background discards the output ...
    args.server_log_mode = "no-log"
    args.run_in_foreground = False
    result = qlever.commands.start.construct_command(args)
    assert result.endswith(" > /dev/null 2>&1")
    assert "server-log.txt" not in result

    # ... and in the foreground, it goes to the terminal.
    args.run_in_foreground = True
    result = qlever.commands.start.construct_command(args)
    assert "/dev/null" not in result
    assert "server-log.txt" not in result


# Tests the rotation of the server log: the existing log moves to `.1`,
# older generations shift up, and all generations are kept.
def test_rotate_server_log(tmp_path):
    log_file = tmp_path / "TestName.server-log.txt"

    # Rotating a non-existing log does nothing.
    qlever.commands.start.rotate_server_log(log_file)
    assert list(tmp_path.iterdir()) == []

    # Rotate four times; all generations are kept, newest first.
    for run in range(4):
        log_file.write_text(f"run {run}")
        qlever.commands.start.rotate_server_log(log_file)
        assert not log_file.exists()
    generations = sorted(p.name for p in tmp_path.iterdir())
    assert generations == [
        "TestName.server-log.txt.1",
        "TestName.server-log.txt.2",
        "TestName.server-log.txt.3",
        "TestName.server-log.txt.4",
    ]
    for i in range(1, 5):
        expected = f"run {4 - i}"
        actual = (tmp_path / f"TestName.server-log.txt.{i}").read_text()
        assert actual == expected
