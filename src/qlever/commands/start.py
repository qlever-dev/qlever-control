from __future__ import annotations

import contextlib
import os
import platform
import shlex
import shutil
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import psutil

from qlever.command import QleverCommand
from qlever.commands.cache_stats import CacheStatsCommand
from qlever.commands.settings import SettingsCommand
from qlever.commands.status import StatusCommand
from qlever.commands.stop import StopCommand
from qlever.commands.warmup import WarmupCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.qleverfile import Qleverfile
from qlever.util import (
    binary_exists,
    binary_help_command,
    is_qlever_server_alive,
    run_command,
    stop_systemd_unit,
    stop_tailing,
    systemd_linger_status,
    systemd_unit_is_active,
    systemd_unit_name,
    systemd_user_env,
    tail_log_file,
)


# Construct the command line based on the config file. With `use_systemd`,
# the command has no redirect of its output, because the systemd unit takes
# care of the log (see `wrap_command_in_systemd_unit`).
def construct_command(args, use_systemd: bool = False) -> str:
    start_cmd = (
        f"{args.server_binary}"
        f" -i {args.name}"
        f" -j {args.num_threads}"
        f" -p {args.port}"
        f" -m {args.memory_for_queries}"
        f" -c {args.cache_max_size}"
        f" -e {args.cache_max_size_single_entry}"
        f" -k {args.cache_max_num_entries}"
    )

    if args.timeout:
        start_cmd += f" -s {args.timeout}"
    if args.access_token:
        start_cmd += f" -a {args.access_token}"
    if args.description:
        start_cmd += f" --index-description {shlex.quote(args.description)}"
    if args.text_description:
        start_cmd += (
            f" --text-description {shlex.quote(args.text_description)}"
        )
    if args.persist_updates:
        start_cmd += " --persist-updates"
    # Only pass the flags for non-default values, so that older server
    # binaries without these options keep working.
    if args.rebuild_index_strategy != "manual":
        start_cmd += f" --rebuild-index-strategy {args.rebuild_index_strategy}"
    if args.rebuild_keep_previous_index_dirs != "original-and-most-recent":
        start_cmd += (
            f" --rebuild-keep-previous-index-dirs"
            f" {args.rebuild_keep_previous_index_dirs}"
        )
    # Merge the runtime parameters from the Qleverfile with those from the
    # command line. The command line takes precedence per parameter name, so
    # specifying one parameter on the command line does not silently drop
    # the other parameters from the Qleverfile. One `--set-runtime-parameter`
    # per assignment (the option is not multitoken).
    set_runtime_parameters = merge_runtime_parameters(
        get_runtime_parameters_from_qleverfile(args),
        vars(args).get("set_runtime_parameters") or [],
    )
    if set_runtime_parameters:
        start_cmd += "".join(
            f" --set-runtime-parameter {shlex.quote(assignment)}"
            for assignment in set_runtime_parameters
        )
    if args.only_pso_and_pos_permutations:
        start_cmd += " --only-pso-and-pos-permutations"
    if args.use_patterns == "no":
        start_cmd += " --no-patterns"
    if args.use_text_index == "yes":
        start_cmd += " -t"
    if args.enable_metrics:
        start_cmd += " --enable-metrics"
    if args.metrics_log == "no":
        start_cmd += " --no-metrics-log"
    # The server samples its own RSS and CPU usage by default. Only
    # pass the flags for non-default settings, so that older binaries
    # without these options keep working.
    if args.resource_usage_log == "no":
        start_cmd += " --no-resource-usage-log"
    elif args.resource_usage_interval != 2:
        start_cmd += (
            f" --resource-usage-interval-s {args.resource_usage_interval}"
        )
    preload_materialized_views = vars(args).get("preload_materialized_views")
    if preload_materialized_views:
        start_cmd += " --preload-materialized-views"
        start_cmd += "".join(
            f" {shlex.quote(view_name)}"
            for view_name in preload_materialized_views
        )
    if use_systemd:
        return start_cmd
    if args.server_log_mode == "no-log":
        # No log file is written. In the foreground, the server output
        # goes to the terminal; in the background, it is discarded.
        if not args.run_in_foreground:
            start_cmd += " > /dev/null 2>&1"
    else:
        redirect = ">>" if args.server_log_mode == "append" else ">"
        start_cmd += f" {redirect} {args.name}.server-log.txt 2>&1"
    return start_cmd


# Kill existing server on the same port. Trust that StopCommand() works?
# Maybe return StopCommand().execute(args) and handle it with a try except?
def kill_existing_server(args) -> bool:
    args.cmdline_regex = rf"^(\S*/)?qlever-server.* -p {args.port}"
    args.no_containers = True
    if not StopCommand().execute(args):
        log.error("Stopping the existing server failed")
        return False
    log.info("")
    return True


# Run the command in a container
def wrap_command_in_container(args, start_cmd) -> str:
    if not args.server_container:
        args.server_container = f"qlever.server.{args.name}"
    run_subcmd = f"run --restart={args.restart_policy}"
    if not args.run_in_foreground:
        run_subcmd += " -d"
    start_cmd = Containerize().containerize_command(
        start_cmd,
        args.system,
        run_subcmd,
        args.image,
        args.server_container,
        volumes=[("$(pwd)", "/index")],
        ports=[(args.port, args.port)],
        working_directory="/index",
        seccomp_profile=args.seccomp_profile,
    )
    return start_cmd


# Run the command as a transient systemd user service. Like a container with
# a restart policy, the service restarts the server after a crash, and it has
# to be stopped via `systemctl` (which `stop` does). Unlike a container, the
# server runs natively, in the current directory.
def wrap_command_in_systemd_unit(args, start_cmd) -> str:
    # Outside of a login session (cron), point `systemd-run` to the user's
    # systemd instance (see `systemd_user_env`).
    env = systemd_user_env()
    prefix = "".join(
        f"{var}={env[var]} "
        for var in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS")
        if var not in os.environ
    )
    # For systemd, a server killed with `SIGTERM` (as `earlyoom` does it) has
    # exited cleanly, so only `always` also covers that case. The server is
    # restarted at once (it can rebind its port right away), and the start
    # limit ends a crash loop (a server that dies right after each start).
    restart = (
        "always"
        if args.restart_policy == "unless-stopped"
        else args.restart_policy
    )
    unit_cmd = (
        f"{prefix}systemd-run --user"
        f" --unit {shlex.quote(systemd_unit_name(args.name))}"
        ' --working-directory "$(pwd)"'
        f" -p Restart={restart} -p RestartSec=0"
        " -p StartLimitIntervalSec=1h -p StartLimitBurst=20"
        " -p Delegate=yes"
    )
    # The server log is appended by the unit, so that a restart after a crash
    # does not truncate the log of the crashed run. The rotation or removal
    # of an existing log according to `--server-log-mode` still happens in
    # `execute`, before the unit is created.
    if args.server_log_mode == "no-log":
        unit_cmd += " -p StandardOutput=null"
    else:
        # The path has to be absolute.
        unit_cmd += (
            ' -p StandardOutput=append:"$(pwd)"/'
            f"{shlex.quote(args.name + '.server-log.txt')}"
        )
    unit_cmd += " -p StandardError=inherit"
    return f"{unit_cmd} {start_cmd}"


# Whether a native server can run as a systemd user service that restarts it
# after a crash. Returns "ok", "no-systemd" (not Linux, no `systemd-run`, or no
# user instance of systemd), or "no-linger" (lingering is not enabled for the
# user, so the service would end together with the login session).
def check_systemd_for_restarts() -> str:
    if platform.system() != "Linux" or shutil.which("systemd-run") is None:
        return "no-systemd"
    linger = systemd_linger_status()
    if linger is None:
        return "no-systemd"
    return "ok" if linger == "yes" else "no-linger"


def server_supports_description_options(args) -> bool:
    """
    Whether the server binary knows the options `--index-description` and
    `--text-description` (added to `qlever-server` in September 2026),
    according to its `--help` output. A binary that cannot be run at all
    counts as supporting them, so that the subsequent `binary_exists` check
    reports the actual problem.
    """
    try:
        help_text = run_command(
            binary_help_command(args.server_binary, args), return_output=True
        )
    except Exception:
        return True
    return "--index-description" in help_text


def get_runtime_parameters_from_qleverfile(args) -> list[str]:
    """
    Return the value of `SET_RUNTIME_PARAMETERS` from the Qleverfile as a
    list of `name=value` assignments, or an empty list if there is no
    Qleverfile or no such option.
    """
    try:
        qleverfile_path = Path(vars(args).get("qleverfile", "Qleverfile"))
        if not qleverfile_path.is_file():
            return []
        # The engine short name is only used for the default container names,
        # which are not read here.
        config = Qleverfile.read(
            qleverfile_path, vars(args).get("engine_short_name", "qlever")
        )
        value = config.get("server", "set_runtime_parameters", fallback=None)
        return shlex.split(value) if value else []
    except Exception:
        return []


def merge_runtime_parameters(
    from_qleverfile: list[str], from_command_line: list[str]
) -> list[str]:
    """
    Merge two lists of `name=value` assignments. Assignments from the
    command line take precedence over assignments for the same name from
    the Qleverfile. The order is that of the Qleverfile, with additional
    names from the command line appended.
    """
    merged = {
        assignment.split("=", 1)[0]: assignment
        for assignment in from_qleverfile
    }
    for assignment in from_command_line:
        merged[assignment.split("=", 1)[0]] = assignment
    return list(merged.values())


def rotate_server_log(log_file: Path) -> None:
    """
    Move an existing server log to `<log>.1`, first shifting all older
    generations up by one (`<log>.1` -> `<log>.2`, ...). All generations
    are kept.
    """
    if not log_file.exists():
        return
    num_old = 0
    while Path(f"{log_file}.{num_old + 1}").exists():
        num_old += 1
    for i in range(num_old, 0, -1):
        Path(f"{log_file}.{i}").rename(f"{log_file}.{i + 1}")
    log_file.rename(f"{log_file}.1")


def show_log_follow_info(log_name: str, run_in_foreground: bool) -> None:
    """
    Tell the user which log is being followed, until when, and what
    Ctrl-C does. The two cases differ in whether Ctrl-C stops the
    server, so the wording is centralized here instead of being
    repeated at each call site.
    """
    if run_in_foreground:
        log.info(
            f"Follow {log_name} as long as the server is running "
            "(Ctrl-C stops the server)"
        )
    else:
        log.info(
            f"Follow {log_name} until the server is ready "
            "(Ctrl-C stops following the log, but NOT the server)"
        )
    log.info("")


def make_server_liveness_check(
    args,
    process: subprocess.Popen | None,
    pid: int | None,
    use_systemd: bool = False,
) -> Callable[[], bool]:
    """
    Build a check that tells whether the server is still running: via the
    container runtime, via systemd, via the `Popen` handle (foreground), or
    via the `pid` of the process started with `nohup`.
    """
    if args.system in Containerize.supported_systems():
        return lambda: Containerize.is_running(
            args.system, args.server_container
        )
    if use_systemd:
        unit = systemd_unit_name(args.name)
        return lambda: systemd_unit_is_active(unit)
    if args.run_in_foreground:
        return lambda: process.poll() is None
    if pid is not None:
        return lambda: psutil.pid_exists(pid)
    return lambda: True


def wait_until_server_ready(
    is_alive: Callable[[], bool],
    is_still_running: Callable[[], bool],
    poll_interval_s: float = 1.0,
) -> bool:
    """
    Poll until the server answers. Returns False if the server process
    exited before it became ready (e.g. because of a corrupt index).
    """
    while not is_alive():
        if not is_still_running():
            log.error("Server process exited before becoming ready")
            return False
        time.sleep(poll_interval_s)
    return True


def wait_for_foreground_server(
    process: subprocess.Popen,
    log_proc: subprocess.Popen | None,
    on_interrupt: Callable[[], None],
) -> None:
    """
    Wait until the server started in the foreground is stopped. On
    Ctrl-C, terminate it and call `on_interrupt` for engine-specific
    cleanup (such as removing the server container).
    """
    try:
        process.wait()
    except KeyboardInterrupt:
        log.warning("\rCtrl-C pressed, stopping the server ...")
        log.info("")
        process.terminate()
        on_interrupt()
    if log_proc is not None:
        stop_tailing(log_proc)


class StartCommand(QleverCommand):
    """
    Class for executing the `start` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Start the QLever server (requires that you have built "
            "an index with the `index` command before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name", "description", "text_description"],
            "server": [
                "server_binary",
                "host_name",
                "port",
                "access_token",
                "memory_for_queries",
                "cache_max_size",
                "cache_max_size_single_entry",
                "cache_max_num_entries",
                "num_threads",
                "timeout",
                "persist_updates",
                "rebuild_index_strategy",
                "rebuild_keep_previous_index_dirs",
                "set_runtime_parameters",
                "only_pso_and_pos_permutations",
                "use_patterns",
                "use_text_index",
                "metrics_log",
                "resource_usage_log",
                "resource_usage_interval",
                "preload_materialized_views",
                "server_log_mode",
                "warmup_cmd",
                "enable_metrics",
            ],
            "runtime": [
                "system",
                "image",
                "server_container",
                "restart_policy",
                "seccomp_profile",
            ],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--kill-existing-with-same-port",
            action="store_true",
            default=False,
            help="If a QLever server is already running "
            "on the same port, kill it before "
            "starting a new server",
        )
        subparser.add_argument(
            "--no-warmup",
            action="store_true",
            default=False,
            help="Do not execute the warmup command",
        )
        subparser.add_argument(
            "--run-in-foreground",
            action="store_true",
            default=False,
            help="Run the server in the foreground "
            "(default: run in the background with `nohup`)",
        )
        subparser.add_argument(
            "runtime_parameters",
            nargs="*",
            help="Space-separated list of runtime parameters to set "
            "(in the form `key=value`) once the server is running",
        ).completer = lambda **kwargs: [
            f"{key}=" for key in Qleverfile.SERVER_RUNTIME_PARAMETERS
        ]

    def execute(self, args) -> bool:
        # Set the endpoint URL.
        args.endpoint_url = f"http://{args.host_name}:{args.port}"

        # The restart policy has no default in the Qleverfile, so that an
        # explicitly set policy can be told apart from the default (which
        # only applies where automatic restarts are possible, see below).
        restart_policy_is_explicit = args.restart_policy is not None
        if not restart_policy_is_explicit:
            args.restart_policy = "unless-stopped"

        # Kill existing server with the same name if so desired.
        #
        # TODO: This is currently disabled because I never used it once over
        # the past weeks and it is not clear to me what the use case is.
        if False:  # or args.kill_existing_with_same_name:
            args.cmdline_regex = rf"^(\S*/)?qlever-server.* -i {args.name}"
            args.no_containers = True
            StopCommand().execute(args)
            log.info("")

        # Kill existing server on the same port if so desired.
        if args.kill_existing_with_same_port:
            if not kill_existing_server(args):
                return False

        # The descriptions are options of the server binary since September
        # 2026. With an older binary, start without them and say so (there is
        # deliberately no fallback to setting them via the API afterwards).
        if (
            not args.show
            and (args.description or args.text_description)
            and not server_supports_description_options(args)
        ):
            log.warning(
                "The server binary does not know the options "
                "`--index-description` and `--text-description`, so the "
                "descriptions from the Qleverfile are NOT set. Please use the "
                "latest version of `qlever-server`"
                + (
                    f" (`{args.system} pull {args.image}`)"
                    if args.system in Containerize.supported_systems()
                    else ""
                )
            )
            log.info("")
            args.description = None
            args.text_description = None

        # A native server with a restart policy runs as a systemd user service
        # (see `wrap_command_in_systemd_unit`), if that is possible. If not,
        # an explicitly set policy is an error, the default just falls back
        # to `nohup`.
        use_systemd = False
        if (
            args.system == "native"
            and not args.run_in_foreground
            and args.restart_policy != "no"
        ):
            status = check_systemd_for_restarts()
            if status == "ok":
                use_systemd = True
            elif status == "no-systemd":
                message = (
                    "Automatic restarts of the server (see "
                    "`--restart-policy`) are not available on this system, "
                    "they need systemd"
                )
                if restart_policy_is_explicit:
                    log.error(message)
                    return False
                log.info(f"{message}, starting without them")
                log.info("")
            else:
                log.warning(
                    "Automatic restarts of the server (see "
                    "`--restart-policy`) need lingering to be enabled for "
                    "your user, so that the server survives the end of your "
                    "login session. Enable it once with `loginctl "
                    "enable-linger`. Until then, the server starts without "
                    "automatic restarts"
                )
                log.info("")

        # Construct the command line based on the config file.
        start_cmd = construct_command(args, use_systemd)

        # Run the command in a container or as a systemd service (see above).
        # Otherwise run with `nohup` so that it keeps running after the shell
        # is closed. With `--run-in-foreground`, run the server in the
        # foreground.
        if args.system in Containerize.supported_systems():
            start_cmd = wrap_command_in_container(args, start_cmd)
        elif use_systemd:
            start_cmd = wrap_command_in_systemd_unit(args, start_cmd)
        elif args.run_in_foreground:
            start_cmd = f"{start_cmd}"
        else:
            # The `echo $!` reports the PID of the server process, which the
            # liveness check below uses to detect a server that exits before
            # it becomes ready (see `make_server_liveness_check`).
            start_cmd = f"nohup {start_cmd} & echo $!"

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            if args.runtime_parameters:
                log.info("")
                SettingsCommand().execute(args)
            return True

        if not binary_exists(args.server_binary, "server-binary", args):
            return False

        # Check if a QLever server is already running on this port.
        if is_qlever_server_alive(args.endpoint_url):
            log.error(f"QLever server already running on {args.endpoint_url}")
            log.info("")
            log.info(
                f"To kill the existing server, use `{args.main_command_name} "
                f"stop` or `{args.main_command_name} start` with option "
                "--kill-existing-with-same-port`"
            )

            # Show output of status command.
            args.cmdline_regex = rf"^(\S*/)?qlever-server.* -p *{args.port}"
            log.info("")
            StatusCommand().execute(args)
            return False

        # A leftover unit from a previous start (for example, one that hit
        # the start limit after a crash loop) would prevent the new one.
        if use_systemd:
            unit = systemd_unit_name(args.name)
            if stop_systemd_unit(unit):
                log.info(f'Removed the leftover systemd unit "{unit}"')
                log.info("")

        # Remove already existing container.
        if (
            args.system in Containerize.supported_systems()
            and args.kill_existing_with_same_port
        ):
            try:
                run_command(f"{args.system} rm -f {args.server_container}")
            except Exception as e:
                log.error(f"Removing existing container failed: {e}")
                return False

        # Check if another process is already listening.
        # if self.net_connections_enabled:
        #     if port in [conn.laddr.port for conn
        #                 in psutil.net_connections()]:
        #         log.error(f"Port {port} is already in use by another process"
        #                   f" (use `lsof -i :{port}` to find out which one)")
        #         return False

        # Handle an existing log file from a previous server run according
        # to `--server-log-mode`: keep it and append (`append`), remove it
        # (`overwrite`), move it to `.1`, `.2`, ... (`rotate`, the
        # default), or write no log at all (`no-log`). For `overwrite` and
        # `rotate`, the wait loop below then correctly waits for the server
        # to create a fresh log.
        log_file = Path(f"{args.name}.server-log.txt")
        if args.server_log_mode == "rotate":
            rotate_server_log(log_file)
        elif args.server_log_mode == "overwrite":
            log_file.unlink(missing_ok=True)

        # Execute the command line.
        # For a server started with `nohup`, the `echo $!` in the command
        # reports its PID, which the liveness check below uses (a server in a
        # container or in the foreground is checked via the container runtime
        # or the process handle instead, see `make_server_liveness_check`).
        capture_pid = (
            not args.run_in_foreground
            and args.system not in Containerize.supported_systems()
            and not use_systemd
        )
        pid = None
        try:
            if capture_pid:
                output = run_command(start_cmd, return_output=True)
                process = None
                with contextlib.suppress(ValueError, AttributeError):
                    pid = int(output.strip().splitlines()[-1])
            else:
                # For `no-log` in the foreground, the command has no
                # redirection, so the output must go to the terminal
                # (otherwise it would fill a pipe that nobody reads and
                # eventually block the server).
                if args.run_in_foreground and args.server_log_mode == "no-log":
                    process = run_command(
                        start_cmd,
                        use_popen=True,
                        show_output=True,
                        show_stderr=True,
                    )
                else:
                    process = run_command(
                        start_cmd,
                        use_popen=args.run_in_foreground,
                    )
        except Exception as e:
            log.error(f"Starting the QLever server failed ({e})")
            return False

        # Tail the server log until the server is ready (note that the `exec`
        # is important to make sure that the tail process is killed and not
        # just the bash process).
        if args.server_log_mode == "no-log":
            if args.run_in_foreground:
                log.info(
                    "No server log is written, the server output goes to "
                    "this terminal (Ctrl-C stops the server)"
                )
            else:
                log.info("Server log disabled (`--server-log-mode no-log`)")
            log.info("")
            tail_proc = None
        else:
            show_log_follow_info(str(log_file), args.run_in_foreground)
            # With `append`, only follow what the new server run writes,
            # not the content of the previous runs. In the background, stop
            # following the log as soon as the server says it is ready
            # (the queries that a busy server logs right after that would
            # otherwise scroll the startup messages away).
            tail_proc = tail_log_file(
                log_file,
                from_beginning=args.server_log_mode != "append",
                stop_after=None
                if args.run_in_foreground
                else "The server is ready",
            )
            if tail_proc is None:
                if use_systemd:
                    stop_systemd_unit(systemd_unit_name(args.name))
                return False
        try:
            server_ready = wait_until_server_ready(
                lambda: is_qlever_server_alive(args.endpoint_url),
                make_server_liveness_check(args, process, pid, use_systemd),
            )
        except KeyboardInterrupt:
            # The tail runs in a session of its own (see `tail_log_file`), so
            # the Ctrl-C does not reach it.
            if tail_proc is not None:
                stop_tailing(tail_proc)
            raise
        if not server_ready:
            if tail_proc is not None:
                stop_tailing(tail_proc)
            # A server that dies before it is ready has a problem with its
            # configuration or its index, which restarting does not solve. So
            # stop the unit right away, instead of letting it restart the
            # server again and again until the start limit is reached.
            if use_systemd:
                stop_systemd_unit(systemd_unit_name(args.name))
            return False

        # Stop following the log.
        if not args.run_in_foreground and tail_proc is not None:
            stop_tailing(tail_proc)

        # Execute the warmup command.
        if args.warmup_cmd and not args.no_warmup:
            log.info("")
            if not WarmupCommand().execute(args):
                log.error("Warmup failed")
                return False

        # Show cache stats.
        if not args.run_in_foreground:
            log.info("")
            args.detailed = False
            args.sparql_endpoint = None
            CacheStatsCommand().execute(args)

        # Apply settings if any.
        if args.runtime_parameters:
            log.info("")
            SettingsCommand().execute(args)

        # With `--run-in-foreground`, wait until the server is stopped.
        if args.run_in_foreground:

            def stop_container() -> None:
                if args.system in Containerize.supported_systems():
                    args.cmdline_regex = "qlever-server.* -i [^ ]*%%NAME%%"
                    args.no_containers = False
                    StopCommand().execute(args)

            wait_for_foreground_server(process, tail_proc, stop_container)

        return True
