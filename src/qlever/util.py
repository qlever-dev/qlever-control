from __future__ import annotations

import errno
import re
import secrets
import shlex
import shutil
import socket
import string
import subprocess
from datetime import date, datetime
from pathlib import Path
from platform import system
from typing import Any, Optional

import psutil

from qlever.log import log

def get_total_file_size(patterns: list[str]) -> int:
    """
    Helper function that gets the total size of all files mathing the given
    patterns in bytes.
    """

    total_size = 0
    search_dir = Path.cwd()
    for pattern in patterns:
        for file in search_dir.glob(pattern):
            total_size += file.stat().st_size
    return total_size


def run_command(
    cmd: str,
    return_output: bool = False,
    show_output: bool = False,
    show_stderr: bool = False,
    use_popen: bool = False,
    new_session: bool = False,
) -> Optional[str | subprocess.Popen]:
    """
    Run the given command and throw an exception if the exit code is non-zero.
    If `return_output` is `True`, return what the command wrote to `stdout`.

    If 'new_session' is `True`, the command will be started in a new process
    group. NOTE: 'new_session' will only work on POSIX systems

    NOTE: The `set -o pipefail` ensures that the exit code of the command is
    non-zero if any part of the pipeline fails (not just the last part).

    TODO: Find the executable for `bash` in `__init__.py`.
    """

    subprocess_args = {
        "executable": shutil.which("bash"),
        "shell": True,
        "text": True,
        "stdout": None if show_output else subprocess.PIPE,
        "stderr": None if show_stderr else subprocess.PIPE,
    }

    # Add process group isolation if new_session is True
    # (Works only on POSIX systems).
    if new_session and system() != "Windows":
        subprocess_args["start_new_session"] = True

    # With `Popen`, the command runs in the current shell and a process object
    # is returned (which can be used, e.g., to kill the process).
    if use_popen:
        if return_output:
            raise Exception("Cannot return output if `use_popen` is `True`")
        return subprocess.Popen(f"set -o pipefail; {cmd}", **subprocess_args)

    # With `run`, the command runs in a subshell and the output is captured.
    result = subprocess.run(f"set -o pipefail; {cmd}", **subprocess_args)

    # If the exit code is non-zero, throw an exception. If something was
    # written to `stderr`, use that as the exception message. Otherwise, use a
    # generic message (which is also what `subprocess.run` does with
    # `check=True`).
    if result.returncode != 0:
        if len(result.stderr) > 0:
            raise Exception(result.stderr.replace("\n", " ").strip())
        else:
            raise Exception(
                f"Command failed with exit code {result.returncode}, "
                f" nothing written to stderr"
            )
    # Optionally, return what was written to `stdout`.
    if return_output:
        return result.stdout


def run_curl_command(
    url: str,
    headers: dict[str, str] = {},
    params: dict[str, str] = {},
    result_file: Optional[str] = None,
) -> str:
    """
    Run `curl` with the given `url`, `headers`, and `params`. If `result_file`
    is `None`, return the output, otherwise, write the output to the given file
    and return the HTTP code. If the `curl` command fails, throw an exception.

    """
    # Construct and run the `curl` command.
    default_result_file = "/tmp/qlever.curl.result"
    actual_result_file = result_file if result_file else default_result_file
    curl_cmd = (
        f'curl -Ls -o "{actual_result_file}"'
        f' -w "%{{http_code}}\n" {url}'
        + "".join([f' -H "{key}: {value}"' for key, value in headers.items()])
        + "".join(
            [
                f" --data-urlencode {key}={shlex.quote(value)}"
                for key, value in params.items()
            ]
        )
    )
    result = subprocess.run(
        curl_cmd,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Case 1: An error occurred, raise an exception.
    if result.returncode != 0:
        if len(result.stderr) > 0:
            raise Exception(result.stderr)
        else:
            raise Exception(
                f"curl command failed with exit code "
                f"{result.returncode}, stderr is empty"
            )
    # Case 2: Return output (read from `default_result_file`).
    if result_file is None:
        result_file_path = Path(default_result_file)
        result = result_file_path.read_text()
        result_file_path.unlink()
        return result
    # Case 3: Return HTTP code.
    return result.stdout


def is_qlever_server_alive(endpoint_url: str) -> bool:
    """
    Helper function that checks if a QLever server is running on the given
    endpoint. Return `True` if the server is alive, `False` otherwise.
    """

    message = "from the `qlever` CLI"
    curl_cmd = (
        f"curl -s {endpoint_url}/ping"
        f" --data-urlencode msg={shlex.quote(message)}"
    )
    log.debug(curl_cmd)
    try:
        run_command(curl_cmd)
        return True
    except Exception:
        return False


def get_existing_index_files(basename: str) -> list[str]:
    """
    Helper function that returns a list of all index files for `basename` in
    the current working directory.
    """
    existing_index_files = []
    existing_index_files.extend(Path.cwd().glob(f"{basename}.index.*"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.text.*"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.vocabulary.*"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.meta-data.json"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.prefixes"))
    # Return only the file names, not the full paths.
    return [path.name for path in existing_index_files]


def show_process_info(psutil_process, cmdline_regex, show_heading=True):
    """
    Helper function that shows information about a process if information
    about the process can be retrieved and the command line matches the
    given regex (in which case the function returns `True`). The heading is
    only shown if `show_heading` is `True` and the function returns `True`.
    """

    # Helper function that shows a line of the process table.
    def show_table_line(pid, user, start_time, rss, cmdline):
        log.info(f"{pid:<8} {user:<8} {start_time:>5}  {rss:>5} {cmdline}")

    try:
        pinfo = psutil_process.as_dict(
            attrs=["pid", "username", "create_time", "memory_info", "cmdline"]
        )
        # Note: pinfo[`cmdline`] is `None` if the process is a zombie.
        cmdline = " ".join(pinfo["cmdline"] or [])
        if len(cmdline) == 0 or not re.search(cmdline_regex, cmdline):
            return False
        pid = pinfo["pid"]
        user = pinfo["username"] if pinfo["username"] else ""
        start_time = datetime.fromtimestamp(pinfo["create_time"])
        if start_time.date() == date.today():
            start_time = start_time.strftime("%H:%M")
        else:
            start_time = start_time.strftime("%b%d")
        rss = f"{pinfo['memory_info'].rss / 1e9:.0f}G"
        if show_heading:
            show_table_line("PID", "USER", "START", "RSS", "COMMAND")
        show_table_line(pid, user, start_time, rss, cmdline)
        return True
    except Exception as e:
        log.error(f"Could not get process info: {e}")
        return False


def get_random_string(length: int) -> str:
    """
    Helper function that returns a randomly chosen string of the given
    length. Take the current time as seed.
    """
    characters = string.ascii_letters + string.digits
    return "".join(secrets.choice(characters) for _ in range(length))


def is_port_used(port: int) -> bool:
    """
    Try to bind to the port on all interfaces to check if the port is already in use.
    If the port is already in use, `socket.bind` will raise an `OSError` with errno EADDRINUSE.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Ensure that the port is not blocked after the check.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", port))
        sock.close()
        return False
    except OSError as err:
        if err.errno != errno.EADDRINUSE:
            log.warning(f"Failed to determine if port is used: {err}")
        return True


def format_size(bytes, suffix="B"):
    """
    Scale bytes to its proper format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f} {unit}{suffix}"
        bytes /= factor


def stop_process(proc: psutil.Process, pinfo: dict[str, Any]) -> bool:
    """
    Try to kill the given process, return True iff it was killed
    successfully. The process_info is used for logging.
    """
    try:
        proc.kill()
        log.info(f"Killed process {pinfo['pid']}")
        return True
    except Exception as e:
        log.error(
            f"Could not kill process with PID "
            f"{pinfo['pid']} ({e}) ... try to kill it "
            f"manually"
        )
        log.info("")
        show_process_info(proc, "", show_heading=True)
        return False


def stop_process_with_regex(cmdline_regex: str) -> list[bool] | None:
    """
    Given a cmdline_regex for a native process, try to kill the processes that
    match the regex and return a list of their stopped status (bool).
    Show the matched processes as log info.
    """
    stop_process_results = []
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(
                attrs=[
                    "pid",
                    "username",
                    "create_time",
                    "memory_info",
                    "cmdline",
                ]
            )
            cmdline = " ".join(pinfo["cmdline"])
        except Exception as e:
            log.debug(f"Error getting process info: {e}")
            return None
        if re.search(cmdline_regex, cmdline):
            log.info(
                f"Found process {pinfo['pid']} from user "
                f"{pinfo['username']} with command line: {cmdline}"
            )
            log.info("")
            stop_process_results.append(stop_process(proc, pinfo))
    return stop_process_results


def binary_exists(binary: str, cmd_arg: str) -> bool:
    """
    When a command is run natively, check if the binary exists on the system
    """
    try:
        run_command(f"{binary} --help")
        return True
    except Exception as e:
        log.error(
            f'Running "{binary}" failed, '
            f"set `--{cmd_arg}` to a different binary or "
            f"set `--system to a container system`"
        )
        log.info("")
        log.info(f"The error message was: {e}")
        return False


def is_server_alive(url: str) -> bool:
    """
    Check if the server is already alive at the given endpoint url
    """
    check_server_cmd = f"curl -s {url}"
    try:
        run_command(check_server_cmd)
        return True
    except Exception:
        return False
