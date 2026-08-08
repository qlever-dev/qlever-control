from __future__ import annotations

import argparse
import errno
import glob
import re
import secrets
import shlex
import shutil
import socket
import string
import subprocess
import time
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any, NamedTuple, Optional

import psutil

from qlever import script_name
from qlever.log import log


def get_total_file_size(
    patterns: list[str], exclude: set[str] | None = None
) -> int:
    """
    Helper function that gets the total size of all files matching the given
    patterns in bytes. Files whose names match any entry in `exclude` are
    skipped.
    """
    if not exclude:
        exclude = set()
    total_size = 0
    search_dir = Path.cwd()
    for pattern in patterns:
        for file in search_dir.glob(pattern):
            if file.name not in exclude:
                total_size += file.stat().st_size
    return total_size


def run_command(
    cmd: str,
    return_output: bool = False,
    show_output: bool = False,
    show_stderr: bool = False,
    use_popen: bool = False,
) -> Optional[str | subprocess.Popen]:
    """
    Run the given command and throw an exception if the exit code is non-zero.
    If `return_output` is `True`, return what the command wrote to `stdout`.

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
        # `result.stderr` is `None` when stderr was not captured (i.e., when
        # `show_stderr` is `True` and it went straight to the terminal).
        stderr = result.stderr or ""
        if len(stderr) > 0:
            raise Exception(stderr.replace("\n", " ").strip())
        else:
            raise Exception(
                f"Command failed with exit code {result.returncode}, "
                f" nothing written to stderr (stdout: {result.stdout})"
            )
    # Optionally, return what was written to `stdout`.
    if return_output:
        return result.stdout


def run_curl_command(
    url: str,
    headers: dict[str, str] = {},
    params: dict[str, str] = {},
    result_file: str | None = None,
    max_time: int | None = None,
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
    if max_time is not None:
        curl_cmd += f" --max-time {int(max_time)}"
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


def pretty_printed_query(
    query: str, show_prefixes: bool, system: str = "docker"
) -> str | None:
    """
    Pretty-print a SPARQL query using the sparql-formatter Docker image.
    Optionally strips PREFIX declarations from the output. Argument
    `system` can either be docker or podman. Returns None if the query
    could not be pretty-printed.
    """
    from qlever.containerize import Containerize

    if system not in Containerize.supported_systems():
        system = "docker"
    remove_prefixes_cmd = " | sed '/^PREFIX /Id'" if not show_prefixes else ""
    pretty_print_query_cmd = (
        f"echo {shlex.quote(query)}"
        f" | {system} run -i --rm docker.io/sparqling/sparql-formatter"
        f"{remove_prefixes_cmd} | grep -v '^$'"
    )
    try:
        query_pretty_printed = run_command(
            pretty_print_query_cmd, return_output=True
        )
        return query_pretty_printed.rstrip()
    except Exception as e:
        log.debug(f"Failed to pretty-print query, returning None: {e}")
        return None


def is_qlever_server_alive(
    endpoint_url: str, max_time: int | None = None
) -> bool:
    """Check if a QLever server is running on the given endpoint.

    `max_time` (seconds) caps the curl invocation when set; default is
    unbounded so existing callers behave as before.
    """
    message = "from the `qlever` CLI"
    max_time_flag = f"--max-time {max_time} " if max_time is not None else ""
    curl_cmd = (
        f"curl -s {max_time_flag}{endpoint_url}/ping"
        f" --data-urlencode msg={shlex.quote(message)}"
    )
    log.debug(curl_cmd)
    try:
        run_command(curl_cmd)
        return True
    except Exception:
        return False


def get_existing_index_files(
    basename: str, add_non_essential: bool = False
) -> list[str]:
    """
    Helper function that returns a list of all index files for `basename` in
    the current working directory.
    """

    # Essential index files.
    existing_index_files = []
    existing_index_files.extend(Path.cwd().glob(f"{basename}.index.*"))
    existing_index_files.extend(
        Path.cwd().glob(f"{basename}.internal.index.*")
    )
    existing_index_files.extend(Path.cwd().glob(f"{basename}.text.*"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.vocabulary.*"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.meta-data.json"))
    existing_index_files.extend(Path.cwd().glob(f"{basename}.prefixes"))

    # Non-essential index files.
    if add_non_essential:
        existing_index_files.extend(Path.cwd().glob(f"{basename}.view.*"))
        existing_index_files.extend(
            Path.cwd().glob(f"{basename}.settings.json")
        )
        existing_index_files.extend(
            Path.cwd().glob(f"{basename}.index-log.txt")
        )
        existing_index_files.extend(
            Path.cwd().glob(f"{basename}.server-log.txt")
        )

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


def find_process_by_binary(
    parent_pid: int | None, binary: str
) -> psutil.Process | None:
    """Find a descendant of parent_pid whose argv[0] basename matches binary."""
    try:
        root = psutil.Process(parent_pid)
        candidates = [root] + root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None
    # Both sides use Path(...).name so any path form (relative, absolute,
    # bare, symlink) matches as long as argv[0] preserves the binary's name.
    target = Path(binary).name
    for proc in candidates:
        try:
            cmdline = proc.cmdline()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if cmdline and Path(cmdline[0]).name == target:
            return proc
    return None


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
            cmdline = (
                " ".join(pinfo["cmdline"])
                if isinstance(pinfo["cmdline"], list)
                else ""
            )
        except Exception as e:
            # For some processes (e.g., zombies), getting info may fail.
            log.debug(f"Error getting process info: {e}")
            continue
        if re.search(cmdline_regex, cmdline):
            log.info(
                f"Found process {pinfo['pid']} from user "
                f"{pinfo['username']} with command line: {cmdline}"
            )
            log.info("")
            stop_process_results.append(stop_process(proc, pinfo))
    return stop_process_results


def binary_exists(binary: str, cmd_arg: str, args) -> bool:
    """
    Check if the binary exists on the user's system. If running inside a
    container, check if the binary exists inside the container system.
    """
    from qlever.containerize import Containerize

    is_containerized = args.system in Containerize.supported_systems()
    cmd = f"{binary} --help"
    if is_containerized and script_name == "qlever":
        cmd = Containerize().containerize_command(
            cmd,
            args.system,
            "run --rm",
            args.image,
            "qlever.check-binary",
            volumes=[("$(pwd)", "/index")],
            working_directory="/index",
        )

    try:
        run_command(cmd)
        return True
    except Exception as e:
        if is_containerized and (
            binary == "qlever-index" or binary == "qlever-server"
        ):
            log.error(
                f'Running "{binary}" failed. '
                f"This might be because you are using a newer version of "
                f"the `qlever` command-line tool together with an older "
                f"Docker image; in that case update with "
                f"`{args.system} pull {args.image}` "
            )
        else:
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


def input_files_exist(input_files: str) -> bool:
    """
    Check if all of the input files exist in current working directory.
    """
    for pattern in shlex.split(input_files):
        if len(glob.glob(pattern)) == 0:
            log.error(f'No file matching "{pattern}" found')
            log.info("")
            log.info(
                f"Did you call `{script_name} get-data`? If you did, "
                "check GET_DATA_CMD and INPUT_FILES in the Qleverfile"
            )
            return False
    return True


def build_image(build_cmd: str, system: str, image: str) -> bool:
    """
    Build a container image using the build command, container system and
    image name. This method is supposed to be used before executing the index
    command and the logs show that.
    """
    log.info(f"Building {system} image {image}...")
    try:
        run_command(build_cmd, show_output=True, show_stderr=True)
        log.info(
            f"Finished building {system} image {image}! "
            "Continuing with index operation...\n"
        )
        return True
    except Exception as e:
        log.error(f"Building the {system} image {image} failed: {e}")
        return False


def get_container_image_id(system: str, image: str) -> str:
    """
    Get the container image ID to check if the image exists on the system.
    """
    try:
        image_id = run_command(
            f"{system} images -q {image}", return_output=True
        )
    except Exception as e:
        log.info(
            f"Couldn't identify if {system} image {image} "
            f"exists on the system : {e}"
        )
        log.info(
            "Assuming that the image doesn't exist and the image would "
            "be built.\n"
        )
        image_id = ""
    return image_id


def edit_option_line(
    line: str, new_value: str, is_suffix: bool, comment_prefix: str | None
) -> str:
    """
    Return `line` with its value replaced by `new_value`, or with
    `new_value` appended to it if `is_suffix` is true. An inline
    comment after the value is kept.
    """
    # Split off an inline comment (whitespace followed by the comment
    # prefix) so that only the value part is edited.
    value_part = line
    comment_part = ""
    if comment_prefix is not None:
        comment_match = re.search(rf"\s{re.escape(comment_prefix)}", line)
        if comment_match:
            value_part = line[: comment_match.start()]
            comment_part = "\t" + line[comment_match.start() :].strip()

    if is_suffix:
        new_line = value_part.rstrip() + new_value
    else:
        # Keep everything up to and including the `=` and the spacing
        # after it, replace the old value.
        prefix_end = re.match(r"^\s*\S+\s*=\s*", value_part).end()
        new_line = value_part[:prefix_end] + new_value
    return new_line + comment_part


def update_ini_values(
    lines: list[str],
    updates: dict[str, dict[str, tuple[str, bool]]],
    comment_prefix: str | None = None,
) -> list[str]:
    """
    Update values in INI-style file content given as `lines` and return
    the modified lines, preserving comments and unrelated lines.

    `updates` maps `{section: {option: (new_value, is_suffix)}}`. An
    existing option gets its value replaced, or `new_value` appended to
    it if `is_suffix` is true. A missing option is added at the end of
    its section, a missing section at the end of the file (suffix
    entries are skipped there, they have no value to append to).

    `comment_prefix` is the inline comment character of the format
    (`;` for `virtuoso.ini`). If None, the format has no inline
    comments and the whole line is treated as the value.
    """
    options_applied = {section: set() for section in updates}
    sections_seen = set()
    result_lines = []
    current_section = None

    def missing_option_lines(section: str) -> list[str]:
        """
        Lines for options of `section` that were not found in the file.
        """
        return [
            f"{option} = {value}"
            for option, (value, is_suffix) in updates[section].items()
            if option not in options_applied[section] and not is_suffix
        ]

    def flush_missing_options(section: str):
        """
        Insert options of `section` that were not found in the file,
        before any blank lines that separate it from the next section.
        """
        insert_at = len(result_lines)
        while insert_at > 0 and result_lines[insert_at - 1].strip() == "":
            insert_at -= 1
        result_lines[insert_at:insert_at] = missing_option_lines(section)

    for line in lines:
        stripped = line.strip()

        # Section headers like `[Parameters]`. Commented-out headers
        # like `;[Striping]` do not match because of the `^` anchor.
        header_match = re.match(r"^\[([^\]]+)\]", stripped)
        if header_match:
            # Add options that were missing from the section we leave.
            if current_section in updates:
                flush_missing_options(current_section)
            current_section = header_match.group(1)
            sections_seen.add(current_section)
            result_lines.append(line)
            continue

        if current_section not in updates:
            result_lines.append(line)
            continue

        option_match = re.match(r"^(\S+)\s*=\s*", stripped)
        if (
            option_match is None
            or option_match.group(1) not in updates[current_section]
        ):
            result_lines.append(line)
            continue

        option_name = option_match.group(1)
        new_value, is_suffix = updates[current_section][option_name]
        result_lines.append(
            edit_option_line(line, new_value, is_suffix, comment_prefix)
        )
        options_applied[current_section].add(option_name)

    # Add options missing from the last section in the file.
    if current_section in updates:
        flush_missing_options(current_section)

    # Add sections that were not in the file at all.
    for section in updates:
        if section not in sections_seen:
            result_lines.append(f"\n[{section}]")
            result_lines.extend(missing_option_lines(section))

    return result_lines


def get_ini_sed_cmd(
    section: str, option: str, new_value: str, is_suffix: bool = False
) -> str:
    """
    Generates a cross-platform sed command to update the value of a
    key = value pair or append to one (by using is_suffix = True) in an INI file.
    """
    if is_suffix:
        pattern = f"s/(^{option}.*)/\\1{new_value}/"
    else:
        pattern = f"s/(^{option}[[:space:]]*=[[:space:]]*).*/\\1{new_value}/"
    return f"sed -E '/^\\[{section}\\]/,/^\\[/ {pattern}'"


def parse_memory(value: str) -> str:
    """
    Validate memory size string like '4G'.
    Returns the string unchanged if valid, raises argparse.ArgumentTypeError otherwise.
    """
    if not re.match(r"^\d+[G]$", value, re.IGNORECASE):
        raise argparse.ArgumentTypeError(
            f"Invalid memory size '{value}'. Use format like 4G, 32G."
        )
    return value.upper()


def container_memory_to_bytes(memory_string: str) -> int:
    """
    Parse a memory usage string from `docker stats` or `podman stats`
    into bytes.  Docker reports binary units (GiB, MiB) while Podman
    reports decimal units (GB, MB).
    """
    memory_string = memory_string.strip()
    # Order matters: the first endswith match wins, and "B" is a suffix
    # of every other unit, so it must stay last.
    units = {
        "TIB": 1024**4,
        "TB": 1000**4,
        "GIB": 1024**3,
        "GB": 1000**3,
        "MIB": 1024**2,
        "MB": 1000**2,
        "KIB": 1024,
        "KB": 1000,
        "B": 1,
    }
    for suffix, multiplier in units.items():
        if memory_string.upper().endswith(suffix):
            number = float(memory_string[: len(memory_string) - len(suffix)])
            return int(number * multiplier)
    return 0


def positive_int(value: str) -> int:
    """
    Parse a CLI value as an `int` and reject anything that is not a
    positive integer. Used as an argparse `type`.
    """
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError(
            f"Invalid value '{value}'. Must be a positive integer."
        )
    return number


def add_memory_options(subparser, index=True, server=True):
    """
    Add total memory-related options to a subparser for setup-config command.
    """
    if index:
        subparser.add_argument(
            "--total-index-memory",
            type=parse_memory,
            default="4G",
            help=(
                "Maximum memory budget for indexing. All relevant [index] "
                "options in the Qleverfile will be auto-generated with sensible "
                "defaults that together stay within this limit. "
            ),
        )

    if server:
        subparser.add_argument(
            "--total-server-memory",
            type=parse_memory,
            default="4G",
            help=(
                "Maximum memory budget for the server. All relevant [server] "
                "options in the Qleverfile will be auto-generated with sensible "
                "defaults that together stay within this limit. "
            ),
        )


def tail_log_file(
    log_file: Path,
    max_wait_seconds: int = 30,
) -> subprocess.Popen | None:
    """
    Wait for the log file to appear and start tailing it from the
    beginning. The old log file should be deleted before calling this
    function.

    Returns the tail process, or None if the log file was not created
    within `max_wait_seconds`.
    """
    waited = 0.0
    while not log_file.exists():
        if waited >= max_wait_seconds:
            log.error(
                f"Log file {log_file} was not created within "
                f"{max_wait_seconds} seconds"
            )
            return None
        time.sleep(0.1)
        waited += 0.1
    tail_cmd = f"exec tail -n +1 -f {log_file}"
    return subprocess.Popen(tail_cmd, shell=True)


def parse_git_hash(log_path: Path) -> str | None:
    """Return the git hash printed on the first line of a QLever index log."""
    try:
        first_line = log_path.read_text().splitlines()[0]
    except (OSError, IndexError):
        return None
    match = re.search(r"git hash ([0-9a-f]+)", first_line)
    return match.group(1) if match else None


class PhaseMarkers(NamedTuple):
    """
    Timestamp markers delimiting each phase of an index build. Any
    marker absent from the log is None. `permutations` holds one
    (begin_time, name) pair per permutation, in log order.
    """

    overall_begin: datetime | None
    merge_begin: datetime | None
    convert_begin: datetime | None
    permutations: list[tuple[datetime, str]]
    normal_end: datetime | None
    text_begin: datetime | None
    text_end: datetime | None


def parse_phase_markers(lines: list[str]) -> PhaseMarkers:
    """
    Scan index-build log lines and extract the timestamp markers that
    delimit each indexing phase. Handles both the old and new
    permutation log formats. Absent timestamps come back as None;
    callers decide how to report missing phases.
    """
    current_line = 0

    # Find the next line at or after `current_line` matching `regex` and
    # parse its leading timestamp. Returns (timestamp, match). When
    # nothing matches and `update_current_line` is False, the cursor is
    # rewound so a failed lookup does not consume the rest of the log.
    def find_next_line(regex: str, update_current_line: bool = True):
        nonlocal current_line
        current_line_backup = current_line
        timestamp_regex = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        timestamp_format = "%Y-%m-%d %H:%M:%S"
        while current_line < len(lines):
            line = lines[current_line]
            current_line += 1
            regex_match = re.search(regex, line)
            if regex_match:
                try:
                    return datetime.strptime(
                        re.match(timestamp_regex, line).group(),
                        timestamp_format,
                    ), regex_match
                except Exception as parse_error:
                    log.error(
                        f"Could not parse timestamp of form "
                        f'"{timestamp_regex}" from line '
                        f' "{line.rstrip()}" ({parse_error})'
                    )
        if not update_current_line:
            current_line = current_line_backup
        return None, None

    overall_begin, _ = find_next_line(r"INFO:\s*Processing")
    merge_begin, _ = find_next_line(r"INFO:\s*Merging partial vocab")
    convert_begin, _ = find_next_line(r"INFO:\s*Converting triples")

    # Collect permutations. The old log format announces a permutation
    # with "Creating a pair" and names it later in "Writing meta data
    # for ..."; the new format names it directly in "Creating
    # permutations ...".
    permutations = []
    while True:
        perm_begin, _ = find_next_line(
            r"INFO:\s*Creating a pair", update_current_line=False
        )
        if perm_begin is None:
            perm_begin, perm_info = find_next_line(
                r"INFO:\s*Creating permutations ([A-Z]+ and [A-Z]+)",
                update_current_line=False,
            )
        else:
            _, perm_info = find_next_line(
                r"INFO:\s*Writing meta data for ([A-Z]+ and [A-Z]+)",
                update_current_line=False,
            )
        if perm_info is None:
            break
        name = perm_info.group(1).replace(" and ", " & ")
        permutations.append((perm_begin, name))

    normal_end, _ = find_next_line(r"INFO:\s*Index build completed")
    text_begin, _ = find_next_line(
        r"INFO:\s*Adding text index", update_current_line=False
    )
    text_end, _ = find_next_line(
        r"INFO:\s*Text index build comp", update_current_line=False
    )

    return PhaseMarkers(
        overall_begin=overall_begin,
        merge_begin=merge_begin,
        convert_begin=convert_begin,
        permutations=permutations,
        normal_end=normal_end,
        text_begin=text_begin,
        text_end=text_end,
    )


def iter_permutation_phases(
    permutations: list[tuple[datetime, str]],
    normal_end: datetime | None,
) -> Iterator[tuple[str, datetime, datetime | None]]:
    """
    Yield (name, begin, end) for each permutation phase. A permutation
    ends where the next one begins, or at `normal_end` for the last.
    Repeated names are disambiguated with a numeric suffix.
    """
    seen = set()
    for index, (begin, name) in enumerate(permutations):
        end = (
            permutations[index + 1][0]
            if index + 1 < len(permutations)
            else normal_end
        )
        if name in seen:
            suffix = 2
            while f"{name} ({suffix})" in seen:
                suffix += 1
            name = f"{name} ({suffix})"
        seen.add(name)
        yield name, begin, end
