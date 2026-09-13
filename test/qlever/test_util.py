import argparse

import pytest

from qlever.util import (
    container_memory_to_bytes,
    edit_option_line,
    get_random_string,
    parse_git_hash,
    positive_int,
    stop_systemd_unit,
    stop_tailing,
    systemd_linger_status,
    systemd_unit_is_active,
    systemd_unit_is_loaded,
    systemd_unit_name,
    systemd_unit_of_process,
    systemd_user_env,
    tail_log_file,
    update_ini_values,
)


def test_get_random_string():
    random_string_1 = get_random_string(20)
    random_string_2 = get_random_string(20)
    assert len(random_string_1) == 20
    assert len(random_string_2) == 20
    assert random_string_1 != random_string_2


@pytest.mark.parametrize(
    "usage,expected",
    [
        ("2TiB", 2 * 1024**4),
        ("1.5GiB", int(1.5 * 1024**3)),
        ("512MiB", 512 * 1024**2),
        ("4KiB", 4 * 1024),
        ("2TB", 2 * 1000**4),
        ("1.5GB", int(1.5 * 1000**3)),
        ("512MB", 512 * 1000**2),
        ("4KB", 4 * 1000),
        ("100B", 100),
        ("0B", 0),
        # Longest matching suffix wins; "GiB"/"GB" must not be read as
        # bare bytes via the trailing "B".
        ("2GiB", 2 * 1024**3),
        ("2GB", 2 * 1000**3),
        # Leading/trailing whitespace and case are tolerated.
        ("  1.5gib ", int(1.5 * 1024**3)),
        # A space between number and unit is accepted by float().
        ("1.5 GiB", int(1.5 * 1024**3)),
        ("", 0),
        ("garbage", 0),
    ],
)
def test_container_memory_to_bytes(usage, expected):
    assert container_memory_to_bytes(usage) == expected


@pytest.mark.parametrize(
    "first_line,expected",
    [
        ("qlever-server, git hash 1a2b3c4, compiled", "1a2b3c4"),
        ("no hash on this line", None),
    ],
)
def test_parse_git_hash_reads_first_line_only(first_line, expected, tmp_path):
    path = tmp_path / "index-log.txt"
    # Second line also carries a hash; only the first line should count.
    path.write_text(first_line + "\nsomething git hash deadbeef here\n")
    assert parse_git_hash(path) == expected


@pytest.mark.parametrize("value,expected", [("1", 1), ("500", 500)])
def test_positive_int_accepts(value, expected):
    assert positive_int(value) == expected


@pytest.mark.parametrize("value", ["0", "-3"])
def test_positive_int_rejects_non_positive(value):
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(value)


@pytest.mark.parametrize("value", ["1.5", "abc"])
def test_positive_int_rejects_non_integer(value):
    # argparse also treats a plain `ValueError` as invalid input
    with pytest.raises(ValueError):
        positive_int(value)


def test_parse_git_hash_missing_file(tmp_path):
    assert parse_git_hash(tmp_path / "nope.txt") is None


def test_parse_git_hash_empty_file(tmp_path):
    path = tmp_path / "empty.txt"
    path.write_text("")
    assert parse_git_hash(path) is None


def test_edit_option_line_replaces_value_and_keeps_alignment():
    line = "PORT               = 7019"
    assert (
        edit_option_line(line, "9999", False, "#")
        == "PORT               = 9999"
    )


def test_edit_option_line_appends_suffix():
    line = "ACCESS_TOKEN       = ${data:NAME}"
    assert (
        edit_option_line(line, "abc", True, "#")
        == "ACCESS_TOKEN       = ${data:NAME}abc"
    )


def test_edit_option_line_keeps_inline_comment():
    line = "PORT               = 7019   # the port"
    assert (
        edit_option_line(line, "9999", False, "#")
        == "PORT               = 9999\t# the port"
    )
    assert (
        edit_option_line(line, "0", True, "#")
        == "PORT               = 70190\t# the port"
    )


def test_edit_option_line_ignores_comment_char_inside_value():
    # No whitespace before the `#`, so it is part of the value.
    line = "ENCODE_AS_ID = https://example.org/geom#osmnode_"
    assert (
        edit_option_line(line, "X", True, "#")
        == "ENCODE_AS_ID = https://example.org/geom#osmnode_X"
    )


def test_edit_option_line_without_comment_prefix():
    # Without a prefix the comment is part of the value, so a suffix
    # lands after it.
    line = "PORT = 7019 # the port"
    assert edit_option_line(line, "0", True, None) == "PORT = 7019 # the port0"


def test_update_ini_values_replaces_and_appends():
    lines = [
        "[server]",
        "PORT               = 7019",
        "ACCESS_TOKEN       = ${data:NAME}",
    ]
    updates = {
        "server": {
            "PORT": ("9999", False),
            "ACCESS_TOKEN": ("abc", True),
        }
    }
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT               = 9999",
        "ACCESS_TOKEN       = ${data:NAME}abc",
    ]


def test_update_ini_values_passes_comment_prefix_on():
    lines = ["[server]", "PORT = 7019  # the port"]
    updates = {"server": {"PORT": ("9999", False)}}
    assert update_ini_values(lines, updates, inline_comment_prefix="#") == [
        "[server]",
        "PORT = 9999\t# the port",
    ]


def test_update_ini_values_adds_option_missing_from_section():
    lines = ["[server]", "PORT = 7019", "", "[runtime]", "SYSTEM = docker"]
    updates = {"server": {"TIMEOUT": ("30s", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT = 7019",
        "TIMEOUT = 30s",
        "",
        "[runtime]",
        "SYSTEM = docker",
    ]


def test_update_ini_values_adds_option_to_last_section():
    lines = ["[server]", "PORT = 7019"]
    updates = {"server": {"TIMEOUT": ("30s", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT = 7019",
        "TIMEOUT = 30s",
    ]


def test_update_ini_values_adds_missing_section():
    lines = ["[server]", "PORT = 7019"]
    updates = {"runtime": {"SYSTEM": ("native", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT = 7019",
        "",
        "[runtime]",
        "SYSTEM = native",
    ]


def test_update_ini_values_aligns_added_option_with_section():
    lines = ["[server]", "PORT               = 7019"]
    updates = {"server": {"TIMEOUT": ("30s", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT               = 7019",
        "TIMEOUT            = 30s",
    ]


def test_update_ini_values_skips_suffix_in_missing_section():
    # A suffix entry has no value to append to, so it is not added.
    lines = ["[server]", "PORT = 7019"]
    updates = {
        "runtime": {
            "SYSTEM": ("native", False),
            "EXTRA": ("abc", True),
        }
    }
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT = 7019",
        "",
        "[runtime]",
        "SYSTEM = native",
    ]


def test_update_ini_values_ignores_commented_out_lines():
    lines = ["[server]", ";PORT = 1111", "PORT = 7019"]
    updates = {"server": {"PORT": ("9999", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        ";PORT = 1111",
        "PORT = 9999",
    ]


def test_update_ini_values_ignores_commented_out_section():
    # `;[server]` is not a section, so `PORT` is not inside one and the
    # section is added at the end instead.
    lines = [";[server]", "PORT = 7019"]
    updates = {"server": {"PORT": ("9999", False)}}
    assert update_ini_values(lines, updates) == [
        ";[server]",
        "PORT = 7019",
        "",
        "[server]",
        "PORT = 9999",
    ]


def test_update_ini_values_leaves_other_sections_alone():
    lines = ["[server]", "PORT = 7019", "[index]", "PORT = 1234"]
    updates = {"server": {"PORT": ("9999", False)}}
    assert update_ini_values(lines, updates) == [
        "[server]",
        "PORT = 9999",
        "[index]",
        "PORT = 1234",
    ]


def test_update_ini_values_keeps_unrelated_lines():
    lines = ["# a comment", "", "[server]", "PORT = 7019", "HOST = localhost"]
    updates = {"server": {"PORT": ("9999", False)}}
    assert update_ini_values(lines, updates) == [
        "# a comment",
        "",
        "[server]",
        "PORT = 9999",
        "HOST = localhost",
    ]


# The systemd helpers ask `systemctl --user` and `loginctl` (with the
# environment that points to the user's systemd instance, see
# `systemd_user_env`).
def test_systemd_helpers(monkeypatch):
    import subprocess
    from unittest.mock import MagicMock

    assert systemd_unit_name("olympics") == "qlever.server.olympics"

    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("DBUS_SESSION_BUS_ADDRESS", raising=False)
    env = systemd_user_env()
    assert env["XDG_RUNTIME_DIR"].startswith("/run/user/")
    assert env["DBUS_SESSION_BUS_ADDRESS"] == (
        f"unix:path={env['XDG_RUNTIME_DIR']}/bus"
    )

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        assert "XDG_RUNTIME_DIR" in kwargs["env"]
        result = MagicMock()
        if cmd[0] == "loginctl":
            result.stdout = "yes\n"
        elif "show" in cmd:
            result.stdout = "loaded\n" if fake_run.loaded else "not-found\n"
        result.returncode = 0 if fake_run.ok else 3
        if "stop" in cmd and fake_run.stop_fails:
            result.returncode = 1
            result.stderr = "Failed to stop\n"
        return result

    fake_run.loaded = True
    fake_run.ok = True
    fake_run.stop_fails = False
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        "qlever.util.shutil.which", lambda _: "/usr/bin/systemctl"
    )

    assert systemd_linger_status() == "yes"
    assert systemd_unit_is_loaded("qlever.server.olympics")
    assert systemd_unit_is_active("qlever.server.olympics")
    assert stop_systemd_unit("qlever.server.olympics")
    assert calls[-2][:4] == [
        "systemctl",
        "--user",
        "stop",
        "qlever.server.olympics",
    ]
    assert calls[-1][:3] == ["systemctl", "--user", "reset-failed"]

    # A `stop` that fails is reported as such, and the unit is not forgotten.
    fake_run.stop_fails = True
    assert not stop_systemd_unit("qlever.server.olympics")
    assert calls[-1][:3] == ["systemctl", "--user", "stop"]
    fake_run.stop_fails = False

    fake_run.loaded = False
    fake_run.ok = False
    assert systemd_linger_status() is None
    assert not systemd_unit_is_active("qlever.server.olympics")
    assert not stop_systemd_unit("qlever.server.olympics")

    # Without `systemctl` and `loginctl`, systemd is not usable at all.
    fake_run.loaded = True
    fake_run.ok = True
    monkeypatch.setattr("qlever.util.shutil.which", lambda _: None)
    assert not systemd_unit_is_loaded("qlever.server.olympics")
    assert not systemd_unit_is_active("qlever.server.olympics")
    assert systemd_linger_status() is None


# The unit of a process is read from its control group; a process outside of
# such a unit (like the test itself) or a nonexistent process has none.
def test_systemd_unit_of_process(monkeypatch):
    import os

    assert systemd_unit_of_process(os.getpid()) is None
    assert systemd_unit_of_process(2**31 - 1) is None

    class FakeCgroupFile:
        def __init__(self, path):
            assert path == "/proc/4711/cgroup"

        def read_text(self):
            return (
                "0::/user.slice/user-8288.slice/user@8288.service/app.slice"
                "/qlever.server.olympics.service\n"
            )

    monkeypatch.setattr("qlever.util.Path", FakeCgroupFile)
    assert systemd_unit_of_process(4711) == "qlever.server.olympics"


# With `stop_after`, the tail of a log file ends by itself after the matching
# line, at the latest with the next line (which the tail cannot write to the
# finished filter any more); without it, the tail runs until it is stopped.
# The pattern is a regular expression and may contain a slash.
def test_tail_log_file_stop_after(tmp_path):
    import time

    log_file = tmp_path / "server-log.txt"
    log_file.write_text("Loading index\n")
    tail_proc = tail_log_file(log_file, stop_after="ready.*port [0-9]+/x")
    assert tail_proc is not None
    time.sleep(0.5)
    with log_file.open("a") as f:
        f.write("The server is ready, port 7/x\n")
    time.sleep(0.5)
    with log_file.open("a") as f:
        f.write("query\n")
    assert tail_proc.wait(timeout=10) == 0

    tail_proc = tail_log_file(log_file)
    assert tail_proc is not None
    assert tail_proc.poll() is None
    stop_tailing(tail_proc)
    assert tail_proc.wait(timeout=10) != 0
