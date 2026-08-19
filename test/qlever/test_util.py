import argparse

import pytest

from qlever.util import (
    container_memory_to_bytes,
    edit_option_line,
    get_random_string,
    parse_git_hash,
    parse_timeout,
    positive_int,
    update_ini_values,
    timeout_seconds,
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
        "\n[runtime]",
        "SYSTEM = native",
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
        "\n[runtime]",
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
        "\n[server]",
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
@pytest.mark.parametrize("value", ["5ns", "5us", "500ms", "30s", "5min", "2h"])
def test_parse_timeout_accepts_every_unit(value):
    assert parse_timeout(value) == value


@pytest.mark.parametrize(
    "value",
    [
        # No unit at all.
        "30",
        # Unit the server does not accept.
        "30sec",
        # Wrong case, the server is case sensitive.
        "30S",
        "5MIN",
        # Not a number.
        "abc",
        "",
        # Fractional values are not supported.
        "1.5s",
        # Whitespace between number and unit.
        "30 s",
    ],
)
def test_parse_timeout_rejects(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timeout(value)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("30s", 30),
        ("5min", 300),
        ("2h", 7200),
        # Rounded to whole seconds.
        ("1500ms", 2),
        # Sub-second timeouts stay a timeout instead of becoming 0.
        ("1ms", 1),
        ("1ns", 1),
    ],
)
def test_timeout_seconds(value, expected):
    assert timeout_seconds(value) == expected
