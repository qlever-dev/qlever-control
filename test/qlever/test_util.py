import argparse

import pytest

from qlever.util import (
    container_memory_to_bytes,
    get_random_string,
    parse_git_hash,
    positive_int,
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
