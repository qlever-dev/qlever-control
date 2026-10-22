from unittest.mock import MagicMock

import psutil
import pytest

from qeval.resource_monitor import (
    Sample,
    container_memory_to_bytes,
    sample_container,
    sample_process,
    sample_to_tsv_row,
)

MODULE = "qeval.resource_monitor"


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
    "sample,expected",
    [
        (Sample(elapsed_s=1.0, rss=100, cpu_percent=5.0), "1.0\t100\t5.0\n"),
        (Sample(), "\t\t\n"),
        (Sample(elapsed_s=2.0), "2.0\t\t\n"),
        # Zero is a real reading, not a missing one: it renders as "0"
        # / "0.0", never as an empty column.
        (Sample(elapsed_s=0.0, rss=0, cpu_percent=0.0), "0.0\t0\t0.0\n"),
    ],
)
def test_sample_to_tsv_row(sample, expected):
    assert sample_to_tsv_row(sample) == expected


def test_sample_container_parses_stats_output(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "1.5GiB / 7.6GiB\t12.5%"
    sample = sample_container("docker", "qlever.index.test")
    assert sample.rss == int(1.5 * 1024**3)
    assert sample.cpu_percent == 12.5


def test_sample_container_returns_empty_on_malformed_output(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "garbage"
    sample = sample_container("docker", "qlever.index.test")
    assert sample == Sample()


def test_sample_process_reads_rss_and_cpu():
    proc = MagicMock()
    proc.memory_info.return_value.rss = 2048
    proc.cpu_percent.return_value = 7.5
    sample = sample_process(proc)
    assert sample.rss == 2048
    assert sample.cpu_percent == 7.5


def test_sample_process_returns_empty_when_process_gone():
    proc = MagicMock()
    proc.memory_info.side_effect = psutil.NoSuchProcess(pid=123)
    sample = sample_process(proc)
    assert sample == Sample()
