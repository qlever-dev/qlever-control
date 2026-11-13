import pytest

from qeval.oxigraph.commands.index import IndexCommand
from qeval.oxigraph.commands.index_stats import IndexStatsCommand


# Every test runs in an empty folder with a small dataset, and nothing that
# `index` would start really runs, so that `index` only decides what to do.
@pytest.fixture(autouse=True)
def dataset_folder(mock_command, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.nt").write_text("<a> <b> <c> .\n")
    module = "qeval.oxigraph.commands.index"
    mock_command(module, "binary_exists").return_value = True
    mock_command(module, "ResourceMonitor")
    mock_command(module, "render_usage_plot_if_installed")


@pytest.fixture(autouse=True)
def run_command(mock_command):
    return mock_command("qeval.oxigraph.commands.index", "run_command")


@pytest.fixture
def run_index(parse_args):
    """Run `index` with the real parser and return its result."""

    def run(*argv):
        command = IndexCommand()
        args = parse_args(
            command,
            [
                "--name",
                "olympics",
                "--input-files",
                "olympics.nt",
                "--system",
                "native",
                *argv,
            ],
            engine="oxigraph",
        )
        return command.execute(args)

    return run


def shown_phases():
    """The phases that `index-stats` shows for the index log."""
    durations = IndexStatsCommand().index_durations("olympics.index-log.txt")
    return list(durations)


def test_oxigraph_index_loads_and_optimizes(run_command, run_index):
    assert run_index()

    assert run_command.call_count == 2
    assert shown_phases() == ["Load time", "Optimize time", "TOTAL time"]


def test_oxigraph_index_skips_optimize_when_not_read_only(
    run_command, run_index
):
    assert run_index("--read-only", "no")

    assert run_command.call_count == 1
    assert shown_phases() == ["TOTAL time"]


def test_oxigraph_index_succeeds_when_optimize_fails(run_command, run_index):
    run_command.side_effect = [None, Exception("optimize failed")]

    assert run_index()

    assert shown_phases() == ["TOTAL time"]


def test_oxigraph_index_fails_when_load_fails(run_command, run_index):
    run_command.side_effect = Exception("load failed")

    assert not run_index()

    assert shown_phases() == []


def test_oxigraph_index_refuses_to_overwrite_index(
    run_command, run_index, tmp_path
):
    (tmp_path / "olympics_index").mkdir()
    (tmp_path / "olympics_index" / "000001.sst").write_bytes(b"x")

    assert not run_index()

    run_command.assert_not_called()


def test_oxigraph_index_waits_for_running_index_container(
    run_command, run_index, mock_command
):
    mock_command(
        "qlever.containerize.Containerize", "is_running"
    ).return_value = True

    assert not run_index("--system", "docker")

    run_command.assert_not_called()
