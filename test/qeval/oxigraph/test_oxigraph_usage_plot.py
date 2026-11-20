import pytest

from qeval.oxigraph.commands.index import IndexCommand
from qeval.oxigraph.usage_plot import subtitle
from qlever.resource_usage.plot_util import SUBTITLE_SEPARATOR


@pytest.mark.parametrize(
    "version_output,expected",
    [
        (
            "oxigraph 0.4.9\n",
            f"oxigraph v0.4.9{SUBTITLE_SEPARATOR}read-only = yes",
        ),
        ("oxigraph dev build\n", "read-only = yes"),
        (Exception("oxigraph not found"), "read-only = yes"),
    ],
    ids=["version", "no version number", "version command fails"],
)
def test_oxigraph_usage_plot_subtitle(
    parse_args, mock_command, tmp_path, version_output, expected
):
    run_command = mock_command("qeval.oxigraph.usage_plot", "run_command")
    run_command.side_effect = [version_output]
    # `index` draws the plot, so the subtitle gets the arguments of `index`.
    args = parse_args(
        IndexCommand(),
        [
            "--name",
            "olympics",
            "--input-files",
            "olympics.nt",
            "--system",
            "native",
        ],
        engine="oxigraph",
    )

    assert subtitle(args, tmp_path / "olympics.index-log.txt") == expected
