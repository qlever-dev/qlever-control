from argparse import Namespace

from qlever.commands.extract_queries import ExtractQueriesCommand


def test_execute_with_missing_log_file_returns_false(tmp_path):
    """
    A missing --log-file must fail cleanly (return False) instead of raising
    FileNotFoundError with a traceback.
    """
    command = ExtractQueriesCommand()
    output_file = tmp_path / "log-queries.txt"
    args = Namespace(
        log_file=str(tmp_path / "no-such-log.txt"),
        output_file=str(output_file),
        description_base="Log extract",
        use_alive_check_tag_as_description_base=False,
        show=False,
        name="test",
    )
    assert command.execute(args) is False
    assert not output_file.exists()
