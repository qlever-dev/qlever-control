import shlex

import pytest

from qeval.base_commands.query import BaseQueryCommand

SERVER_ARGS = ["--host-name", "localhost", "--port", "7878"]


# No test may send a real request, so the command that runs `curl` is
# replaced in every test.
@pytest.fixture(autouse=True)
def run_command(mock_command):
    return mock_command("qlever.commands.query", "run_command")


def test_base_query_sends_query_to_engine_endpoint(parse_args, run_command):
    command = BaseQueryCommand()
    args = parse_args(command, SERVER_ARGS)

    assert command.execute(args)

    curl_cmd = run_command.call_args.args[0]
    assert "localhost:7878/sparql" in shlex.split(curl_cmd)


def test_base_query_rejects_pin_to_cache(parse_args):
    with pytest.raises(SystemExit):
        parse_args(BaseQueryCommand(), ["--pin-to-cache", *SERVER_ARGS])


@pytest.mark.parametrize(
    "accept", ["application/qlever-results+json", "application/octet-stream"]
)
def test_base_query_rejects_qlever_only_accept(parse_args, accept):
    with pytest.raises(SystemExit):
        parse_args(BaseQueryCommand(), ["--accept", accept, *SERVER_ARGS])
