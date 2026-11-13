from types import SimpleNamespace

import pytest

from qeval.oxigraph.commands.start import StartCommand

MODULE = "qeval.oxigraph.commands.start"


# Every test runs in an empty folder that holds an Oxigraph index.
@pytest.fixture(autouse=True)
def index_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics_index").mkdir()
    (tmp_path / "olympics_index" / "000001.sst").write_bytes(b"x")


# Nothing that `start` would start or check really runs, so that `start`
# only decides what to do. By default, no server answers before the start,
# the started server becomes ready, and Ctrl-C is pressed at once when the
# server runs in the foreground.
@pytest.fixture(autouse=True)
def mocks(mock_command):
    run_command = mock_command(MODULE, "run_command")
    # The PID of the server that `nohup` started.
    run_command.return_value = "4711\n"
    mock_command(MODULE, "binary_exists").return_value = True
    is_server_alive = mock_command(MODULE, "is_server_alive")
    is_server_alive.return_value = False
    wait_until_server_ready = mock_command(MODULE, "wait_until_server_ready")
    wait_until_server_ready.return_value = True
    mock_command(
        MODULE,
        "wait_for_foreground_server",
        lambda process, log_proc, on_interrupt: on_interrupt(),
    )
    # In a container, the log is followed with `docker logs -f` after a wait.
    mock_command(f"{MODULE}.subprocess", "Popen")
    mock_command(f"{MODULE}.time", "sleep")
    return SimpleNamespace(
        run_command=run_command,
        is_server_alive=is_server_alive,
        wait_until_server_ready=wait_until_server_ready,
        tail_log_file=mock_command(MODULE, "tail_log_file"),
        stop_tailing=mock_command(MODULE, "stop_tailing"),
        stop_container=mock_command(MODULE, "stop_container"),
    )


@pytest.fixture
def run_start(parse_args):
    """Run `start` with the real parser and return its result."""

    def run(*argv):
        command = StartCommand()
        args = parse_args(
            command,
            [
                "--name",
                "olympics",
                "--host-name",
                "localhost",
                "--port",
                "7878",
                "--server-container",
                "olympics.oxigraph",
                "--system",
                "native",
                *argv,
            ],
            engine="oxigraph",
        )
        return command.execute(args)

    return run


def test_oxigraph_start_starts_server(mocks, run_start):
    assert run_start()

    mocks.run_command.assert_called_once()
    mocks.stop_tailing.assert_called_once_with(
        mocks.tail_log_file.return_value
    )


def test_oxigraph_start_needs_index(mocks, run_start, tmp_path):
    (tmp_path / "olympics_index" / "000001.sst").unlink()

    assert not run_start()

    mocks.run_command.assert_not_called()


def test_oxigraph_start_refuses_when_server_answers(mocks, run_start):
    mocks.is_server_alive.return_value = True

    assert not run_start()

    mocks.run_command.assert_not_called()


def test_oxigraph_start_stops_following_log_when_server_not_ready(
    mocks, run_start
):
    mocks.wait_until_server_ready.return_value = False

    assert not run_start()

    mocks.stop_tailing.assert_called_once_with(
        mocks.tail_log_file.return_value
    )


@pytest.mark.parametrize(
    "system,container_found,removes_container,warns",
    [
        ("docker", True, True, False),
        ("docker", False, True, True),
        # A native server is already stopped by Ctrl-C itself.
        ("native", False, False, False),
    ],
    ids=["container found", "container gone", "native"],
)
def test_oxigraph_start_cleans_up_after_ctrl_c(
    mocks, run_start, caplog, system, container_found, removes_container, warns
):
    mocks.stop_container.return_value = container_found

    assert run_start("--run-in-foreground", "--system", system)

    assert mocks.stop_container.called == removes_container
    if removes_container:
        mocks.stop_container.assert_called_once_with("olympics.oxigraph")
    warned = any(
        record.levelname == "WARNING" and "olympics.oxigraph" in record.message
        for record in caplog.records
    )
    assert warned == warns
