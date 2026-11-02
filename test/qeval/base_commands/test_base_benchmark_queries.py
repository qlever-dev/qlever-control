from pathlib import Path

import yaml

from qeval.base_commands.benchmark_queries import BaseBenchmarkQueriesCommand

QUERIES_YML = """\
name: Fake benchmark
description: Two small queries
queries:
  - name: q1
    description: All triples
    query: SELECT * WHERE { ?s ?p ?o }
  - name: q2
    description: All subjects
    query: SELECT ?s WHERE { ?s ?p ?o }
"""


class FakeBenchmarkQueriesCommand(BaseBenchmarkQueriesCommand):
    def __init__(self, index_stats=None):
        super().__init__()
        self.index_stats = index_stats

    def index_stats_command(self):
        return self.index_stats


def test_base_benchmark_queries_ignores_clear_cache(parse_args):
    command = FakeBenchmarkQueriesCommand()
    args = parse_args(
        command,
        [
            "--queries-tsv",
            "queries.tsv",
            "--accept",
            "text/tab-separated-values",
            "--clear-cache",
            "yes",
            "--show",
        ],
    )

    assert command.execute(args)

    assert args.clear_cache == "no"


def test_base_benchmark_queries_writes_result_file(
    parse_args, fake_index_stats, mock_command, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "queries.yml").write_text(QUERIES_YML)
    (tmp_path / "olympics.index-log.txt").write_text("TOTAL time 90\n")
    (tmp_path / "olympics.fake-index.data").write_bytes(b"x" * 1000)

    def write_result(url, headers, params, result_file, max_time):
        Path(result_file).write_text("?s\n<a>\n<b>\n")
        return "200"

    curl = mock_command(
        "qlever.commands.benchmark_queries", "run_curl_command"
    )
    # Each query gets a result with two rows.
    curl.side_effect = write_result
    # Pretty-printing runs a container, without it the query is kept as is.
    mock_command(
        "qlever.commands.benchmark_queries", "pretty_printed_query"
    ).return_value = None

    command = FakeBenchmarkQueriesCommand(fake_index_stats)
    args = parse_args(
        command,
        [
            "--queries-yml",
            "queries.yml",
            "--accept",
            "text/tab-separated-values",
            "--host-name",
            "localhost",
            "--port",
            "7878",
            "--result-file",
            "olympics.fake",
        ],
    )

    assert command.execute(args)

    assert [call.args[0] for call in curl.call_args_list] == [
        "localhost:7878/sparql",
        "localhost:7878/sparql",
    ]
    results = yaml.safe_load(
        (tmp_path / "olympics.fake.results.yaml").read_text()
    )
    assert results["name"] == "Fake benchmark"
    assert results["description"] == "Two small queries"
    assert results["index_time"] == 90.0
    assert results["index_size"] == 1000
    assert [
        (query["name"], query["description"], query["result_size"])
        for query in results["queries"]
    ] == [("q1", "All triples", 2), ("q2", "All subjects", 2)]
