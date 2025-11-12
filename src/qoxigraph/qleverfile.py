from __future__ import annotations


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define additional oxigraph specific Qleverfile parameters"""

    def arg(*args, **kwargs):
        return (args, kwargs)

    index_args = all_args["index"]
    server_args = all_args["server"]

    index_args["index_binary"] = arg(
        "--index-binary",
        type=str,
        default="oxigraph",
        help=(
            "The binary for building the index (default: oxigraph) "
            "(this requires that you have oxigraph-cli installed "
            "on your machine)"
        ),
    )
    index_args["lenient"] = arg(
        "--lenient",
        action="store_true",
        default=False,
        help="Attempt to keep loading even if the data file is invalid",
    )
    index_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the oxigraph load process. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )

    server_args["server_binary"] = arg(
        "--server-binary",
        type=str,
        default="oxigraph",
        help=(
            "The binary for starting the server (default: oxigraph) "
            "(this requires that you have oxigraph-cli installed "
            "on your machine)"
        ),
    )
    server_args["read_only"] = arg(
        "--read-only",
        type=str,
        choices=["yes", "no"],
        default="yes",
        help=(
            "The HTTP server will not permit mutation operations in "
            "read-only mode"
        ),
    )
    server_args["timeout"] = arg(
        "--timeout",
        type=str,
        default="60s",
        help="The maximal time in seconds a query is allowed to run",
    )
    server_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the oxigraph "
            "serve/serve-read-only. This allows advanced users to specify "
            "options not exposed in Qleverfile. The string is appended "
            "verbatim to the command."
        ),
    )
