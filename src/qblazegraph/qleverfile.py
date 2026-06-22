from __future__ import annotations


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define additional blazegraph specific Qleverfile parameters"""

    def arg(*args, **kwargs):
        return (args, kwargs)

    index_args = all_args["index"]
    server_args = all_args["server"]

    index_args["jvm_args"] = arg(
        "--jvm_args",
        type=str,
        default="-Xmx4G",
        help=(
            "Arguments for the JVM. "
            "Do not set to all available RAM. "
            "Increasing is only necessary for large numbers of long literals."
        ),
    )
    index_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the Blazegraph BulkLoader. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )

    server_args["jvm_args"] = arg(
        "--jvm_args",
        type=str,
        default="-Xmx4G",
        help=("Arguments for the JVM."),
    )
    server_args["timeout"] = arg(
        "--timeout",
        type=str,
        default="60s",
        help="The maximal time in seconds a query is allowed to run",
    )
    server_args["read_only"] = arg(
        "--read-only",
        type=str,
        choices=["yes", "no"],
        default="yes",
        help="The REST API will not permit mutation operations in read-only mode.",
    )
    server_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional -D props to pass directly to the java -jar blazegraph.jar. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )
