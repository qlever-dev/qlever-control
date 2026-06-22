from __future__ import annotations


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define additional jena specific Qleverfile parameters"""

    def arg(*args, **kwargs):
        return (args, kwargs)

    index_args = all_args["index"]
    server_args = all_args["server"]

    index_args["index_binary"] = arg(
        "--index-binary",
        type=str,
        default="tdb2.xloader",
        help=(
            "The binary for building the index (default: tdb2.xloader) "
            "(this requires that you have apache-jena installed "
            "on your machine)"
        ),
    )
    index_args["threads"] = arg(
        "--threads",
        type=int,
        default=2,
        help=(
            "Set the number of threads to use with sort(1). "
            "The recommendation for an initial setting is to set it to the "
            "number of cores (not hardware threads) minus 1. This is sensitive "
            "to the hardware environment."
        ),
    )
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
    index_args["extra_env_args"] = arg(
        "--extra-env-args",
        type=str,
        default="",
        help=(
            "Additional environment variable arguments to pass directly to "
            "the tdb2.xloader binary as a string of key=value pairs. This allows "
            "advanced users to specify options not exposed in Qleverfile. "
            "The string is appended verbatim to the bash env command."
        ),
    )
    index_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the tdb2.xloader binary. "
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
    server_args["server_binary"] = arg(
        "--server-binary",
        type=str,
        default="fuseki-server",
        help=(
            "The binary for starting the server (default: fuseki-server) "
            "(this requires that you have apache-jena-fuseki installed "
            "on your machine)"
        ),
    )
    server_args["timeout"] = arg(
        "--timeout",
        type=str,
        default="60s",
        help="The maximal time in seconds a query is allowed to run",
    )
    server_args["extra_env_args"] = arg(
        "--extra-env-args",
        type=str,
        default="",
        help=(
            "Additional environment variable arguments to pass directly to "
            "the fuseki-server binary as a string of key=value pairs. This allows "
            "advanced users to specify options not exposed in Qleverfile. "
            "The string is appended verbatim to the bash env command."
        ),
    )
    server_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the fuseki-server binary. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )
