from __future__ import annotations


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define additional mdb specific Qleverfile parameters"""

    def arg(*args, **kwargs):
        return (args, kwargs)

    index_args = all_args["index"]
    server_args = all_args["server"]

    index_args["index_binary"] = arg(
        "--index-binary",
        type=str,
        default="isql",
        help=(
            "The isql binary for building the index (default: isql) "
            "(this requires that you have virtuoso binaries installed "
            "on your machine)"
        ),
    )
    index_args["isql_port"] = arg(
        "--isql-port",
        type=int,
        default=1111,
        help="The port used by Virtuoso's ISQL index binary",
    )
    index_args["num_parallel_loaders"] = arg(
        "--num-parallel-loaders",
        type=int,
        default=1,
        choices=range(1, 11),
        help=(
            "It is recommended a maximum of no cores / 2.5, to optimally "
            "parallelize the data load and hence maximize load speed."
        ),
    )
    index_args["free_memory_gb"] = arg(
        "--free-memory-gb",
        type=str,
        default="4G",
        help=(
            "Amount of free system memory to allocate for Virtuoso buffers. "
            "Virtuoso will use between 2/3 - 3/5 of system memory and set "
            "NumberOfBuffers and MaxDirtyBuffers accordingly."
        ),
    )

    server_args["server_binary"] = arg(
        "--server-binary",
        type=str,
        default="virtuoso-t",
        help=(
            "The binary for starting the server (default: virtuoso-t) "
            "(this requires that you have virtuoso binaries installed "
            "on your machine)"
        ),
    )
    server_args["max_query_memory"] = arg(
        "--max-query-memory",
        type=str,
        default="2G",
        help="The memory allocated to query processor.",
    )
    server_args["timeout"] = arg(
        "--timeout",
        type=str,
        default="30s",
        help="The maximal time in seconds a query is allowed to run",
    )
    server_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default="",
        help=(
            "Additional arguments to pass directly to the virtuoso-t binary. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )
