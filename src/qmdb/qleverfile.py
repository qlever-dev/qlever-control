from __future__ import annotations


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define additional mdb specific Qleverfile parameters"""

    def arg(*args, **kwargs):
        return (args, kwargs)

    data_args = all_args["data"]
    index_args = all_args["index"]
    server_args = all_args["server"]

    data_args["format"] = arg(
        "--format",
        type=str,
        default="ttl",
        choices=["ttl", "nt", "n3", "rdf", "gql", "qm"],
        help="Specify the file format",
    )

    index_args["index_binary"] = arg(
        "--index-binary",
        type=str,
        default="mdb",
        help=(
            "The binary for building the index (default: mdb) "
            "(this requires that you have MillenniumDB built from source "
            "on your machine)"
        ),
    )
    index_args["buffer_strings"] = arg(
        "--buffer-strings",
        type=str,
        default=None,
        help="Size of buffer for strings used during import",
    )
    index_args["buffer_tensors"] = arg(
        "--buffer-tensors",
        type=str,
        default=None,
        help="Size of buffer for tensors used during import",
    )
    index_args["prefixes"] = arg(
        "--prefixes",
        type=str,
        default=None,
        help="Prefixes file path (for IRI compression)",
    )
    index_args["btree_permutations"] = arg(
        "--btree-permutations",
        type=int,
        choices=[3, 4, 6],
        default=4,
        help="Btree permutations -> 3, 4 or 6",
    )
    index_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default=None,
        help=(
            "Additional arguments to pass directly to the mdb import process. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )

    server_args["server_binary"] = arg(
        "--server-binary",
        type=str,
        default="mdb",
        help=(
            "The binary for starting the server (default: mdb) "
            "(this requires that you have MillenniumDB built from source "
            "on your machine)"
        ),
    )
    server_args["timeout"] = arg(
        "--timeout",
        type=str,
        default="60s",
        help="The maximal time in seconds a query is allowed to run",
    )
    server_args["threads"] = arg(
        "--threads",
        type=int,
        default=None,
        help="Number of worker threads",
    )
    server_args["strings_dynamic"] = arg(
        "--strings-dynamic",
        type=str,
        default=None,
        help="Size for the strings-dynamic-buffer",
    )
    server_args["strings_static"] = arg(
        "--strings-static",
        type=str,
        default=None,
        help="Size for static strings-static-buffer",
    )
    server_args["tensors_dynamic"] = arg(
        "--tensors-dynamic",
        type=str,
        default=None,
        help="Size for the tensors-dynamic-buffer",
    )
    server_args["tensors_static"] = arg(
        "--tensors-static",
        type=str,
        default=None,
        help="Size for static tensors-static-buffer",
    )
    server_args["private_buffer"] = arg(
        "--private-buffer",
        type=str,
        default=None,
        help="Size for the private-buffer",
    )
    server_args["versioned_buffer"] = arg(
        "--versioned-buffer",
        type=str,
        default=None,
        help="Size for the versioned-buffer",
    )
    server_args["unversioned_buffer"] = arg(
        "--unversioned-buffer",
        type=str,
        default=None,
        help="Size for the unversioned-buffer",
    )
    server_args["extra_args"] = arg(
        "--extra-args",
        type=str,
        default=None,
        help=(
            "Additional arguments to pass directly to the mdb server process. "
            "This allows advanced users to specify options not exposed in "
            "Qleverfile. The string is appended verbatim to the command."
        ),
    )
