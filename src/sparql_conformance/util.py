import re
import os
import shutil
from argparse import Namespace
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, unquote

from qlever.log import log
from sparql_conformance.config import Config


def make_args(config: Config, **overrides):
    base = dict(
        # GENERAL used by more than one.
        name="qlever-sparql-conformance",
        host_name=config.server_address,
        port=config.port,
        system=config.system,
        image=config.image,
        server_container="qlever-sparql-conformance-server-container",
        access_token="abc",
        only_pso_and_pos_permutations=False,
        use_patterns=True,
        # STOP SERVER.
        no_containers=config.system == "native",
        show=False,
        cmdline_regex="ServerMain.* -i [^ ]*%%NAME%%",
        # QUERY.
        sparql_endpoint=None,
        pin_to_cache=False,
        no_time=True,
        predefined_query=None,
        log_level="ERROR",
        # START SERVER.
        description="",
        text_description="",
        memory_for_queries="4GB",
        cache_max_size="1GB",
        cache_max_size_single_entry="100MB",
        cache_max_num_entries=1000000,
        num_threads=1,
        timeout=None,
        persist_updates=False,
        use_text_index="no",
        warmup_cmd=None,
        kill_existing_with_same_port=False,
        no_warmup=True,
        run_in_foreground=False,
        # INDEX.
        index_container = "qlever-sparql-conformance-index-container",
        cat_input_files=None,
        input_files='*.ttl',
        format='ttl',
        settings_json='{ "num-triples-per-batch": 1000000 }',
        parallel_parsing=False,
        text_index=None,
        stxxl_memory=None,
        parser_buffer_size=None,
        ulimit=None,
        overwrite_existing=True,
        vocabulary_type='on-disk-compressed',
        encode_as_id=None,
    )
    return Namespace(**{**base, **overrides})

def local_name(uri: str) -> str:
    """Extract the local name from a URI (after # or /)."""
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.split("/")[-1]


def uri_to_path(uri):
    parsed = urlparse(str(uri))
    if parsed.scheme != 'file':
        return uri
    return unquote(parsed.path)


def path_exists(path):
    if not os.path.exists(path):
        log.error(f"{path} does not exist!")
        return False
    return True


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def escape(string: Optional[str]) -> str:
    """
    Takes any string and returns the escaped version to use in html.
    """
    if string is None:
        return ''
    return string.replace(
        "&",
        "&amp;").replace(
        "<",
        "&lt;").replace(
        ">",
        "&gt;").replace(
        '\"',
        "&quot;").replace(
        "'",
        "&apos;")


def read_file(file_path: str) -> str:
    """
    Reads and returns the content of a file.

    If file does not exist return empty string.

    Parameters:
        file_path (str): The path to the file to be read.

    Returns:
        str: The content of the file.
    """
    try:
        data = open(file_path, "r", encoding="utf-8").read()
    except BaseException:
        data = ""
    return data


def remove_date_time_parts(index_log: str) -> str:
    """
    Remove date and time from index log.
    ex. 2023-12-20 14:02:33.089	- INFO:  You specified the input format: TTL
    to: INFO:  You specified the input format: TTL

    Parameters:
        index_log (str): The index log.

    Returns:
        The index log without time and date as a string.
    """
    pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\s*-"
    return re.sub(pattern, "", index_log)


def copy_graph_to_workdir(file_path: str, workdir: str) -> str:
    """
    Copy the file to the docker working directory and returns the new relative path.

    Args:
        file_path (str): Path to the source file.
        workdir (str): Path to the working directory mounted in docker.

    Returns:
        str: Basename, usable inside the container.
    """
    src = Path(file_path).resolve()
    dest = Path(workdir).resolve() / src.name
    shutil.copy(src, dest)
    return src.name


def get_accept_header(result_format: str) -> str:
    format_headers = {
        "csv": "text/csv",
        "tsv": "text/tab-separated-values",
        "srx": "application/sparql-results+xml",
        "ttl": "text/turtle",
        "json": "application/sparql-results+json"
    }
    return format_headers.get(result_format, "application/sparql-results+json")
