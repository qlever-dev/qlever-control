from __future__ import annotations

import os
from pathlib import Path

from qlever.log import mute_log
from qlever.util import run_command
from qmdb.commands.index import IndexCommand
from qmdb.commands.query import QueryCommand
from qmdb.commands.start import StartCommand
from qmdb.commands.stop import StopCommand
import sparql_conformance.util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, write_ttl_file




def _make_args(config: Config, **overrides):
    return getattr(conformance_util, "make_args")(config, **overrides)


def _get_accept_header(result_format: str) -> str:
    return getattr(conformance_util, "get_accept_header")(result_format)


def _read_file(path: str) -> str:
    return getattr(conformance_util, "read_file")(path)


def _copy_graph_to_workdir(file_path: str, workdir: str) -> str:
    return getattr(conformance_util, "copy_graph_to_workdir")(
        file_path, workdir
    )


class MdbManager(EngineManager):
    """Manager for MillenniumDB using qmdb commands."""

    def protocol_endpoint(self) -> str:
        return "sparql"

    def setup(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[bool, bool, str, str]:
        server_success = False
        graph_files, cleanup_paths = self._prepare_graphs(graph_paths)
        index_success, index_log = self._index(config, graph_files)
        self._cleanup_graph_copies(cleanup_paths)
        if not index_success:
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(config)
        if not server_success:
            return index_success, server_success, index_log, server_log
        return index_success, server_success, index_log, server_log

    def cleanup(self, config: Config):
        self._stop_server(config)
        with mute_log():
            run_command(
                f"rm -rf {config.run_id}_index {config.run_id}.index-log.txt "
                f"{config.run_id}.server-log.txt"
            )

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> tuple[int, str]:
        return self._query(
            config,
            query,
            "application/x-www-form-urlencoded",
            result_format,
        )

    def update(self, config: Config, query: str) -> tuple[int, str]:
        return self._query(
            config,
            query,
            "application/sparql-update",
            "json",
            sparql_endpoint=(
                f"{config.server_address}:{config.port}/update"
            ),
        )

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
        sparql_endpoint: str | None = None,
    ) -> tuple[int, str]:
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
            sparql_endpoint=sparql_endpoint,
        )
        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, called_from_conformance_test=True)
                query_output = str(qc.query_output)
                body, _, status_line = query_output.rpartition("HTTP_STATUS:")
                status_line = status_line.strip()
                if not status_line:
                    return 1, query_output
                status = int(status_line)
            return status, body
        except Exception as e:
            return 1, str(e)

    def _index(
        self,
        config: Config,
        graph_files: list[str],
    ) -> tuple[bool, str]:
        index_binary = "mdb"
        if config.system == "native":
            index_binary = str(Path(config.path_to_binaries, index_binary))
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            index_binary=index_binary,
            btree_permutations=4,
            buffer_strings=None,
            buffer_tensors=None,
            prefixes=None,
            extra_args=None,
            cat_input_files=None,
            format="ttl",
        )
        try:
            with mute_log():
                result = IndexCommand().execute(
                    args=args, called_from_conformance_test=True
                )
        except Exception as e:
            return False, str(e)

        index_log = _read_file(f"./{config.run_id}.index-log.txt")
        return result, index_log

    def _start_server(self, config: Config) -> tuple[bool, str]:
        server_binary = "mdb"
        if config.system == "native":
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            server_binary=server_binary,
            threads=2,
            timeout="60s",
            extra_args=None,
            strings_dynamic=None,
            strings_static=None,
            tensors_dynamic=None,
            tensors_static=None,
            private_buffer=None,
            versioned_buffer=None,
            unversioned_buffer=None,
            run_in_foreground=False,
        )
        try:
            with mute_log():
                result = StartCommand().execute(
                    args, called_from_conformance_test=True
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file(f"./{config.run_id}.server-log.txt")
        return result, server_log

    def _stop_server(self, config: Config) -> tuple[bool, str]:
        args = _make_args(
            config,
            cmdline_regex=StopCommand.DEFAULT_REGEX,
        )
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _prepare_graphs(
        self,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[list[str], list[Path]]:
        workdir = Path(os.getcwd()).resolve()
        graph_files: list[str] = []
        cleanup_paths: list[Path] = []
        for graph_path, _graph_name in graph_paths:
            src = Path(graph_path).resolve()
            if src.suffix == ".rdf":
                ttl_name = f"{src.stem}.ttl"
                write_ttl_file(
                    ttl_name, rdf_xml_to_turtle(str(src), _graph_name)
                )
                graph_file = ttl_name
                cleanup_paths.append(workdir / ttl_name)
            elif src.parent == workdir:
                graph_file = src.name
            else:
                graph_file = _copy_graph_to_workdir(str(src), str(workdir))
                cleanup_paths.append(workdir / src.name)
            graph_files.append(graph_file)
        return graph_files, cleanup_paths

    def _cleanup_graph_copies(self, cleanup_paths: list[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue
