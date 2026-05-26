import os
import shlex
import shutil
from pathlib import Path
from typing import List, Tuple
from urllib.parse import quote, urlparse

from qblazegraph.commands.index import IndexCommand
from qblazegraph.commands.start import StartCommand
from qblazegraph.commands.stop import StopCommand
from qlever.commands.query import QueryCommand
from qlever.log import mute_log
from qlever.util import run_command
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, replace_empty_base_iri
import sparql_conformance.util as conformance_util


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


class BlazegraphManager(EngineManager):
    """Manager for Blazegraph using qblazegraph commands."""

    _CONFORMANCE_RWSTORE_TEMPLATE = "RWStore.conformance.properties"
    _DEFAULT_RWSTORE_TEMPLATE = "RWStore.properties"

    def __init__(self):
        self._quads_mode = False

    def protocol_endpoint(self) -> str:
        return "blazegraph/namespace/kb/sparql"

    def default_graph_construct_query(self) -> str:
        if self._quads_mode:
            return (
                "CONSTRUCT {?s ?p ?o} WHERE { "
                "GRAPH <http://www.bigdata.com/rdf#nullGraph> {?s ?p ?o}}"
            )
        return "CONSTRUCT {?s ?p ?o} WHERE { ?s ?p ?o }"

    def setup(
        self,
        config: Config,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> Tuple[bool, bool, str, str]:
        self._quads_mode = self._requires_quads_mode(graph_paths)
        requires_quads_mode = self._quads_mode
        server_success = False
        try:
            self._ensure_rwstore_properties(graph_paths)
        except Exception as e:
            print(f"Error preparing RWStore.properties: {e}")
            return False, server_success, str(e), ""

        if not requires_quads_mode:
            graph_files, cleanup_paths = self._prepare_graphs_for_index(
                graph_paths
            )
            index_success, index_log = self._index(config, graph_files)
            self._cleanup_graph_copies(cleanup_paths)
            if not index_success:
                return index_success, server_success, index_log, ""
            server_success, server_log = self._start_server(config)
            return index_success, server_success, index_log, server_log

        graph_files, cleanup_paths = self._prepare_graphs_for_http_load(
            graph_paths
        )
        index_success, index_log = self._index_empty_journal(config)
        if not index_success:
            self._cleanup_graph_copies(cleanup_paths)
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(config)
        if not server_success:
            self._cleanup_graph_copies(cleanup_paths)
            return index_success, server_success, index_log, server_log

        load_success, load_log = self._load_graphs_over_http(
            config, graph_files
        )
        self._cleanup_graph_copies(cleanup_paths)
        if not load_success:
            combined_log = f"{server_log}\n\n{load_log}".strip()
            return index_success, False, index_log, combined_log

        combined_log = f"{server_log}\n\n{load_log}".strip()
        return index_success, server_success, index_log, combined_log

    def cleanup(self, config: Config):
        self._stop_server(config)
        with mute_log():
            run_command(
                "rm -f blazegraph.jnl "
                "qlever-sparql-conformance.index-log.txt "
                "qlever-sparql-conformance.server-log.txt "
                "web.xml qlever-sparql-conformance.web.xml"
            )

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> Tuple[int, str]:
        return self._query(config, query, "query=", result_format)

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        return self._query(config, query, "update=", "json")

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
    ) -> Tuple[int, str]:
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
            sparql_endpoint=(
                f"{config.server_address}:{config.port}"
                "/blazegraph/namespace/kb/sparql"
            ),
        )
        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, True)
                query_output = str(qc.query_output)
                body, _, status_line = query_output.rpartition("HTTP_STATUS:")
                status = int(status_line.strip())
            return status, body
        except Exception as e:
            return 1, str(e)

    def _index(
        self,
        config: Config,
        graph_files: List[str],
    ) -> Tuple[bool, str]:
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            jvm_args="",
            extra_args="",
            blazegraph_jar="blazegraph.jar",
            image=config.image or "test",
        )
        try:
            with mute_log():
                result = IndexCommand().execute(args)
        except Exception as e:
            return False, str(e)

        index_log = _read_file("./qlever-sparql-conformance.index-log.txt")
        return result, index_log

    def _index_empty_journal(self, config: Config) -> Tuple[bool, str]:
        empty_file = Path(".qlever-sparql-conformance.empty.ttl")
        empty_file.write_text("", encoding="utf-8")
        try:
            return self._index(config, [empty_file.name])
        finally:
            try:
                empty_file.unlink()
            except FileNotFoundError:
                pass

    def _start_server(self, config: Config) -> Tuple[bool, str]:
        args = _make_args(
            config,
            run_in_foreground=False,
            jvm_args="",
            extra_args="",
            blazegraph_jar="blazegraph.jar",
            read_only="no",
            timeout="60s",
            image=config.image or "test",
        )
        try:
            with mute_log():
                result = StartCommand().execute(
                    args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file("./qlever-sparql-conformance.server-log.txt")
        return result, server_log

    def _stop_server(self, config: Config) -> Tuple[bool, str]:
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

    def _ensure_rwstore_properties(
        self,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> None:
        destination = Path("RWStore.properties")
        repo_root = Path(__file__).resolve().parents[3]
        source_file = self._DEFAULT_RWSTORE_TEMPLATE
        if self._requires_quads_mode(graph_paths):
            source_file = self._CONFORMANCE_RWSTORE_TEMPLATE
        source = repo_root / "src" / "qblazegraph" / source_file
        shutil.copy(source, destination)

    @staticmethod
    def _requires_quads_mode(
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> bool:
        for _graph_path, graph_name in graph_paths:
            if graph_name not in ("", "-", None):
                return True
        return False

    def _prepare_graphs_for_index(
        self,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> Tuple[List[str], List[Path]]:
        workdir = Path(os.getcwd()).resolve()
        cwd_uri = workdir.as_uri() + "/"
        graph_files: List[str] = []
        cleanup_paths: List[Path] = []
        for i, (graph_path, graph_name) in enumerate(graph_paths):
            src = Path(graph_path).resolve()
            if src.suffix == ".rdf":
                generated_name = f"{src.stem}.{i}.ttl"
                turtle_data = rdf_xml_to_turtle(str(src), graph_name)
                (workdir / generated_name).write_text(
                    turtle_data, encoding="utf-8"
                )
                graph_files.append(generated_name)
                cleanup_paths.append(workdir / generated_name)
                continue
            temp_name, temp_path = replace_empty_base_iri(src, workdir, cwd_uri, "blazegraph")
            if temp_path is not None:
                graph_files.append(temp_name)
                cleanup_paths.append(temp_path)
                continue
            if src.parent == workdir:
                graph_files.append(src.name)
                continue
            graph_files.append(
                _copy_graph_to_workdir(str(src), str(workdir))
            )
            cleanup_paths.append(workdir / src.name)
        return graph_files, cleanup_paths

    def _prepare_graphs_for_http_load(
        self,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> Tuple[List[Tuple[Path, str]], List[Path]]:
        workdir = Path(os.getcwd()).resolve()
        cwd_uri = workdir.as_uri() + "/"
        file_to_named_uri: dict[str, str] = {}
        for gp, gn in graph_paths:
            if gn and gn not in ("-", ""):
                fname = Path(gp).resolve().name
                file_to_named_uri[fname] = gn
        graph_files: List[Tuple[Path, str]] = []
        cleanup_paths: List[Path] = []
        for i, (graph_path, graph_name) in enumerate(graph_paths):
            src = Path(graph_path).resolve()
            if src.suffix == ".rdf":
                generated_path = workdir / f"{src.stem}.{i}.ttl"
                turtle_data = rdf_xml_to_turtle(str(src), graph_name)
                generated_path.write_text(turtle_data, encoding="utf-8")
                graph_files.append((generated_path, graph_name))
                cleanup_paths.append(generated_path)
                continue
            is_default = graph_name in ("-", "", None)
            if is_default:
                replacement = file_to_named_uri.get(src.name, cwd_uri)
                temp_name, temp_path = replace_empty_base_iri(src, workdir, replacement, "blazegraph")
                if temp_path is not None:
                    graph_files.append((temp_path, graph_name))
                    cleanup_paths.append(temp_path)
                    continue
            graph_files.append((src, graph_name))
        return graph_files, cleanup_paths

    def _load_graphs_over_http(
        self,
        config: Config,
        graph_files: List[Tuple[Path, str]],
    ) -> Tuple[bool, str]:
        endpoint = (
            f"http://{config.server_address}:{config.port}"
            "/blazegraph/namespace/kb/sparql"
        )
        load_logs: List[str] = []
        for graph_path, graph_name in graph_files:
            content_type = self._get_rdf_content_type(graph_path)
            normalized_graph_name = self._normalize_graph_name(
                graph_name, graph_path
            )
            url = endpoint
            if normalized_graph_name is not None:
                url = (
                    f"{endpoint}?context-uri="
                    f"{quote(normalized_graph_name, safe='')}"
                )
            curl_cmd = (
                f'curl -sS -X POST "{url}" '
                f'-H "Content-Type: {content_type}" '
                f'--data-binary @{shlex.quote(str(graph_path))} '
                f'-w "HTTP_STATUS:%{{http_code}}"'
            )
            try:
                response = run_command(curl_cmd, return_output=True)
            except Exception as e:
                load_logs.append(
                    f"LOAD_FAIL path={graph_path} "
                    f"graph={normalized_graph_name}: {e}"
                )
                return False, "\n".join(load_logs)

            body, _, status_line = response.rpartition("HTTP_STATUS:")
            status_line = status_line.strip()
            status = int(status_line) if status_line.isdigit() else 0
            load_logs.append(
                f"LOAD path={graph_path.name} "
                f"graph={normalized_graph_name or '-'} status={status}"
            )
            if not 200 <= status < 300:
                load_logs.append(body.strip())
                return False, "\n".join(load_logs)

        return True, "\n".join(load_logs)

    @staticmethod
    def _get_rdf_content_type(graph_path: Path) -> str:
        suffix = graph_path.suffix.lower()
        mapping = {
            ".ttl": "text/turtle",
            ".nt": "application/n-triples",
            ".nq": "application/n-quads",
            ".trig": "application/trig",
            ".rdf": "application/rdf+xml",
            ".xml": "application/rdf+xml",
        }
        return mapping.get(suffix, "text/turtle")

    @staticmethod
    def _normalize_graph_name(graph_name: str, graph_path: Path) -> str | None:
        if graph_name in ("", "-", None):
            return None
        if BlazegraphManager._is_absolute_uri(graph_name):
            return graph_name
        return graph_path.resolve().as_uri()

    @staticmethod
    def _is_absolute_uri(value: str) -> bool:
        parsed = urlparse(value)
        return bool(parsed.scheme)

    def _cleanup_graph_copies(self, cleanup_paths: List[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue
