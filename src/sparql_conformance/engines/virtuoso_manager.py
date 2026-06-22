from __future__ import annotations

import os
import re
from pathlib import Path

from qlever.log import mute_log
from qlever.util import run_command
from qvirtuoso.commands.index import IndexCommand
from qvirtuoso.commands.query import QueryCommand
from qvirtuoso.commands.start import StartCommand
from qvirtuoso.commands.stop import StopCommand
from sparql_conformance import util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, write_ttl_file, replace_empty_base_iri


DEFAULT_GRAPH_URI = "urn:qlever:default-graph"
UPDATE_USER = "qlever_update"
UPDATE_PASSWORD = "qlever_update_pw"
QUERY_USER = "SPARQL"


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


class VirtuosoManager(EngineManager):
    """Manager for Virtuoso using qvirtuoso commands."""

    def protocol_endpoint(self) -> str:
        return "sparql"

    def setup(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[bool, bool, str, str]:
        server_success = False
        graph_files, cleanup_paths, graph_names = self._prepare_graphs(
            graph_paths
        )
        index_success, index_log = self._index(
            config, graph_files, graph_names
        )
        self._cleanup_graph_copies(cleanup_paths)
        if not index_success:
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(
            config,
            graph_names=graph_names,
        )
        if not server_success:
            return index_success, server_success, index_log, server_log
        auth_success, auth_log = self._configure_update_auth(
            config,
            graph_names,
        )
        if not auth_success:
            return (
                index_success,
                server_success,
                index_log,
                f"{server_log}\n{auth_log}",
            )
        return index_success, server_success, index_log, server_log

    def cleanup(self, config: Config):
        self._stop_server(config)
        self._stop_index_container(config)
        with mute_log():
            if config.system != "native":
                # Force-remove any containers left in stopped/removing state so
                # the next docker run can reuse the name without a name conflict.
                run_command(
                    f"{config.system} rm -f "
                    f"{config.run_id}-server-container "
                    f"{config.run_id}-index-container "
                    "2>/dev/null || true"
                )
            run_command(
                f"rm -f {config.run_id}*log.txt "
                "virtuoso.db virtuoso.trx virtuoso.pxa "
                "virtuoso-temp.db virtuoso.cpt-after-recov "
                "virtuoso.trx-after-recov"
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
            "query=",
            result_format,
            endpoint_suffix="/sparql",
            use_http_auth=False,
        )

    def update(self, config: Config, query: str) -> tuple[int, str]:
        return self._query(
            config,
            query,
            "update=",
            "json",
            endpoint_suffix="/sparql-auth",
            use_http_auth=True,
        )

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
        endpoint_suffix: str,
        use_http_auth: bool,
    ) -> tuple[int, str]:
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
        )
        if self._should_set_default_graph_uri(query, content_type):
            args.default_graph_uri = DEFAULT_GRAPH_URI
        args.sparql_endpoint = (
            f"{config.server_address}:{config.port}{endpoint_suffix}"
        )
        if use_http_auth:
            args.http_user = UPDATE_USER
            args.http_password = UPDATE_PASSWORD
        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, called_from_conformance_test=True)
                query_output = str(qc.query_output)
                body, _, status_line = query_output.rpartition(
                    "HTTP_STATUS:"
                )
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
        graph_names: list[str],
    ) -> tuple[bool, str]:
        index_binary = "isql"
        server_binary = "virtuoso-t"
        if config.system == "native":
            index_binary = str(Path(config.path_to_binaries, index_binary))
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            index_binary=index_binary,
            isql_port=1111,
            num_parallel_loaders=1,
            free_memory_gb="4G",
            server_binary=server_binary,
        )
        args.graph_files = graph_files
        args.graph_names = graph_names
        args.default_graph_uri = DEFAULT_GRAPH_URI
        try:
            with mute_log():
                result = IndexCommand().execute(
                    args=args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        index_log = _read_file(f"./{config.run_id}.index-log.txt")
        return result, index_log

    def _start_server(
        self,
        config: Config,
        graph_names: list[str] | None = None,
    ) -> tuple[bool, str]:
        server_binary = "virtuoso-t"
        if config.system == "native":
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            server_binary=server_binary,
            max_query_memory="2G",
            extra_args="",
            run_in_foreground=False,
            timeout="30s",
        )
        args.default_graph_uri = DEFAULT_GRAPH_URI
        if graph_names is not None:
            args.graph_names = graph_names
        try:
            with mute_log():
                result = StartCommand().execute(
                    args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file(f"./{config.run_id}.server-log.txt")
        return result, server_log

    def _stop_server(self, config: Config) -> tuple[bool, str]:
        args = _make_args(config, cmdline_regex=StopCommand.DEFAULT_REGEX)
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _stop_index_container(self, config: Config) -> tuple[bool, str]:
        args = _make_args(config)
        args.server_container = args.index_container
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _prepare_graphs(
        self,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[list[str], list[Path], list[str]]:
        workdir = Path(os.getcwd()).resolve()
        cwd_uri = workdir.as_uri() + "/"
        file_to_named_uri: dict[str, str] = {}
        for gp, gn in graph_paths:
            if gn and gn not in ("-", ""):
                fname = Path(gp).resolve().name
                file_to_named_uri[fname] = gn
        graph_files: list[str] = []
        cleanup_paths: list[Path] = []
        graph_names: list[str] = []
        for graph_path, graph_name in graph_paths:
            if graph_path.endswith(".rdf"):
                graph_path_new = Path(graph_path).name
                graph_path_new = graph_path_new.replace(".rdf", ".ttl")
                write_ttl_file(
                    graph_path_new,
                    rdf_xml_to_turtle(graph_path, graph_name),
                )
                graph_files.append(graph_path_new)
                cleanup_paths.append(workdir / graph_path_new)
                graph_names.append(self._map_graph_name(graph_name))
                continue
            src = Path(graph_path).resolve()
            replacement = file_to_named_uri.get(src.name, cwd_uri)
            temp_name, temp_path = replace_empty_base_iri(src, workdir, replacement, "virtuoso")
            if temp_path is not None:
                graph_files.append(temp_name)
                cleanup_paths.append(temp_path)
                graph_names.append(self._map_graph_name(graph_name))
                continue
            if src.parent == workdir:
                graph_files.append(src.name)
                graph_names.append(self._map_graph_name(graph_name))
                continue
            graph_files.append(
                _copy_graph_to_workdir(str(src), str(workdir))
            )
            cleanup_paths.append(workdir / src.name)
            graph_names.append(self._map_graph_name(graph_name))
        return graph_files, cleanup_paths, graph_names

    @staticmethod
    def _map_graph_name(graph_name: str) -> str:
        if graph_name == "-":
            return DEFAULT_GRAPH_URI
        return graph_name

    def _cleanup_graph_copies(self, cleanup_paths: list[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue

    def _configure_update_auth(
        self,
        config: Config,
        graph_names: list[str],
    ) -> tuple[bool, str]:
        graph_names_unique = list(dict.fromkeys(graph_names))
        escaped_user = self._sql_quote(UPDATE_USER)
        escaped_password = self._sql_quote(UPDATE_PASSWORD)
        escaped_query_user = self._sql_quote(QUERY_USER)
        sql_lines = [
            "whenever sqlerror continue;",
            f"DB.DBA.USER_CREATE('{escaped_user}', '{escaped_password}');",
            f"DB.DBA.USER_SET_OPTION('{escaped_user}', 'SQL_ENABLE', '1');",
            f"DB.DBA.USER_SET_OPTION('{escaped_user}', 'DAV_ENABLE', '1');",
            # Grant execute on procedures needed for INSERT/UPDATE queries
            f"GRANT EXECUTE ON DB.DBA.RDF_MAKE_LONG_OF_LITERAL TO \"{escaped_user}\";",
            f"GRANT EXECUTE ON DB.DBA.L_O_LOOK TO \"{escaped_user}\";",
            # Grant SELECT/EXECUTE on objects needed for SERVICE federation.
            # Use PUBLIC to avoid issues with SPARQL being a Virtuoso keyword.
            "GRANT SELECT ON DB.DBA.SPARQL_SINV_2 TO PUBLIC;",
            "GRANT EXECUTE ON DB.DBA.SPARQL_SINV_IMP TO PUBLIC;",
            "whenever sqlerror exit;",
            f"ADD USER GROUP \"{escaped_user}\" \"SPARQL_UPDATE\";",
            f"DB.DBA.RDF_DEFAULT_USER_PERMS_SET('{escaped_user}', 3, 0);",
            "DB.DBA.RDF_DEFAULT_USER_PERMS_SET("
            f"'{escaped_query_user}', 1, 0);",
        ]
        for graph_name in graph_names_unique:
            if graph_name in ("", None):
                continue
            escaped_graph_name = self._sql_quote(graph_name)
            sql_lines.append(
                "DB.DBA.RDF_GRAPH_USER_PERMS_SET("
                f"'{escaped_graph_name}', '{escaped_user}', 3);"
            )
            sql_lines.append(
                "DB.DBA.RDF_GRAPH_USER_PERMS_SET("
                f"'{escaped_graph_name}', '{escaped_query_user}', 1);"
            )

        sql = "\n".join(sql_lines)
        isql_binary = "isql"
        if config.system == "native":
            isql_binary = str(Path(config.path_to_binaries, isql_binary))
        full_cmd = (
            f"cat <<'SQL' | {isql_binary} 1111 dba dba\n"
            f"{sql}\n"
            "SQL"
        )
        if config.system != "native":
            container_name = _make_args(config).server_container
            exec_cmd = f"{config.system} exec -w /database {container_name}"
            full_cmd = f"{exec_cmd} bash -lc \"{full_cmd}\""
        try:
            with mute_log(50):
                run_command(full_cmd)
        except Exception as e:
            return False, str(e)
        return True, "Success"

    @staticmethod
    def _sql_quote(value: str) -> str:
        return value.replace("'", "''")

    @staticmethod
    def _should_set_default_graph_uri(query: str, content_type: str) -> bool:
        query_lower = query.lower()
        if "define input:default-graph-uri" in query_lower:
            return False
        if content_type == "update=":
            # WITH sets the active graph context; injecting the default graph
            # URI causes Virtuoso to also read from the pre-loaded default graph.
            if re.search(r"\bwith\b", query_lower):
                return False
            return True
        if re.search(r"\bgraph\b", query_lower):
            return False
        return True
