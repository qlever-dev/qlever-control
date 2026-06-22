from __future__ import annotations

import re
import os
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import rdflib

from qgraphdb.commands.index import IndexCommand
from qgraphdb.commands.query import QueryCommand
from qgraphdb.commands.start import StartCommand
from qgraphdb.commands.stop import StopCommand
from qlever.log import mute_log
from qlever.util import run_command, run_curl_command
import sparql_conformance.util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, write_ttl_file, replace_empty_base_iri


GRAPHDB_CONFIG_TTL_URL = (
    "https://graphdb.ontotext.com/documentation/11.0/_downloads/"
    "565be93599bf4c3324147fb94b562595/repo-config.ttl"
)
DEFAULT_NAME = "qlever-sparql-conformance"
DEFAULT_BASE_IRI = "http://example.org/"

_SPARQL_RESULTS_NS = "http://www.w3.org/2005/sparql-results#"
_RS = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")


def _sparql_xml_to_result_set_ttl(xml_str: str) -> str:
    """Convert SPARQL results XML to SPARQL result-set Turtle vocabulary.

    GraphDB returns HTTP 406 when Accept: text/turtle is requested for
    SELECT/ASK queries.  We fetch XML instead and convert here so that
    compare_ttl() in rdf_tools.py can compare the result against the
    .ttl expected-result files from the SPARQL 1.0 test suite.
    """
    g = rdflib.Graph()
    g.bind("rs", _RS)

    root = ET.fromstring(xml_str)
    ns = _SPARQL_RESULTS_NS

    result_set = rdflib.BNode()
    g.add((result_set, rdflib.RDF.type, _RS.ResultSet))

    # ASK result
    boolean_elem = root.find(f"{{{ns}}}boolean")
    if boolean_elem is not None:
        val = (boolean_elem.text or "").strip().lower() == "true"
        g.add((result_set, _RS.boolean,
               rdflib.Literal(val, datatype=rdflib.XSD.boolean)))
        return g.serialize(format="turtle")

    # SELECT result
    head = root.find(f"{{{ns}}}head")
    if head is not None:
        for var in head.findall(f"{{{ns}}}variable"):
            g.add((result_set, _RS.resultVariable,
                   rdflib.Literal(var.get("name"))))

    results_elem = root.find(f"{{{ns}}}results")
    if results_elem is not None:
        for result_elem in results_elem.findall(f"{{{ns}}}result"):
            solution = rdflib.BNode()
            g.add((result_set, _RS.solution, solution))
            for binding_elem in result_elem.findall(f"{{{ns}}}binding"):
                name = binding_elem.get("name")
                binding_node = rdflib.BNode()
                g.add((solution, _RS.binding, binding_node))
                g.add((binding_node, _RS.variable, rdflib.Literal(name)))

                uri_elem = binding_elem.find(f"{{{ns}}}uri")
                lit_elem = binding_elem.find(f"{{{ns}}}literal")
                bnode_elem = binding_elem.find(f"{{{ns}}}bnode")

                if uri_elem is not None:
                    text = (uri_elem.text or "").strip()
                    g.add((binding_node, _RS.value, rdflib.URIRef(text)))
                elif lit_elem is not None:
                    text = lit_elem.text or ""
                    datatype = lit_elem.get("datatype")
                    lang = lit_elem.get(
                        "{http://www.w3.org/XML/1998/namespace}lang")
                    if datatype:
                        g.add((binding_node, _RS.value,
                               rdflib.Literal(text,
                                              datatype=rdflib.URIRef(datatype))))
                    elif lang:
                        g.add((binding_node, _RS.value,
                               rdflib.Literal(text, lang=lang)))
                    else:
                        g.add((binding_node, _RS.value,
                               rdflib.Literal(text)))
                elif bnode_elem is not None:
                    text = (bnode_elem.text or "").strip()
                    g.add((binding_node, _RS.value, rdflib.BNode(text)))

    return g.serialize(format="turtle")


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


def _graph_to_trig(turtle_data: str, graph_name: str) -> str:
    graph = rdflib.Graph()
    graph.parse(data=turtle_data, format="turtle", publicID=graph_name)
    dataset = rdflib.ConjunctiveGraph()
    context = dataset.get_context(rdflib.URIRef(graph_name))
    for triple in graph:
        context.add(triple)
    return str(dataset.serialize(format="trig"))


def _ensure_base_iri(query: str) -> str:
    if re.search(r"(?im)^\s*base\s+<", query or ""):
        return query
    return f"BASE <{DEFAULT_BASE_IRI}>\n{query}"


def _is_select_or_ask(query: str) -> bool:
    """Return True for SELECT/ASK queries; CONSTRUCT/DESCRIBE return RDF so False."""
    no_comments = re.sub(r'#[^\n]*', '', query)
    m = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', no_comments, re.IGNORECASE)
    return m is not None and m.group(1).upper() in ('SELECT', 'ASK')


def _is_rdf_query(query: str) -> bool:
    """Return True for CONSTRUCT/DESCRIBE queries that return RDF, not a result set."""
    no_comments = re.sub(r'#[^\n]*', '', query)
    m = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', no_comments, re.IGNORECASE)
    return m is not None and m.group(1).upper() in ('CONSTRUCT', 'DESCRIBE')


def _license_file_path() -> Path:
    for key in ("GRAPHDB_LICENSE_FILE", "GRAPHDB_LICENSE_PATH"):
        if value := os.environ.get(key):
            return Path(value)
    for candidate in ("graphdb.license", "graphdb.license"):
        path = Path(candidate)
        if path.exists():
            print(f"Using GraphDB license file {path}")
            return path
    return Path("graphdb.license")


def _set_config_ttl_option(option: str, value: str) -> None:
    config_path = Path("config.ttl")
    if not config_path.exists():
        return
    graph = rdflib.Graph()
    graph.parse(config_path, format="ttl")
    for sub, pred, obj in list(graph):
        pred_str = str(pred).split("#")[-1]
        if pred_str == option:
            graph.remove((sub, pred, obj))
            graph.add((sub, pred, rdflib.Literal(value)))
    graph.serialize(destination=config_path, format="ttl")


class GraphdbManager(EngineManager):
    """Manager for GraphDB using qgraphdb commands."""

    def __init__(self):
        self._run_id = DEFAULT_NAME

    def protocol_endpoint(self) -> str:
        return f"repositories/{self._run_id}"

    def protocol_update_endpoint(self) -> str:
        return f"repositories/{self._run_id}/statements"

    def setup(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[bool, bool, str, str]:
        self._run_id = config.run_id
        server_success = False
        config_ready, config_log = self._ensure_config_ttl()
        if not config_ready:
            return False, server_success, config_log, ""

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
                f"rm -rf {config.run_id}_index "
                f"{config.run_id}.index-log.txt "
                f"{config.run_id}.server-log.txt"
            )

    def reset_graphs(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> bool:
        """Clear all graphs and reload initial data via HTTP without restarting GraphDB."""
        status, _ = self.update(config, "CLEAR ALL")
        if status >= 400:
            self.cleanup(config)
            ok_i, ok_s, _, _ = self.setup(config, graph_paths)
            return ok_i and ok_s

        if not graph_paths:
            return True

        workdir = Path(os.getcwd()).resolve()
        cwd_uri = workdir.as_uri() + "/"
        file_to_named_uri: dict[str, str] = {}
        for gp, gn in graph_paths:
            if gn and gn not in ("-", ""):
                fname = Path(gp).resolve().name
                abs_gn = gn if "://" in gn else DEFAULT_BASE_IRI + fname
                file_to_named_uri[fname] = abs_gn

        base_data_url = (
            f"http://{config.server_address}:{config.port}"
            f"/repositories/{config.run_id}/rdf-graphs/service"
        )
        tmp_file = workdir / "_graphdb_reset_tmp.ttl"
        try:
            for graph_path, graph_name in graph_paths:
                src = Path(graph_path).resolve()
                is_named = graph_name not in ("-", "", None)
                if src.suffix == ".rdf":
                    abs_name = graph_name if "://" in graph_name else DEFAULT_BASE_IRI + src.name
                    turtle_data = rdf_xml_to_turtle(str(src), abs_name)
                elif is_named:
                    turtle_data = src.read_text(encoding="utf-8")
                else:
                    replacement = file_to_named_uri.get(src.name, cwd_uri)
                    temp_name, temp_path = replace_empty_base_iri(
                        src, workdir, replacement, "graphdb"
                    )
                    if temp_path is not None:
                        turtle_data = temp_path.read_text(encoding="utf-8")
                        temp_path.unlink()
                    else:
                        turtle_data = src.read_text(encoding="utf-8")

                if is_named:
                    abs_name = graph_name if "://" in graph_name else DEFAULT_BASE_IRI + src.name
                    encoded = urllib.parse.quote(abs_name, safe="")
                    target_url = f"{base_data_url}?graph={encoded}"
                else:
                    target_url = f"{base_data_url}?default"

                tmp_file.write_text(turtle_data, encoding="utf-8")
                with mute_log():
                    run_command(
                        f'curl -s -X PUT "{target_url}"'
                        f' -H "Content-Type: text/turtle"'
                        f' --data-binary @"{tmp_file}"'
                    )
        finally:
            if tmp_file.exists():
                tmp_file.unlink()
        return True

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> tuple[int, str]:
        if result_format == "ttl" and _is_select_or_ask(query):
            # GraphDB returns HTTP 406 for Accept: text/turtle on SELECT/ASK
            # queries.  Fetch as SPARQL results XML and convert to rs: Turtle.
            status, body = self._query(config, query, "query=", "srx")
            if status != 200:
                return status, body
            try:
                return 200, _sparql_xml_to_result_set_ttl(body)
            except Exception as e:
                return 1, str(e)
        if result_format == "srx" and _is_rdf_query(query):
            # CONSTRUCT/DESCRIBE return RDF; Accept: application/sparql-results+xml
            # causes GraphDB to reply "No acceptable file format found." (406).
            result_format = "ttl"
        return self._query(config, query, "query=", result_format)

    def update(self, config: Config, query: str) -> tuple[int, str]:
        return self._query(
            config,
            query,
            "update=",
            "json",
            endpoint_suffix="/statements",
        )

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
        endpoint_suffix: str = "",
    ) -> tuple[int, str]:
        query = _ensure_base_iri(query)
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
            sparql_endpoint=(
                f"http://{config.server_address}:{config.port}"
                f"/repositories/{config.run_id}{endpoint_suffix}"
            ),
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

    def _ensure_config_ttl(self) -> tuple[bool, str]:
        if Path("config.ttl").exists():
            _set_config_ttl_option("enable-context-index", "true")
            return True, ""
        try:
            with mute_log():
                run_curl_command(
                    url=GRAPHDB_CONFIG_TTL_URL, result_file="config.ttl"
                )
        except Exception as e:
            return False, str(e)
        _set_config_ttl_option("enable-context-index", "true")
        return True, ""

    def _index(
        self,
        config: Config,
        graph_files: list[str],
    ) -> tuple[bool, str]:
        index_binary = "importrdf"
        if config.system == "native":
            index_binary = str(Path(config.path_to_binaries, index_binary))
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            index_binary=index_binary,
            threads=None,
            jvm_args="-Xms4G -Xmx4G",
            entity_index_size=10000000,
            ruleset="empty",
            extra_args="",
            timeout="60s",
            read_only="no",
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
        server_binary = "graphdb"
        if config.system == "native":
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            server_binary=server_binary,
            heap_size_gb="4G",
            extra_env_args="",
            extra_args="",
            run_in_foreground=False,
            read_only="no",
            timeout="60s",
            license_file_path=_license_file_path(),
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
        cwd_uri = workdir.as_uri() + "/"
        file_to_named_uri: dict[str, str] = {}
        for gp, gn in graph_paths:
            if gn and gn not in ("-", ""):
                fname = Path(gp).resolve().name
                abs_gn = gn if "://" in gn else DEFAULT_BASE_IRI + fname
                file_to_named_uri[fname] = abs_gn
        graph_files: list[str] = []
        cleanup_paths: list[Path] = []
        for graph_path, graph_name in graph_paths:
            is_named_graph = graph_name not in ("-", "", None)
            if graph_path.endswith(".rdf"):
                graph_path_new = Path(graph_path).name
                abs_graph_name = (
                    graph_name if "://" in graph_name
                    else DEFAULT_BASE_IRI + graph_path_new
                )
                turtle_data = rdf_xml_to_turtle(graph_path, abs_graph_name)
                if is_named_graph:
                    graph_path_new = graph_path_new.replace(".rdf", ".trig")
                    trig_data = _graph_to_trig(turtle_data, abs_graph_name)
                    (workdir / graph_path_new).write_text(
                        trig_data, encoding="utf-8"
                    )
                else:
                    graph_path_new = graph_path_new.replace(".rdf", ".ttl")
                    write_ttl_file(graph_path_new, turtle_data)
                graph_files.append(graph_path_new)
                cleanup_paths.append(workdir / graph_path_new)
                continue
            src = Path(graph_path).resolve()
            if is_named_graph:
                graph_path_new = src.stem + ".trig"
                abs_graph_name = (
                    graph_name if "://" in graph_name
                    else DEFAULT_BASE_IRI + src.name
                )
                turtle_data = src.read_text(encoding="utf-8")
                trig_data = _graph_to_trig(turtle_data, abs_graph_name)
                (workdir / graph_path_new).write_text(
                    trig_data, encoding="utf-8"
                )
                graph_files.append(graph_path_new)
                cleanup_paths.append(workdir / graph_path_new)
                continue
            replacement = file_to_named_uri.get(src.name, cwd_uri)
            temp_name, temp_path = replace_empty_base_iri(src, workdir, replacement, "graphdb")
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

    def _cleanup_graph_copies(self, cleanup_paths: list[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue
