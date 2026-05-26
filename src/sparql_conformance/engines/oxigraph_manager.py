from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import rdflib

from qlever.log import mute_log
from qlever.util import run_command
from qoxigraph.commands.index import IndexCommand
from qoxigraph.commands.query import QueryCommand
from qoxigraph.commands.start import StartCommand
from qoxigraph.commands.stop import StopCommand
from sparql_conformance import util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, write_ttl_file, replace_empty_base_iri


DEFAULT_NAME = "qlever-sparql-conformance"

_SPARQL_RESULTS_NS = "http://www.w3.org/2005/sparql-results#"
_RS = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")


def _sparql_xml_to_result_set_ttl(xml_str: str) -> str:
    """Convert SPARQL results XML to SPARQL result-set Turtle vocabulary.

    Oxigraph rejects Accept: text/turtle for SELECT/ASK queries.
    We fetch XML instead and convert here so that compare_ttl() can
    compare the result against .ttl expected-result files.
    """
    g = rdflib.Graph()
    g.bind("rs", _RS)

    root = ET.fromstring(xml_str)
    ns = _SPARQL_RESULTS_NS

    result_set = rdflib.BNode()
    g.add((result_set, rdflib.RDF.type, _RS.ResultSet))

    boolean_elem = root.find(f"{{{ns}}}boolean")
    if boolean_elem is not None:
        val = (boolean_elem.text or "").strip().lower() == "true"
        g.add((result_set, _RS.boolean,
               rdflib.Literal(val, datatype=rdflib.XSD.boolean)))
        return g.serialize(format="turtle")

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
                        g.add((binding_node, _RS.value, rdflib.Literal(text)))
                elif bnode_elem is not None:
                    text = (bnode_elem.text or "").strip()
                    g.add((binding_node, _RS.value, rdflib.BNode(text)))

    return g.serialize(format="turtle")


def _sparql_json_to_result_set_ttl(json_str: str) -> str:
    """Convert SPARQL results JSON to SPARQL result-set Turtle vocabulary.

    Used as an intermediate step because this version of Oxigraph only
    supports application/sparql-results+json (not XML) for SELECT/ASK.
    """
    data = json.loads(json_str)
    g = rdflib.Graph()
    g.bind("rs", _RS)

    result_set = rdflib.BNode()
    g.add((result_set, rdflib.RDF.type, _RS.ResultSet))

    if "boolean" in data:
        val = bool(data["boolean"])
        g.add((result_set, _RS.boolean,
               rdflib.Literal(val, datatype=rdflib.XSD.boolean)))
        return g.serialize(format="turtle")

    head = data.get("head", {})
    for var in head.get("vars", []):
        g.add((result_set, _RS.resultVariable, rdflib.Literal(var)))

    for binding_dict in data.get("results", {}).get("bindings", []):
        solution = rdflib.BNode()
        g.add((result_set, _RS.solution, solution))
        for name, value_obj in binding_dict.items():
            binding_node = rdflib.BNode()
            g.add((solution, _RS.binding, binding_node))
            g.add((binding_node, _RS.variable, rdflib.Literal(name)))

            vtype = value_obj.get("type", "")
            vvalue = value_obj.get("value", "")

            if vtype == "uri":
                g.add((binding_node, _RS.value, rdflib.URIRef(vvalue)))
            elif vtype == "literal":
                datatype = value_obj.get("datatype")
                lang = value_obj.get("xml:lang")
                if datatype:
                    g.add((binding_node, _RS.value,
                           rdflib.Literal(vvalue, datatype=rdflib.URIRef(datatype))))
                elif lang:
                    g.add((binding_node, _RS.value,
                           rdflib.Literal(vvalue, lang=lang)))
                else:
                    g.add((binding_node, _RS.value, rdflib.Literal(vvalue)))
            elif vtype == "bnode":
                g.add((binding_node, _RS.value, rdflib.BNode(vvalue)))

    return g.serialize(format="turtle")


def _is_select_or_ask(query: str) -> bool:
    """Return True for SELECT/ASK queries; CONSTRUCT/DESCRIBE return RDF so False."""
    no_comments = re.sub(r'#[^\n]*', '', query)
    m = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', no_comments, re.IGNORECASE)
    return m is not None and m.group(1).upper() in ('SELECT', 'ASK')


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
    graph.parse(data=turtle_data, format="turtle")
    dataset = rdflib.ConjunctiveGraph()
    context = dataset.get_context(rdflib.URIRef(graph_name))
    for triple in graph:
        context.add(triple)
    return str(dataset.serialize(format="trig"))


class OxigraphManager(EngineManager):
    """Manager for Oxigraph using qoxigraph commands."""

    def protocol_endpoint(self) -> str:
        return "query"

    def setup(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[bool, bool, str, str]:
        server_success = False
        config.GRAPHSTORE = "store"
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
                f"rm -rf {DEFAULT_NAME}_index "
                f"{DEFAULT_NAME}.index-log.txt "
                f"{DEFAULT_NAME}.server-log.txt"
            )

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> tuple[int, str]:
        if result_format == "ttl" and _is_select_or_ask(query):
            # Oxigraph rejects Accept: text/turtle for SELECT/ASK and also
            # rejects application/sparql-results+xml — only JSON is supported.
            # Fetch as JSON and convert to rs: Turtle vocabulary.
            status, body = self._query(config, query, "query=", "json")
            if status != 200:
                return status, body
            try:
                return 200, _sparql_json_to_result_set_ttl(body)
            except Exception as e:
                return 1, str(e)
        # DESCRIBE/CONSTRUCT return RDF — Oxigraph rejects sparql-results+xml.
        # Syntax tests set result_format="srx" for non-CONSTRUCT queries, but
        # DESCRIBE also returns RDF, so we override to ttl for those queries.
        if result_format == "srx" and not _is_select_or_ask(query):
            result_format = "ttl"
        return self._query(config, query, "query=", result_format)

    def update(self, config: Config, query: str) -> tuple[int, str]:
        return self._query(config, query, "update=", "json")

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
    ) -> tuple[int, str]:
        sparql_endpoint = None
        if content_type == "update=":
            sparql_endpoint = (
                f"{config.server_address}:{config.port}/update"
            )
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
        index_binary = "oxigraph"
        if config.system == "native":
            index_binary = str(Path(config.path_to_binaries, index_binary))
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            index_binary=index_binary,
            extra_args="",
            read_only="yes",
            lenient=True,
            format="ttl",
        )
        try:
            with mute_log():
                result = IndexCommand().execute(
                    args=args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        index_log = _read_file(f"./{DEFAULT_NAME}.index-log.txt")
        return result, index_log

    def _start_server(self, config: Config) -> tuple[bool, str]:
        server_binary = "oxigraph"
        if config.system == "native":
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            server_binary=server_binary,
            extra_args="",
            read_only="no",
            run_in_foreground=False,
            timeout="60s",
        )
        try:
            with mute_log():
                result = StartCommand().execute(
                    args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file(f"./{DEFAULT_NAME}.server-log.txt")
        return result, server_log

    def _stop_server(self, config: Config) -> tuple[bool, str]:
        args = _make_args(config, cmdline_regex=StopCommand.DEFAULT_REGEX)
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
                file_to_named_uri[fname] = gn
        graph_files: list[str] = []
        cleanup_paths: list[Path] = []
        for graph_path, graph_name in graph_paths:
            is_named = bool(graph_name and graph_name != "-")
            if graph_path.endswith(".rdf"):
                turtle_data = rdf_xml_to_turtle(graph_path, graph_name)
                if is_named:
                    out_name = Path(graph_path).name.replace(".rdf", ".trig")
                    write_ttl_file(out_name, _graph_to_trig(turtle_data, graph_name))
                else:
                    out_name = Path(graph_path).name.replace(".rdf", ".ttl")
                    write_ttl_file(out_name, turtle_data)
                graph_files.append(out_name)
                cleanup_paths.append(workdir / out_name)
                continue
            src = Path(graph_path).resolve()
            if is_named:
                turtle_data = src.read_text(encoding="utf-8")
                out_name = src.stem + ".trig"
                write_ttl_file(out_name, _graph_to_trig(turtle_data, graph_name))
                graph_files.append(out_name)
                cleanup_paths.append(workdir / out_name)
                continue
            replacement = file_to_named_uri.get(src.name, cwd_uri)
            temp_name, temp_path = replace_empty_base_iri(src, workdir, replacement, "oxigraph")
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
