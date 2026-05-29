import re
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.error import URLError
from urllib.parse import urlparse, parse_qs, unquote_plus
from typing import Dict, Optional

import rdflib


class MockSPARQLServer:
    """
    In-process HTTP server that answers SPARQL queries using rdflib.
    Used to mock remote SERVICE endpoints during federation test execution.

    Each registered endpoint URL is mapped to an rdflib Graph loaded from
    the TTL data specified in the test manifest's qt:serviceData.
    Routing uses a path key derived from the original URL, so multiple
    endpoints can share one server instance.
    """

    def __init__(self) -> None:
        self._endpoint_graphs: Dict[str, rdflib.ConjunctiveGraph] = {}
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.port: int = 0

    def _url_to_key(self, url: str) -> str:
        parsed = urlparse(url)
        return (parsed.netloc + parsed.path).replace("/", "_").strip("_")

    def add_endpoint(self, url: str, ttl_data: str) -> None:
        """Register an endpoint URL with the TTL data it should serve queries against."""
        g = rdflib.ConjunctiveGraph()
        g.parse(data=ttl_data, format="turtle")
        self._endpoint_graphs[self._url_to_key(url)] = g

    def local_url_for(self, url: str, host: str = "127.0.0.1") -> str:
        """Return the replacement URL for an original endpoint URL."""
        return f"http://{host}:{self.port}/{self._url_to_key(url)}"

    def start(self) -> None:
        """Start the server on an OS-assigned free port."""
        endpoint_graphs = self._endpoint_graphs
        mock_server = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                path_key = parsed.path.strip("/")
                query = unquote_plus(parse_qs(parsed.query).get("query", [""])[0])
                self._handle(path_key, query)

            def do_POST(self):
                content_type = self.headers.get("Content-Type", "")
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode("utf-8")
                path_key = urlparse(self.path).path.strip("/")
                if "application/sparql-query" in content_type:
                    query = body
                else:
                    query = unquote_plus(parse_qs(body).get("query", [""])[0])
                self._handle(path_key, query)

            def _handle(self, path_key: str, query: str) -> None:
                graph = endpoint_graphs.get(path_key)
                if graph is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                # Strip engine-specific DEFINE directives (e.g. Virtuoso adds
                # DEFINE input:default-graph-uri) that rdflib can't handle.
                query = re.sub(r'(?im)^DEFINE\s+\S+\s+\S+\s*$', '', query)
                # Rewrite any SERVICE URLs that point at this mock's own port
                # to 127.0.0.1 so rdflib can resolve them when the sub-query
                # contains a nested SERVICE (e.g. service6 in the W3C suite).
                port = mock_server.port
                query = re.sub(
                    rf'SERVICE\s*<[^>]*:{port}/([^>]*)>',
                    lambda m: f'SERVICE <http://127.0.0.1:{port}/{m.group(1)}>',
                    query,
                    flags=re.IGNORECASE,
                )
                try:
                    result = graph.query(query)
                    if result.type in ("SELECT", "ASK"):
                        accept = self.headers.get("Accept", "")
                        if "application/sparql-results+json" in accept:
                            body = result.serialize(format="json")
                            ct = "application/sparql-results+json"
                        else:
                            body = result.serialize(format="xml")
                            ct = "application/sparql-results+xml"
                        if isinstance(body, str):
                            body = body.encode("utf-8")
                    else:
                        result_graph = rdflib.Graph()
                        for triple in result:
                            result_graph.add(triple)
                        body = result_graph.serialize(format="turtle")
                        if isinstance(body, str):
                            body = body.encode("utf-8")
                        ct = "text/turtle"
                    self.send_response(200)
                    self.send_header("Content-Type", ct)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (URLError, OSError):
                    # rdflib tried to execute a nested SERVICE clause and got a
                    # network error (e.g. host.docker.internal not resolvable on
                    # the host).  Return empty results so the calling engine can
                    # apply its own SERVICE SILENT / error semantics.
                    accept = self.headers.get("Accept", "")
                    if "application/sparql-results+json" in accept:
                        body = b'{"head":{"vars":[]},"results":{"bindings":[]}}'
                        ct = "application/sparql-results+json"
                    else:
                        body = (b'<?xml version="1.0"?>'
                                b'<sparql xmlns="http://www.w3.org/2005/sparql-results#">'
                                b'<head/><results/></sparql>')
                        ct = "application/sparql-results+xml"
                    self.send_response(200)
                    self.send_header("Content-Type", ct)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as exc:
                    err = str(exc).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(err)))
                    self.end_headers()
                    self.wfile.write(err)

            def log_message(self, fmt, *args) -> None:
                pass  # suppress access log noise during tests

        server = ThreadingHTTPServer(("0.0.0.0", 0), _Handler)
        self.port = server.server_address[1]
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Shut down the server and wait for the thread to exit."""
        if self._server is not None:
            self._server.shutdown()
            if self._thread is not None:
                self._thread.join()
            self._server = None
            self._thread = None
            self.port = 0
