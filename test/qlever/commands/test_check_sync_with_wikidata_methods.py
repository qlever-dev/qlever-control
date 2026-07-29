import io
import unittest
import urllib.error
from unittest import mock

from rdflib import Graph, Literal, URIRef

from qlever.commands.check_sync_with_wikidata import (
    CheckSyncWithWikidataCommand,
)

XSD = "http://www.w3.org/2001/XMLSchema#"
WD = "http://www.wikidata.org/entity/"
WIKIBASE = "http://wikiba.se/ontology#"


def literal(lexical, datatype):
    return Literal(lexical, datatype=URIRef(f"{XSD}{datatype}"))


class TestCheckSyncWithWikidataCommand(unittest.TestCase):
    def setUp(self):
        self.command = CheckSyncWithWikidataCommand()

    def test_description(self):
        self.assertEqual(
            self.command.description(),
            "Check that entities on this endpoint are exactly in sync with"
            " wikidata.org, by comparing against `Special:EntityData`",
        )

    def test_should_have_qleverfile(self):
        self.assertTrue(self.command.should_have_qleverfile())

    def test_relevant_qleverfile_arguments(self):
        self.assertEqual(
            self.command.relevant_qleverfile_arguments(),
            {"server": ["host_name", "port"]},
        )

    def test_canonical_term_numeric(self):
        # The lexical form of a numeric literal is not preserved by the
        # index, so numbers are compared by value: leading `+` and trailing
        # zeros disappear, and the datatype is unified.
        self.assertEqual(
            self.command.canonical_term(literal("+190", "decimal")),
            '"190"^^NUM',
        )
        self.assertEqual(
            self.command.canonical_term(literal("190.0", "double")),
            '"190"^^NUM',
        )
        # Values are rounded to 10 significant digits (the index stores
        # decimals with limited precision and rounds slightly differently
        # than IEEE string parsing).
        self.assertEqual(
            self.command.canonical_term(
                literal("25.9816937839249", "decimal")
            ),
            self.command.canonical_term(literal("25.98169378392", "decimal")),
        )
        self.assertEqual(
            self.command.canonical_term(literal("168.73846826", "decimal")),
            self.command.canonical_term(literal("168.738468261", "decimal")),
        )
        # Large integers are kept exact.
        self.assertEqual(
            self.command.canonical_term(
                literal("123456789012345678", "integer")
            ),
            '"123456789012345678"^^NUM',
        )

    def test_fetch_canonical_retries_server_errors(self):
        # A transient HTTP 5xx on the download is retried once.
        error = urllib.error.HTTPError(
            "url", 500, "Internal Server Error", {}, io.BytesIO()
        )
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"ttl"
        response.__enter__.return_value.url = (
            "https://www.wikidata.org/wiki/Special:EntityData/Q42.ttl"
        )
        with (
            mock.patch(
                "urllib.request.urlopen", side_effect=[error, response]
            ),
            mock.patch("time.sleep"),
        ):
            ttl_bytes, redirected_to = self.command.fetch_canonical("Q42")
        self.assertEqual(ttl_bytes, b"ttl")
        self.assertIsNone(redirected_to)
        # A client error (4xx) is not retried.
        client_error = urllib.error.HTTPError(
            "url", 404, "Not Found", {}, io.BytesIO()
        )
        with (
            mock.patch("urllib.request.urlopen", side_effect=[client_error]),
            mock.patch("time.sleep"),
        ):
            with self.assertRaises(urllib.error.HTTPError):
                self.command.fetch_canonical("Q42")

    def test_canonical_term_datetime(self):
        # The export omits the timezone designator for years outside
        # [-9999, 9999], so dates are compared without it.
        self.assertEqual(
            self.command.canonical_term(
                literal("-11700-01-01T00:00:00Z", "dateTime")
            ),
            self.command.canonical_term(
                literal("-11700-01-01T00:00:00", "dateTime")
            ),
        )
        self.assertNotEqual(
            self.command.canonical_term(
                literal("2020-01-01T00:00:00Z", "dateTime")
            ),
            self.command.canonical_term(
                literal("2021-01-01T00:00:00Z", "dateTime")
            ),
        )

    def test_canonical_term_non_numeric(self):
        self.assertEqual(
            self.command.canonical_term(Literal("hello", lang="en")),
            '"hello"@en',
        )
        self.assertEqual(
            self.command.canonical_term(URIRef(f"{WD}Q42")),
            f"<{WD}Q42>",
        )

    def test_geo_values_match(self):
        # The observed encoding differences (up to 1e-5) must compare as
        # equal, a difference above the tolerance must not.
        key = "<http://www.wikidata.org/value/abc> <p>"
        self.assertTrue(
            self.command.geo_values_match(
                {key: [[5.483421, 50.642359]]},
                {key: [[5.48342, 50.642359]]},
            )
        )
        self.assertTrue(
            self.command.geo_values_match(
                {key: [[50.81588]]}, {key: [[50.81587]]}
            )
        )
        self.assertFalse(
            self.command.geo_values_match(
                {key: [[50.81588]]}, {key: [[50.816]]}
            )
        )
        self.assertFalse(self.command.geo_values_match({key: [[50.8]]}, {}))

    def test_normalize_triples_exclusions(self):
        graph = Graph()
        entity = URIRef(f"{WD}Q42")
        # An ordinary triple, an entity-level counter, a reference-type
        # triple, and a normalized-quantity link.
        graph.add(
            (
                entity,
                URIRef(f"{WD.replace('entity/', 'prop/direct/')}P31"),
                URIRef(f"{WD}Q5"),
            )
        )
        graph.add(
            (entity, URIRef(f"{WIKIBASE}sitelinks"), literal("5", "integer"))
        )
        graph.add(
            (
                URIRef("http://www.wikidata.org/reference/abc"),
                URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                URIRef(f"{WIKIBASE}Reference"),
            )
        )
        graph.add(
            (
                URIRef("http://www.wikidata.org/value/abc"),
                URIRef(f"{WIKIBASE}quantityNormalized"),
                URIRef("http://www.wikidata.org/value/def"),
            )
        )
        with_counters, _ = self.command.normalize_triples(graph, "Q42", False)
        without_counters, _ = self.command.normalize_triples(
            graph, "Q42", True
        )
        # The reference type and the normalized-quantity link are always
        # excluded; the counter only with `exclude_counters`.
        self.assertEqual(len(with_counters), 2)
        self.assertEqual(len(without_counters), 1)

    def test_normalize_triples_geo(self):
        # Geographic values enter the set comparison only via a placeholder
        # (so the presence of the triple is still compared exactly), and
        # the values are collected for the tolerance comparison.
        canonical, qlever = Graph(), Graph()
        value = URIRef("http://www.wikidata.org/value/abc")
        latitude = URIRef(f"{WIKIBASE}geoLatitude")
        canonical.add((value, latitude, literal("51.566527777778", "double")))
        qlever.add((value, latitude, literal("51.56652777778", "decimal")))
        canonical_lines, canonical_geo = self.command.normalize_triples(
            canonical, "Q42", False
        )
        qlever_lines, qlever_geo = self.command.normalize_triples(
            qlever, "Q42", False
        )
        self.assertEqual(canonical_lines, qlever_lines)
        self.assertTrue(
            self.command.geo_values_match(canonical_geo, qlever_geo)
        )

    def test_canonical_graph_without_munging(self):
        # The version and modification date are grafted from the document
        # node onto the entity, and the document node is dropped.
        ttl = f"""
        @prefix schema: <http://schema.org/> .
        @prefix wd: <{WD}> .
        @prefix xsd: <{XSD}> .
        <https://www.wikidata.org/wiki/Special:EntityData/Q42>
            schema:about wd:Q42 ;
            schema:version "123"^^xsd:integer ;
            schema:dateModified "2026-07-28T00:00:00Z"^^xsd:dateTime .
        wd:Q42 schema:name "Douglas Adams"@en .
        """
        graph, version = self.command.canonical_graph(ttl.encode(), "Q42")
        self.assertEqual(version, "123")
        self.assertEqual(self.command.entity_version(graph, "Q42"), "123")
        subjects = set(str(s) for s in graph.subjects())
        self.assertEqual(subjects, {f"{WD}Q42"})

    def test_extract_document(self):
        # The document of Q42 consists of its subject triples, its statement
        # nodes (also with the lowercase IRIs of old statements), the
        # references and values reachable from them, and the sitelink
        # article blocks with their wiki metadata; the statement of the
        # OTHER entity Q43 does not belong to it.
        turtle = f"""
        @prefix schema: <http://schema.org/> .
        @prefix wd: <{WD}> .
        @prefix wds: <http://www.wikidata.org/entity/statement/> .
        @prefix wdref: <http://www.wikidata.org/reference/> .
        @prefix wdv: <http://www.wikidata.org/value/> .
        @prefix p: <http://www.wikidata.org/prop/> .
        @prefix prov: <http://www.w3.org/ns/prov#> .
        wd:Q42 p:P31 wds:Q42-aaa , wds:q42-bbb .
        wds:Q42-aaa prov:wasDerivedFrom wdref:ref1 .
        wds:q42-bbb p:P2 wdv:value1 .
        wdref:ref1 p:P3 wdv:value2 .
        wdv:value1 p:P4 "x" .
        wdv:value2 p:P5 "y" .
        <https://en.wikipedia.org/wiki/A> schema:about wd:Q42 ;
            schema:isPartOf <https://en.wikipedia.org/> .
        <https://en.wikipedia.org/> p:P6 "wiki" .
        wd:Q43 p:P31 wds:Q43-ccc .
        wds:Q43-ccc p:P7 "other" .
        """
        graph = Graph()
        graph.parse(data=turtle, format="turtle")
        document = self.command.extract_document(graph, "Q42")
        subjects = set(str(s) for s in document.subjects())
        self.assertEqual(
            subjects,
            {
                f"{WD}Q42",
                "http://www.wikidata.org/entity/statement/Q42-aaa",
                "http://www.wikidata.org/entity/statement/q42-bbb",
                "http://www.wikidata.org/reference/ref1",
                "http://www.wikidata.org/value/value1",
                "http://www.wikidata.org/value/value2",
                "https://en.wikipedia.org/wiki/A",
                "https://en.wikipedia.org/",
            },
        )
        self.assertEqual(len(document), 10)


if __name__ == "__main__":
    unittest.main()
