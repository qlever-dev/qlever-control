import unittest

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

    def test_canonical_term_non_numeric(self):
        self.assertEqual(
            self.command.canonical_term(Literal("hello", lang="en")),
            '"hello"@en',
        )
        self.assertEqual(
            self.command.canonical_term(URIRef(f"{WD}Q42")),
            f"<{WD}Q42>",
        )

    def test_canonical_wkt(self):
        # Coordinates are rounded to 6 decimal places and the keyword is
        # uppercased, matching the fixed-precision encoding of QLever.
        self.assertEqual(
            self.command.canonical_wkt(
                "Point(-0.14544444444444 51.566527777778)"
            ),
            self.command.canonical_wkt("POINT(-0.145444 51.566528)"),
        )

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
        with_counters = self.command.normalize_triples(graph, "Q42", False)
        without_counters = self.command.normalize_triples(graph, "Q42", True)
        # The reference type and the normalized-quantity link are always
        # excluded; the counter only with `exclude_counters`.
        self.assertEqual(len(with_counters), 2)
        self.assertEqual(len(without_counters), 1)

    def test_normalize_triples_geo(self):
        # The two lexical forms of the same coordinate (canonical vs. the
        # export of the fixed-precision encoding) must compare equal.
        canonical, qlever = Graph(), Graph()
        value = URIRef("http://www.wikidata.org/value/abc")
        latitude = URIRef(f"{WIKIBASE}geoLatitude")
        canonical.add((value, latitude, literal("51.566527777778", "double")))
        qlever.add((value, latitude, literal("51.56652777778", "decimal")))
        self.assertEqual(
            self.command.normalize_triples(canonical, "Q42", False),
            self.command.normalize_triples(qlever, "Q42", False),
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
        graph, version = self.command.canonical_graph(
            ttl.encode(), "Q42", None, None
        )
        self.assertEqual(version, "123")
        self.assertEqual(self.command.entity_version(graph, "Q42"), "123")
        subjects = set(str(s) for s in graph.subjects())
        self.assertEqual(subjects, {f"{WD}Q42"})


if __name__ == "__main__":
    unittest.main()
