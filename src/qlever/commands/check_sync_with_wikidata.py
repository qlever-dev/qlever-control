from __future__ import annotations

import glob
import gzip
import json
import logging
import random
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from tqdm.contrib.logging import tqdm_logging_redirect

from qlever.command import QleverCommand
from qlever.log import log

# User agent for all requests to wikidata.org (required by the API etiquette).
USER_AGENT = "qlever-check-sync/0.1 (https://github.com/qlever-dev/qlever)"

WD = "http://www.wikidata.org/entity/"
SCHEMA = "http://schema.org/"
WIKIBASE = "http://wikiba.se/ontology#"
STATEMENT = "http://www.wikidata.org/entity/statement/"
REFERENCE = "http://www.wikidata.org/reference/"
VALUE = "http://www.wikidata.org/value/"
ONTOLEX = "http://www.w3.org/ns/lemon/ontolex#"

# The IRI prefix of the `Special:EntityData` document node. For items, the
# munging moves its `schema:version` and `schema:dateModified` onto the
# entity and drops the node. The lexemes dump is ingested UNMUNGED (see the
# Wikidata Qleverfile), so for lexemes the index contains this node, and it
# goes stale when the entity is updated via the stream (the updater knows
# nothing about it). It is therefore excluded from the comparison and used
# only as a fallback for the version gate.
DOCUMENT = "https://www.wikidata.org/wiki/Special:EntityData/"

# Entity-level counter triples that are contained in the full dump (and hence
# in the index), but NOT in the output of `Special:EntityData`. They are
# excluded from the comparison on both sides.
EXCLUDED_ENTITY_PREDICATES = {
    f"{WIKIBASE}sitelinks",
    f"{WIKIBASE}statements",
    f"{WIKIBASE}identifiers",
}

# `munge.sh` drops the `rdf:type wikibase:Reference` triples from the dump,
# but the update stream contains them, so an index that has received updates
# has them for some references and not for others. They carry no information
# (every `wdref:` node is a reference), so they are excluded on both sides.
# The same heterogeneity exists for the `wikibase:quantityNormalized` links.
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
EXCLUDED_TYPE_OBJECTS = {f"{WIKIBASE}Reference"}
EXCLUDED_PREDICATES = {f"{WIKIBASE}quantityNormalized"}

# A lexeme in the index is heterogeneous in a second way: loaded from the
# UNMUNGED lexemes dump, but updated via the munged flavor of the stream.
# The munging drops the `rdfs:label` triples of a lexeme document (they
# duplicate `wikibase:lemma`, `ontolex:representation`, and
# `skos:definition`) and the `rdf:type` triples below (they duplicate the
# `ontolex:` types, or carry no information in the case of
# `wikibase:Statement`). A stream-touched lexeme therefore lacks them for
# the touched parts and keeps them for the rest, so they are excluded from
# the comparison of a lexeme on both sides.
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
LEXEME_EXCLUDED_TYPE_OBJECTS = {
    f"{WIKIBASE}Lexeme",
    f"{WIKIBASE}Form",
    f"{WIKIBASE}Sense",
    f"{WIKIBASE}Statement",
}

# Geographic coordinates are stored by QLever in a fixed-precision encoding
# and exported in a normalized form; differences up to 1e-5 degrees have
# been observed (5.483421 came back as 5.48342, and 50.81588 as 50.81587).
# Rounding cannot absorb such differences reliably (whatever the number of
# decimal places, an encoding error can straddle a rounding boundary), so
# the geographic values are excluded from the exact set comparison and
# compared separately with the following tolerance.
GEO_COMPONENT_PREDICATES = {
    f"{WIKIBASE}geoLatitude",
    f"{WIKIBASE}geoLongitude",
    f"{WIKIBASE}geoPrecision",
}
WKT_DATATYPE = "http://www.opengis.net/ont/geosparql#wktLiteral"
GEO_TOLERANCE = 2e-5

# Defining-formula literals (P2534 and friends) are MathML RENDERED by
# MediaWiki's Math extension, and extension updates change the rendered
# markup retroactively without an entity edit (observed 2026-08-20 on
# Q96843171: "mathjax_ignore" appeared in the class list of live
# EntityData, while the dump-era index has the old rendering). The class
# list of such literals is therefore normalized before the comparison.
MATHML_DATATYPE = "http://www.w3.org/1998/Math/MathML"
MATHML_CLASS_RE = re.compile(r'class="mwe-math-element[^"]*"')

# The redirect marker written by a Wikidata merge. An entity can become a
# redirect DURING the check (observed live); it is then reported as a
# redirect, like the redirects that are already detected at download time.
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"


class CheckSyncWithWikidataCommand(QleverCommand):
    """
    Class for executing the `check-sync-with-wikidata` command.
    """

    def __init__(self):
        # Width for `qid`, set in `execute` once the entities are known.
        self.qid_width = 0

    def description(self) -> str:
        return (
            "Check that entities on this endpoint are exactly in sync with"
            " wikidata.org, by comparing against `Special:EntityData`"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            help="URL of the QLever server,"
            " default is http://{host_name}:{port}",
        )
        subparser.add_argument(
            "--entities",
            help="Comma-separated list of entity IDs to check"
            " (default: a random sample, see `--num-entities`)",
        )
        subparser.add_argument(
            "--num-entities",
            type=int,
            default=10,
            help="Number of randomly sampled entities to check (default: 10)",
        )
        subparser.add_argument(
            "--recent-fraction",
            type=float,
            default=0.5,
            help="Fraction of the sample drawn from recently edited entities"
            " (they exercise the update path); the rest is drawn uniformly"
            " (default: 0.5)",
        )
        subparser.add_argument(
            "--lexeme-fraction",
            type=float,
            default=0.2,
            help="Fraction of the sample drawn from lexemes; they are"
            " deliberately oversampled relative to their share of the"
            " entities, because they take a separate path into the index"
            " (unmunged dump) that would otherwise go untested"
            " (default: 0.2)",
        )
        subparser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Seed for the random sample (default: 42)",
        )
        subparser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help="Number of entities munged together in one run of"
            " `munge.sh` (default: 50); this amortizes the JVM startup,"
            " which dominates the cost of checking a single entity",
        )
        subparser.add_argument(
            "--munge",
            choices=["auto", "yes", "no"],
            default="auto",
            help="Munge the canonical data with the `munge.sh` from the"
            " `service-*` directory before comparing (`auto`: munge if such"
            " a directory exists, which is the right thing for an index"
            " built from the munged dump)",
        )
        subparser.add_argument(
            "--keep-files",
            action="store_true",
            default=False,
            help="Keep the downloaded and munged files for inspection",
        )

    # SPARQL helpers.

    def sparql(self, endpoint, query, accept):
        data = urllib.parse.urlencode({"query": query}).encode()
        request = urllib.request.Request(
            endpoint,
            data=data,
            headers={
                "Accept": accept,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode()

    def qlever_entity_graph(self, endpoint, entity_id):
        """
        Fetch the full document of the given entity from the QLever endpoint:
        the triples with the entity as subject, the statement nodes reachable
        from the entity (plus their references and values), and the sitelink
        article blocks. For a lexeme, the document also includes its forms
        and senses, with their own statements; they are reached via the
        zero-or-one property path below, which for an item simply yields the
        item itself. The `schema:about` query also fetches the
        `Special:EntityData` document node of a lexeme, which is needed for
        the version gate (see `entity_version`) but excluded from the
        comparison (see `normalize_triples`).
        """
        e = f"<{WD}{entity_id}>"
        root = f"{e} (<{ONTOLEX}lexicalForm>|<{ONTOLEX}sense>)? ?root ."
        # NOTE: Statement IRIs of old statements contain the entity ID in
        # lowercase. The IRIs of the statements of a form or sense start
        # with the ID of the lexeme, so the prefix below covers them too.
        st = (
            f'FILTER(STRSTARTS(STR(?st), "{STATEMENT}{entity_id}-")'
            f' || STRSTARTS(STR(?st), "{STATEMENT}{entity_id.lower()}-"))'
        )
        queries = [
            f"CONSTRUCT {{ ?root ?p ?o }} WHERE {{ {root} ?root ?p ?o }}",
            f"CONSTRUCT {{ ?st ?p2 ?o2 }} WHERE"
            f" {{ {root} ?root ?p1 ?st . {st} ?st ?p2 ?o2 }}",
            f"CONSTRUCT {{ ?x ?p3 ?o3 }} WHERE"
            f" {{ {root} ?root ?p1 ?st . {st} ?st ?p2 ?x ."
            f' FILTER(STRSTARTS(STR(?x), "http://www.wikidata.org/reference/")'
            f' || STRSTARTS(STR(?x), "http://www.wikidata.org/value/"))'
            f" ?x ?p3 ?o3 }}",
            f"CONSTRUCT {{ ?x2 ?p5 ?o5 }} WHERE"
            f" {{ {root} ?root ?p1 ?st . {st} ?st ?p2 ?x ."
            f' FILTER(STRSTARTS(STR(?x), "http://www.wikidata.org/reference/"))'
            f" ?x ?p3 ?x2 ."
            f' FILTER(STRSTARTS(STR(?x2), "http://www.wikidata.org/value/"))'
            f" ?x2 ?p5 ?o5 }}",
            f"CONSTRUCT {{ ?a ?p4 ?o4 }} WHERE"
            f" {{ ?a <{SCHEMA}about> {e} . ?a ?p4 ?o4 }}",
            f"CONSTRUCT {{ ?w ?p6 ?o6 }} WHERE"
            f" {{ ?a <{SCHEMA}about> {e} ."
            f" ?a <{SCHEMA}isPartOf> ?w . ?w ?p6 ?o6 }}",
        ]
        graph = Graph()
        for query in queries:
            turtle = self.sparql(endpoint, query, "text/turtle")
            graph.parse(data=turtle, format="turtle")
        return graph

    # Canonical data from wikidata.org.

    def fetch_canonical(self, entity_id):
        """
        Download the canonical TTL for the entity from `Special:EntityData`.
        Returns `(ttl_bytes, redirected_to)`, where `redirected_to` is not
        `None` if the entity is a redirect. A transient server error
        (HTTP 5xx) is retried once.
        """
        url = (
            "https://www.wikidata.org/wiki/Special:EntityData/"
            f"{entity_id}.ttl?flavor=dump"
        )
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT}
        )
        for attempt in (1, 2):
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    body = response.read()
                    final_url = response.url
                break
            except urllib.error.HTTPError as e:
                if attempt == 1 and e.code >= 500:
                    time.sleep(5)
                    continue
                raise
        match = re.search(r"EntityData/([QL]\d+)", final_url)
        redirected_to = None
        if match and match.group(1) != entity_id:
            redirected_to = match.group(1)
        return body, redirected_to

    def canonical_graph(self, ttl_bytes, entity_id):
        """
        Parse the canonical TTL and normalize it like `munge.sh` would, for
        a comparison WITHOUT munging: graft the version and modification date
        from the document node onto the entity and drop the document node.
        Unlike `munge.sh`, this cannot compute the entity-level counters,
        which is why they are excluded from the comparison in this mode (see
        `EXCLUDED_ENTITY_PREDICATES`). Returns `(graph, version)`.
        """
        graph = Graph()
        graph.parse(data=ttl_bytes, format="turtle")
        entity = URIRef(f"{WD}{entity_id}")
        doc_nodes = set(
            s for s in graph.subjects() if "Special:EntityData" in str(s)
        )
        version = None
        for doc in doc_nodes:
            for p, o in list(graph.predicate_objects(doc)):
                if str(p) in (f"{SCHEMA}version", f"{SCHEMA}dateModified"):
                    graph.add((entity, p, o))
                    if str(p) == f"{SCHEMA}version":
                        version = str(o)
                graph.remove((doc, p, o))
        return graph, version

    def extract_document(self, graph, entity_id):
        """
        Extract the document of the given entity from the given graph: the
        triples with the entity as subject, the statement nodes of the
        entity (recognized by their IRI prefix), the references and values
        reachable from those statements, the sitelink article blocks, and
        the wiki metadata. This mirrors exactly the queries of
        `qlever_entity_graph`, so that the two sides of the comparison cover
        the same universe. It is what makes munging in batches possible: the
        munged output of a batch is one graph without entity boundaries, and
        this reconstructs them (references and values are shared between
        entities and are assigned to every entity that reaches them, on both
        sides).
        """
        entity = URIRef(f"{WD}{entity_id}")
        statement_prefixes = (
            f"{STATEMENT}{entity_id}-",
            f"{STATEMENT}{entity_id.lower()}-",
        )
        document = Graph()
        statements = set()
        # The document of a lexeme also includes its forms and senses, with
        # their own statements (their statement IRIs start with the ID of
        # the lexeme, so the prefixes above cover them).
        roots = {entity}
        for link in (f"{ONTOLEX}lexicalForm", f"{ONTOLEX}sense"):
            roots |= set(graph.objects(entity, URIRef(link)))
        for root in roots:
            for p, o in graph.predicate_objects(root):
                document.add((root, p, o))
                if isinstance(o, URIRef) and str(o).startswith(
                    statement_prefixes
                ):
                    statements.add(o)
        references_and_values = set()
        for statement in statements:
            for p, o in graph.predicate_objects(statement):
                document.add((statement, p, o))
                if isinstance(o, URIRef) and str(o).startswith(
                    (REFERENCE, VALUE)
                ):
                    references_and_values.add(o)
        # References can point to values.
        for node in list(references_and_values):
            for _, o in graph.predicate_objects(node):
                if isinstance(o, URIRef) and str(o).startswith(VALUE):
                    references_and_values.add(o)
        for node in references_and_values:
            for p, o in graph.predicate_objects(node):
                document.add((node, p, o))
        wikis = set()
        for article in graph.subjects(URIRef(f"{SCHEMA}about"), entity):
            for p, o in graph.predicate_objects(article):
                document.add((article, p, o))
                if str(p) == f"{SCHEMA}isPartOf":
                    wikis.add(o)
        for wiki in wikis:
            for p, o in graph.predicate_objects(wiki):
                document.add((wiki, p, o))
        return document

    def munge(self, ttl_bytes, munge_script, keep_dir):
        """
        Run the given `munge.sh` on the given canonical TTL and return the
        parsed result.
        """
        workdir = Path(tempfile.mkdtemp(prefix="qlever-check-sync."))
        try:
            input_path = workdir / "input.ttl"
            input_path.write_bytes(ttl_bytes)
            result = subprocess.run(
                [
                    str(munge_script),
                    "-f",
                    str(input_path),
                    "-d",
                    str(workdir),
                    "-c",
                    "150000000",
                    "--",
                    "--skolemize",
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            output_path = workdir / "wikidump-000000001.ttl.gz"
            if result.returncode != 0 or not output_path.exists():
                raise Exception(
                    f"munge.sh failed (exit code {result.returncode}):"
                    f" {result.stderr.strip()[-500:]}"
                )
            munged = Graph()
            with gzip.open(output_path, "rt") as f:
                munged.parse(data=f.read(), format="turtle")
            return munged
        finally:
            if keep_dir is not None:
                shutil.copytree(
                    workdir, keep_dir / workdir.name, dirs_exist_ok=True
                )
            shutil.rmtree(workdir, ignore_errors=True)

    # Sampling via the MediaWiki API.

    def mediawiki_api(self, params):
        url = "https://www.wikidata.org/w/api.php?" + urllib.parse.urlencode(
            {**params, "format": "json"}
        )
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def sample_namespace(
        self, num_entities, recent_fraction, rng, namespace, title_pattern
    ):
        """
        Sample entity IDs from the given namespace (items live in namespace
        0 with titles like `Q42`, lexemes in namespace 146 with titles like
        `Lexeme:L42`); the ID is the first group of `title_pattern`.
        """
        num_recent = round(num_entities * recent_fraction)
        num_uniform = num_entities - num_recent
        entities = []
        if num_recent > 0:
            result = self.mediawiki_api(
                {
                    "action": "query",
                    "list": "recentchanges",
                    "rcnamespace": namespace,
                    "rctype": "edit",
                    "rclimit": "500",
                }
            )
            titles = sorted(
                set(
                    match.group(1)
                    for rc in result["query"]["recentchanges"]
                    if (match := re.fullmatch(title_pattern, rc["title"]))
                )
            )
            entities += rng.sample(titles, min(num_recent, len(titles)))
        while len(entities) < num_recent + num_uniform:
            result = self.mediawiki_api(
                {
                    "action": "query",
                    "list": "random",
                    "rnnamespace": namespace,
                    "rnlimit": str(
                        min(20, num_recent + num_uniform - len(entities))
                    ),
                }
            )
            entities += [
                match.group(1)
                for r in result["query"]["random"]
                if (match := re.fullmatch(title_pattern, r["title"]))
                and match.group(1) not in entities
            ]
            time.sleep(1)
        return entities

    def sample_entities(
        self, num_entities, recent_fraction, lexeme_fraction, seed
    ):
        rng = random.Random(seed)
        num_lexemes = round(num_entities * lexeme_fraction)
        entities = self.sample_namespace(
            num_entities - num_lexemes, recent_fraction, rng, "0", r"(Q\d+)"
        )
        entities += self.sample_namespace(
            num_lexemes, recent_fraction, rng, "146", r"Lexeme:(L\d+)"
        )
        # Mix the recently edited and the uniformly drawn entities (and the
        # items and lexemes), so that the intermediate results are a mix.
        rng.shuffle(entities)
        return entities

    # Comparison.

    def canonical_term(self, term):
        """
        Return the N-Triples form of the term, with numeric literals in a
        canonical form. This is needed because a numeric literal is stored by
        the index as a value with limited precision, not as a lexical form:
        `"+190"^^xsd:decimal` is exported as `"190"^^xsd:decimal`,
        `"25.9816937839249"^^xsd:decimal` as `"25.98169378392"^^xsd:decimal`,
        and a decimal can even come back as `xsd:double`. Integers are kept
        exact; all other numbers are rounded to 10 significant digits, with a
        unified datatype marker (the encoding used by the index rounds
        slightly differently than IEEE string parsing, with differences
        observed in the 12th significant digit).
        """
        # QLever's export omits the timezone designator for dates with
        # years outside [-9999, 9999] ("-11700-01-01T00:00:00" instead of
        # "-11700-01-01T00:00:00Z"), so compare dates without it (all times
        # in Wikidata are UTC, so it carries no information here).
        if (
            isinstance(term, Literal)
            and term.datatype is not None
            and str(term.datatype)
            == "http://www.w3.org/2001/XMLSchema#dateTime"
        ):
            return f'"{str(term).rstrip("Z")}"^^DATE'
        # See the comment at `MATHML_DATATYPE`.
        if (
            isinstance(term, Literal)
            and term.datatype is not None
            and str(term.datatype) == MATHML_DATATYPE
        ):
            normalized = MATHML_CLASS_RE.sub(
                'class="mwe-math-element"', str(term)
            )
            return f"{json.dumps(normalized)}^^MATHML"
        numeric_datatypes = {
            "http://www.w3.org/2001/XMLSchema#decimal",
            "http://www.w3.org/2001/XMLSchema#integer",
            "http://www.w3.org/2001/XMLSchema#double",
            "http://www.w3.org/2001/XMLSchema#float",
        }
        if (
            isinstance(term, Literal)
            and term.datatype is not None
            and str(term.datatype) in numeric_datatypes
        ):
            try:
                value = Decimal(str(term))
                if value == value.to_integral_value() and abs(value) < 2**60:
                    lexical = str(int(value))
                else:
                    lexical = format(float(value), ".10g")
                return f'"{lexical}"^^NUM'
            except (InvalidOperation, OverflowError):
                pass
        return term.n3()

    def normalize_triples(self, graph, entity_id, exclude_counters):
        """
        Return `(lines, geo_values)`: the set of N-Triples lines of the
        graph, and the geographic values, which take part in the set
        comparison only via a placeholder (so that the PRESENCE of each such
        triple is still compared exactly) and whose values are compared
        separately with a tolerance (see `GEO_TOLERANCE`). Without munging,
        the entity-level counter triples are excluded (see
        `EXCLUDED_ENTITY_PREDICATES`); with munging, the munge script
        computes them on the canonical side, so they are compared like all
        others.
        """
        entity = f"{WD}{entity_id}"
        is_lexeme = entity_id.startswith("L")
        lines = set()
        geo_values = {}
        for s, p, o in graph:
            # The `Special:EntityData` document node of a lexeme is in the
            # index (unmunged dump), but goes stale on the first update via
            # the stream, so it takes part only in the version gate.
            if str(s).startswith(DOCUMENT):
                continue
            # A lexeme that was updated via the stream has the version and
            # modification date grafted onto the entity (munged flavor of
            # the stream), an untouched one does not; both are in sync, so
            # this heterogeneity is excluded from the comparison.
            if (
                is_lexeme
                and str(s) == entity
                and str(p) in (f"{SCHEMA}version", f"{SCHEMA}dateModified")
            ):
                continue
            # See the comment at `LEXEME_EXCLUDED_TYPE_OBJECTS`.
            if is_lexeme and (
                str(p) == RDFS_LABEL
                or (
                    str(p) == RDF_TYPE
                    and str(o) in LEXEME_EXCLUDED_TYPE_OBJECTS
                )
            ):
                continue
            if (
                exclude_counters
                and str(s) == entity
                and str(p) in EXCLUDED_ENTITY_PREDICATES
            ):
                continue
            if str(p) == RDF_TYPE and str(o) in EXCLUDED_TYPE_OBJECTS:
                continue
            if str(p) in EXCLUDED_PREDICATES:
                continue
            is_geo_component = str(
                p
            ) in GEO_COMPONENT_PREDICATES and isinstance(o, Literal)
            is_wkt = (
                isinstance(o, Literal)
                and o.datatype is not None
                and str(o.datatype) == WKT_DATATYPE
            )
            if is_geo_component or is_wkt:
                object_string = "GEO"
                numbers = [
                    float(match.group(0))
                    for match in re.finditer(r"-?\d+(\.\d+)?", str(o))
                ]
                geo_values.setdefault(f"{s.n3()} {p.n3()}", []).append(numbers)
            else:
                object_string = self.canonical_term(o)
            lines.add(f"{s.n3()} {p.n3()} {object_string}")
        return lines, geo_values

    def geo_values_match(self, canonical_values, qlever_values):
        """
        Compare the two dicts of geographic values (as returned by
        `normalize_triples`): for each subject and predicate, each list of
        numbers on the one side must have a counterpart on the other side
        whose numbers are all within `GEO_TOLERANCE`.
        """
        if set(canonical_values) != set(qlever_values):
            return False

        def close(numbers_1, numbers_2):
            return len(numbers_1) == len(numbers_2) and all(
                abs(a - b) <= GEO_TOLERANCE
                for a, b in zip(numbers_1, numbers_2)
            )

        for key, canonical_list in canonical_values.items():
            qlever_list = list(qlever_values[key])
            if len(canonical_list) != len(qlever_list):
                return False
            for numbers in canonical_list:
                counterpart = next(
                    (q for q in qlever_list if close(numbers, q)), None
                )
                if counterpart is None:
                    return False
                qlever_list.remove(counterpart)
        return True

    def entity_version(self, graph, entity_id):
        """
        Return the `schema:version` of the given entity in the given graph.
        For a lexeme that was loaded from the (unmunged) dump and never
        updated since, the version sits on the `Special:EntityData` document
        node instead of the entity; once the entity is updated via the
        stream, the version on the entity is the current one and the one on
        the document node is stale, which is why the entity is tried first.
        """
        for subject in (f"{WD}{entity_id}", f"{DOCUMENT}{entity_id}"):
            for o in graph.objects(
                URIRef(subject), URIRef(f"{SCHEMA}version")
            ):
                return str(o)
        return None

    def entity_date_modified(self, graph, entity_id):
        """
        Return the `schema:dateModified` of the given entity in the given
        graph (like `entity_version`, with the document node as fallback).
        """
        for subject in (f"{WD}{entity_id}", f"{DOCUMENT}{entity_id}"):
            for o in graph.objects(
                URIRef(subject), URIRef(f"{SCHEMA}dateModified")
            ):
                return str(o)
        return None

    def qid(self, entity_id):
        """
        Return `<entity_id>:`, right-padded so that the log lines of the
        different entities of a run are aligned (the padding width is set
        in `execute` to the longest sampled entity ID).
        """
        return f"{entity_id + ':':<{self.qid_width + 1}}"

    def compare_entity(
        self, entity_id, qlever_graph, canonical_document, exclude_counters
    ):
        """
        Compare the two documents of the given entity. Returns one of
        `match`, `divergent`, `undecidable`, or `error`.
        """
        for graph in (qlever_graph, canonical_document):
            for target in graph.objects(
                URIRef(f"{WD}{entity_id}"), URIRef(OWL_SAMEAS)
            ):
                log.info(
                    f"{self.qid(entity_id)} redirect to"
                    f" {str(target).rsplit('/', 1)[-1]}, skipped"
                )
                return "redirect"
        canonical_version = self.entity_version(canonical_document, entity_id)
        qlever_version = self.entity_version(qlever_graph, entity_id)
        if canonical_version is None or qlever_version is None:
            log.warning(
                f"{self.qid(entity_id)} could not determine version"
                f" (canonical: {canonical_version},"
                f" endpoint: {qlever_version})"
            )
            return "error"
        last_updated = self.entity_date_modified(
            canonical_document, entity_id
        ) or self.entity_date_modified(qlever_graph, entity_id)
        if canonical_version != qlever_version:
            log.info(
                f"{self.qid(entity_id)} version mismatch (canonical:"
                f" {canonical_version}, endpoint: {qlever_version}),"
                f" edited since the endpoint's stream position"
            )
            return "undecidable"
        canonical, canonical_geo = self.normalize_triples(
            canonical_document, entity_id, exclude_counters
        )
        qlever, qlever_geo = self.normalize_triples(
            qlever_graph, entity_id, exclude_counters
        )
        missing = canonical - qlever
        extra = qlever - canonical
        if not missing and not extra:
            if not self.geo_values_match(canonical_geo, qlever_geo):
                log.error(
                    f"{self.qid(entity_id)} DIVERGENT at version"
                    f" {qlever_version}, last updated {last_updated}"
                    f" (geographic values differ by more than"
                    f" {GEO_TOLERANCE})"
                )
                return "divergent"
            log.info(
                f"{self.qid(entity_id)} exact match at version"
                f" {qlever_version}, last updated {last_updated}"
                f" ({len(qlever):>7,} triples)"
            )
            return "match"
        log.error(
            f"{self.qid(entity_id)} DIVERGENT at version {qlever_version},"
            f" last updated {last_updated}"
            f" ({len(missing)} triples missing on the endpoint,"
            f" {len(extra)} extra)"
        )
        for line in sorted(missing)[:5]:
            log.error(f"  missing: {line}")
        for line in sorted(extra)[:5]:
            log.error(f"  extra:   {line}")
        return "divergent"

    def check_batch(self, batch, endpoint, munge_script, keep_dir, pbar=None):
        """
        Check a batch of entities: download the canonical data and query the
        endpoint pairwise (so that the version gate has the best chance),
        then munge the whole batch in ONE run of `munge.sh`, and compare
        entity by entity. Returns a dict from entity ID to outcome. The
        progress bar `pbar` is advanced once per downloaded entity (the
        download loop dominates the running time).
        """
        outcomes = {}
        snapshots = []
        for entity_id in batch:
            try:
                ttl_bytes, redirected_to = self.fetch_canonical(entity_id)
                if redirected_to is None and b"sameAs" in ttl_bytes:
                    # A recently merged entity is served as a redirect stub
                    # (no HTTP redirect yet). It must not go into the munged
                    # batch: its dangling document node makes `munge.sh`
                    # graft its version onto the NEXT entity of the batch.
                    stub = Graph()
                    stub.parse(data=ttl_bytes, format="turtle")
                    target = next(
                        iter(
                            stub.objects(
                                URIRef(f"{WD}{entity_id}"),
                                URIRef(OWL_SAMEAS),
                            )
                        ),
                        None,
                    )
                    if target is not None:
                        redirected_to = str(target).rsplit("/", 1)[-1]
                if redirected_to is not None:
                    log.info(
                        f"{self.qid(entity_id)} redirect to"
                        f" {redirected_to}, skipped"
                    )
                    outcomes[entity_id] = "redirect"
                else:
                    qlever_graph = self.qlever_entity_graph(
                        endpoint, entity_id
                    )
                    snapshots.append((entity_id, ttl_bytes, qlever_graph))
            except Exception as e:
                log.warning(f"{self.qid(entity_id)} check failed ({e})")
                outcomes[entity_id] = "error"
            if pbar is not None:
                pbar.update(1)
            time.sleep(1)
        # Lexemes are ingested into the index from the UNMUNGED lexemes dump
        # (see the Wikidata Qleverfile), so their canonical data is compared
        # without munging, whatever `--munge` says.
        batch_graph = None
        munge_failed = False
        if munge_script is not None:
            munge_batch = [
                (entity_id, ttl)
                for entity_id, ttl, _ in snapshots
                if not entity_id.startswith("L")
            ]
            if munge_batch:
                try:
                    batch_graph = self.munge(
                        b"".join(ttl for _, ttl in munge_batch),
                        munge_script,
                        keep_dir,
                    )
                except Exception as e:
                    log.warning(f"Munging the batch failed ({e})")
                    munge_failed = True
        for entity_id, ttl_bytes, qlever_graph in snapshots:
            is_lexeme = entity_id.startswith("L")
            if munge_failed and not is_lexeme:
                outcomes[entity_id] = "error"
                continue
            try:
                if batch_graph is not None and not is_lexeme:
                    canonical_document = self.extract_document(
                        batch_graph, entity_id
                    )
                    exclude_counters = False
                else:
                    graph, _ = self.canonical_graph(ttl_bytes, entity_id)
                    canonical_document = self.extract_document(
                        graph, entity_id
                    )
                    exclude_counters = True
                outcomes[entity_id] = self.compare_entity(
                    entity_id,
                    qlever_graph,
                    canonical_document,
                    exclude_counters,
                )
            except Exception as e:
                log.warning(f"{self.qid(entity_id)} check failed ({e})")
                outcomes[entity_id] = "error"
        return outcomes

    def execute(self, args) -> bool:
        endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        if "://" not in endpoint:
            endpoint = f"http://{endpoint}"
        munge_scripts = sorted(glob.glob("service-*/munge.sh"))
        if args.munge == "yes" and not munge_scripts:
            log.error(
                "`--munge yes` was given, but no `service-*/munge.sh` was"
                " found in the current directory"
            )
            return False
        munge_script = None
        if munge_scripts and args.munge in ("auto", "yes"):
            munge_script = Path(munge_scripts[-1]).resolve()
        description = (
            f"Check entities on {endpoint} against wikidata.org"
            f" (munge: {munge_script or 'no'})"
        )
        self.show(description, only_show=args.show)
        if args.show:
            return True

        keep_dir = Path.cwd() if args.keep_files else None
        for name in ("recent_fraction", "lexeme_fraction"):
            if not 0.0 <= getattr(args, name) <= 1.0:
                option = name.replace("_", "-")
                log.error(f"`--{option}` must be between 0.0 and 1.0")
                return False
        if args.entities:
            entities = [e.strip() for e in args.entities.split(",")]
            invalid = [e for e in entities if not re.fullmatch(r"[QL]\d+", e)]
            if invalid:
                log.error(f"Invalid entity IDs: {invalid}")
                return False
        else:
            log.info(
                f"Sampling {args.num_entities} entities"
                f" ({args.recent_fraction:.0%} recently edited,"
                f" {args.lexeme_fraction:.0%} lexemes,"
                f" seed {args.seed}) ..."
            )
            entities = self.sample_entities(
                args.num_entities,
                args.recent_fraction,
                args.lexeme_fraction,
                args.seed,
            )
        log.info(f"Entities: {', '.join(entities)}")
        self.qid_width = max(len(e) for e in entities)

        if args.batch_size < 1:
            log.error("`--batch-size` must be at least 1")
            return False

        def batches(entity_ids):
            for i in range(0, len(entity_ids), args.batch_size):
                yield entity_ids[i : i + args.batch_size]

        outcomes = {}
        with tqdm_logging_redirect(
            loggers=[logging.getLogger("qlever")],
            desc="Entities",
            total=len(entities),
            leave=False,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}{postfix}",
        ) as pbar:
            for batch in batches(entities):
                outcomes.update(
                    self.check_batch(
                        batch, endpoint, munge_script, keep_dir, pbar
                    )
                )
        retry = [e for e, o in outcomes.items() if o == "undecidable"]
        if retry:
            log.info(
                f"Retrying {len(retry)} entities that were edited"
                f" during the check ..."
            )
            time.sleep(5)
            with tqdm_logging_redirect(
                loggers=[logging.getLogger("qlever")],
                desc="Entities",
                total=len(retry),
                leave=False,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}{postfix}",
            ) as pbar:
                for batch in batches(retry):
                    outcomes.update(
                        self.check_batch(
                            batch, endpoint, munge_script, keep_dir, pbar
                        )
                    )

        counts = {
            status: sum(1 for o in outcomes.values() if o == status)
            for status in (
                "match",
                "divergent",
                "undecidable",
                "redirect",
                "error",
            )
        }
        log.info("")
        log.info(
            f"Result: {counts['match']} exact matches,"
            f" {counts['divergent']} divergent,"
            f" {counts['undecidable']} undecidable (edited during check),"
            f" {counts['redirect']} redirects skipped,"
            f" {counts['error']} errors"
        )
        if counts["divergent"] > 0:
            divergent = [e for e, o in outcomes.items() if o == "divergent"]
            log.error(f"Endpoint DIVERGES from wikidata.org: {divergent}")
            return False
        return True
