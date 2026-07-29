from __future__ import annotations

import glob
import gzip
import json
import random
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path

from rdflib import Graph, Literal, URIRef

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


class CheckSyncWithWikidataCommand(QleverCommand):
    """
    Class for executing the `check-sync-with-wikidata` command.
    """

    def __init__(self):
        pass

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
        article blocks.
        """
        e = f"<{WD}{entity_id}>"
        # NOTE: Statement IRIs of old statements contain the entity ID in
        # lowercase.
        st = (
            f'FILTER(STRSTARTS(STR(?st), "{STATEMENT}{entity_id}-")'
            f' || STRSTARTS(STR(?st), "{STATEMENT}{entity_id.lower()}-"))'
        )
        queries = [
            f"CONSTRUCT {{ {e} ?p ?o }} WHERE {{ {e} ?p ?o }}",
            f"CONSTRUCT {{ ?st ?p2 ?o2 }} WHERE"
            f" {{ {e} ?p1 ?st . {st} ?st ?p2 ?o2 }}",
            f"CONSTRUCT {{ ?x ?p3 ?o3 }} WHERE"
            f" {{ {e} ?p1 ?st . {st} ?st ?p2 ?x ."
            f' FILTER(STRSTARTS(STR(?x), "http://www.wikidata.org/reference/")'
            f' || STRSTARTS(STR(?x), "http://www.wikidata.org/value/"))'
            f" ?x ?p3 ?o3 }}",
            f"CONSTRUCT {{ ?x2 ?p5 ?o5 }} WHERE"
            f" {{ {e} ?p1 ?st . {st} ?st ?p2 ?x ."
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
        `None` if the entity is a redirect.
        """
        url = (
            "https://www.wikidata.org/wiki/Special:EntityData/"
            f"{entity_id}.ttl?flavor=dump"
        )
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            final_url = response.url
        match = re.search(r"EntityData/(Q\d+)", final_url)
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
        for p, o in graph.predicate_objects(entity):
            document.add((entity, p, o))
            if isinstance(o, URIRef) and str(o).startswith(statement_prefixes):
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

    def sample_entities(self, num_entities, recent_fraction, seed):
        rng = random.Random(seed)
        num_recent = round(num_entities * recent_fraction)
        num_uniform = num_entities - num_recent
        entities = []
        if num_recent > 0:
            result = self.mediawiki_api(
                {
                    "action": "query",
                    "list": "recentchanges",
                    "rcnamespace": "0",
                    "rctype": "edit",
                    "rclimit": "500",
                }
            )
            titles = sorted(
                set(
                    rc["title"]
                    for rc in result["query"]["recentchanges"]
                    if re.fullmatch(r"Q\d+", rc["title"])
                )
            )
            entities += rng.sample(titles, min(num_recent, len(titles)))
        while len(entities) < num_recent + num_uniform:
            result = self.mediawiki_api(
                {
                    "action": "query",
                    "list": "random",
                    "rnnamespace": "0",
                    "rnlimit": str(
                        min(20, num_recent + num_uniform - len(entities))
                    ),
                }
            )
            entities += [
                r["title"]
                for r in result["query"]["random"]
                if re.fullmatch(r"Q\d+", r["title"])
                and r["title"] not in entities
            ]
            time.sleep(1)
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
        lines = set()
        geo_values = {}
        for s, p, o in graph:
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
        """
        for o in graph.objects(
            URIRef(f"{WD}{entity_id}"), URIRef(f"{SCHEMA}version")
        ):
            return str(o)
        return None

    def compare_entity(
        self, entity_id, qlever_graph, canonical_document, exclude_counters
    ):
        """
        Compare the two documents of the given entity. Returns one of
        `match`, `divergent`, `undecidable`, or `error`.
        """
        canonical_version = self.entity_version(canonical_document, entity_id)
        qlever_version = self.entity_version(qlever_graph, entity_id)
        if canonical_version is None or qlever_version is None:
            log.warning(
                f"{entity_id}: could not determine version"
                f" (canonical: {canonical_version},"
                f" endpoint: {qlever_version})"
            )
            return "error"
        if canonical_version != qlever_version:
            log.info(
                f"{entity_id}: version mismatch (canonical:"
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
                    f"{entity_id}: DIVERGENT at version {qlever_version}"
                    f" (geographic values differ by more than"
                    f" {GEO_TOLERANCE})"
                )
                return "divergent"
            log.info(
                f"{entity_id}: exact match at version"
                f" {qlever_version} ({len(qlever):,} triples)"
            )
            return "match"
        log.error(
            f"{entity_id}: DIVERGENT at version {qlever_version}"
            f" ({len(missing)} triples missing on the endpoint,"
            f" {len(extra)} extra)"
        )
        for line in sorted(missing)[:5]:
            log.error(f"  missing: {line}")
        for line in sorted(extra)[:5]:
            log.error(f"  extra:   {line}")
        return "divergent"

    def check_batch(self, batch, endpoint, munge_script, keep_dir):
        """
        Check a batch of entities: download the canonical data and query the
        endpoint pairwise (so that the version gate has the best chance),
        then munge the whole batch in ONE run of `munge.sh`, and compare
        entity by entity. Returns a dict from entity ID to outcome.
        """
        outcomes = {}
        snapshots = []
        for entity_id in batch:
            try:
                ttl_bytes, redirected_to = self.fetch_canonical(entity_id)
                if redirected_to is not None:
                    log.info(
                        f"{entity_id}: redirect to {redirected_to}, skipped"
                    )
                    outcomes[entity_id] = "redirect"
                else:
                    qlever_graph = self.qlever_entity_graph(
                        endpoint, entity_id
                    )
                    snapshots.append((entity_id, ttl_bytes, qlever_graph))
            except Exception as e:
                log.warning(f"{entity_id}: check failed ({e})")
                outcomes[entity_id] = "error"
            time.sleep(1)
        batch_graph = None
        if munge_script is not None and snapshots:
            try:
                batch_graph = self.munge(
                    b"".join(ttl for _, ttl, _ in snapshots),
                    munge_script,
                    keep_dir,
                )
            except Exception as e:
                log.warning(f"Munging the batch failed ({e})")
                for entity_id, _, _ in snapshots:
                    outcomes[entity_id] = "error"
                return outcomes
        for entity_id, ttl_bytes, qlever_graph in snapshots:
            try:
                if batch_graph is not None:
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
                log.warning(f"{entity_id}: check failed ({e})")
                outcomes[entity_id] = "error"
        return outcomes

    def execute(self, args) -> bool:
        endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"http://{args.host_name}:{args.port}"
        )
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
        if not 0.0 <= args.recent_fraction <= 1.0:
            log.error("`--recent-fraction` must be between 0.0 and 1.0")
            return False
        if args.entities:
            entities = [e.strip() for e in args.entities.split(",")]
            invalid = [e for e in entities if not re.fullmatch(r"Q\d+", e)]
            if invalid:
                log.error(f"Invalid entity IDs: {invalid}")
                return False
        else:
            log.info(
                f"Sampling {args.num_entities} entities"
                f" ({args.recent_fraction:.0%} recently edited,"
                f" seed {args.seed}) ..."
            )
            entities = self.sample_entities(
                args.num_entities, args.recent_fraction, args.seed
            )
        log.info(f"Entities: {', '.join(entities)}")

        if args.batch_size < 1:
            log.error("`--batch-size` must be at least 1")
            return False

        def batches(entity_ids):
            for i in range(0, len(entity_ids), args.batch_size):
                yield entity_ids[i : i + args.batch_size]

        outcomes = {}
        for batch in batches(entities):
            outcomes.update(
                self.check_batch(batch, endpoint, munge_script, keep_dir)
            )
        retry = [e for e, o in outcomes.items() if o == "undecidable"]
        if retry:
            log.info(
                f"Retrying {len(retry)} entities that were edited"
                f" during the check ..."
            )
            time.sleep(5)
            for batch in batches(retry):
                outcomes.update(
                    self.check_batch(batch, endpoint, munge_script, keep_dir)
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
