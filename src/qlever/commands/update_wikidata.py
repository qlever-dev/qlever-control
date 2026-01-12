from __future__ import annotations

import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from enum import Enum, auto

import rdflib.term
import requests_sse
from rdflib import Graph
from termcolor import colored
from tqdm.contrib.logging import tqdm_logging_redirect

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


# Monkey patch `rdflib.term._castLexicalToPython` to avoid casting of literals
# to Python types. We do not need it (all we want it convert Turtle to N-Triples),
# and we can speed up parsing by a factor of about 2.
def custom_cast_lexical_to_python(lexical, datatype):
    return None  # Your desired behavior


rdflib.term._castLexicalToPython = custom_cast_lexical_to_python


def retry_with_backoff(operation, operation_name, max_retries, log):
    """
    Retry an operation with exponential backoff, see backoff intervals below
    (in seconds). Returns the result of the operation if successful, or raises
    the last exception.
    """
    backoff_intervals = [5, 10, 30, 60, 300, 900, 1800, 3600]

    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            if attempt < max_retries - 1:
                # Use the appropriate backoff interval (once we get to the end
                # of the list, keep using the last interval).
                retry_delay = (
                    backoff_intervals[attempt]
                    if attempt < len(backoff_intervals)
                    else backoff_intervals[-1]
                )
                # Show the delay as seconds, minutes, or hours.
                if retry_delay >= 3600:
                    delay_str = f"{retry_delay // 3600}h"
                elif retry_delay >= 60:
                    delay_str = f"{retry_delay // 60}min"
                else:
                    delay_str = f"{retry_delay}s"
                log.warn(
                    f"{operation_name} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay_str} ..."
                )
                time.sleep(retry_delay)
            else:
                # If this was the last attempt, re-raise the exception.
                raise


def connect_to_sse_stream(sse_stream_url, since=None, event_id=None):
    """
    Connect to the SSE stream and return the connected EventSource.

    Args:
        sse_stream_url: URL of the SSE stream
        since: ISO date string to start from (mutually exclusive with event_id)
        event_id: Event ID to resume from (mutually exclusive with since)

    Returns:
        The connected EventSource object
    """
    if event_id:
        event_id_json = json.dumps(event_id)
        source = requests_sse.EventSource(
            sse_stream_url,
            headers={
                "Accept": "text/event-stream",
                "User-Agent": "qlever update-wikidata",
                "Last-Event-ID": event_id_json,
            },
        )
    else:
        source = requests_sse.EventSource(
            sse_stream_url,
            params={"since": since} if since else {},
            headers={
                "Accept": "text/event-stream",
                "User-Agent": "qlever update-wikidata",
            },
        )

    source.connect()
    return source


class UpdateWikidataCommand(QleverCommand):
    """
    Class for executing the `update` command.
    """

    def __init__(self):
        # SPARQL query to get the date until which the updates of the
        # SPARQL endpoint are complete.
        self.sparql_updates_complete_until_query = (
            "PREFIX wikibase: <http://wikiba.se/ontology#> "
            "PREFIX schema: <http://schema.org/> "
            "SELECT * WHERE { "
            "{ SELECT (MIN(?date_modified) AS ?updates_complete_until) { "
            "wikibase:Dump schema:dateModified ?date_modified } } "
            "UNION { wikibase:Dump wikibase:updatesCompleteUntil ?updates_complete_until } "
            "} ORDER BY DESC(?updates_complete_until) LIMIT 1"
        )
        # URL of the Wikidata SSE stream.
        self.wikidata_update_stream_url = (
            "https://stream.wikimedia.org/v2/"
            "stream/rdf-streaming-updater.mutation.v2"
        )
        # Remember if Ctrl+C was pressed, so we can handle it gracefully.
        self.ctrl_c_pressed = False
        # Set to `True` when finished.
        self.finished = False

    def description(self) -> str:
        return "Update from given SSE stream"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "sse_stream_url",
            nargs="?",
            type=str,
            default=self.wikidata_update_stream_url,
            help="URL of the SSE stream to update from",
        )
        subparser.add_argument(
            "--batch-size",
            type=int,
            default=100000,
            help="Group this many messages together into one update "
            "(default: one update for each message); NOTE: this simply "
            "concatenates the `rdf_added_data` and `rdf_deleted_data` fields, "
            "which is not 100%% correct; as soon as chaining is supported, "
            "this will be fixed",
        )
        subparser.add_argument(
            "--lag-seconds",
            type=int,
            default=1,
            help="When a message is encountered that is within this many "
            "seconds of the current time, finish the current batch "
            "(and show a warning that this happened)",
        )
        subparser.add_argument(
            "--since",
            type=str,
            help="Consume stream messages since this date "
            "(default: determine automatically from the SPARQL endpoint)",
        )
        subparser.add_argument(
            "--until",
            type=str,
            help="Stop consuming stream messages when reaching this date "
            "(default: continue indefinitely)",
        )
        subparser.add_argument(
            "--offset",
            type=int,
            help="Consume stream messages starting from this offset "
            "(default: not set)",
        )
        subparser.add_argument(
            "--topic",
            type=str,
            choices=[
                "eqiad.rdf-streaming-updater.mutation",
                "codfw.rdf-streaming-updater.mutation",
            ],
            default="eqiad.rdf-streaming-updater.mutation",
            help="The topic to consume from the SSE stream (default: "
            "eqiad.rdf-streaming-updater.mutation)",
        )
        subparser.add_argument(
            "--partition",
            type=int,
            default=0,
            help="The partition to consume from the SSE stream (default: 0)",
        )
        subparser.add_argument(
            "--min-or-max-date",
            choices=["min", "max"],
            default="max",
            help="Use the minimum or maximum date of the batch for the "
            "`updatesCompleteUntil` property (default: maximum)",
        )
        subparser.add_argument(
            "--wait-between-batches",
            type=int,
            default=300,
            help="Wait this many seconds between batches that were "
            "finished due to a message that is within `lag_seconds` of "
            "the current time (default: 300s)",
        )
        subparser.add_argument(
            "--num-messages",
            type=int,
            help="Process exactly this many messages and then exit "
            "(default: no bound on the number of messages)",
        )
        subparser.add_argument(
            "--verbose",
            choices=["no", "yes"],
            default="yes",
            help='Verbose logging, "yes" or "no" (default: "yes")',
        )
        subparser.add_argument(
            "--use-cached-sparql-queries",
            action="store_true",
            help="Use cached SPARQL query files if they exist with matching "
            "offset and target batch size (default: off)",
        )
        subparser.add_argument(
            "--check-offset-before-each-batch",
            choices=["yes", "no"],
            default="yes",
            help="Before each batch, verify that the stream offset matches the "
            "offset from the endpoint (default: yes)",
        )
        subparser.add_argument(
            "--rewind-to-earlier-offset",
            choices=["yes", "no"],
            default="yes",
            help="When the stream offset is later than the offset from the "
            "endpoint (e.g., after a server restart), rewind to the endpoint "
            "offset and reprocess messages (default: yes)",
        )
        subparser.add_argument(
            "--num-retries",
            type=int,
            default=10,
            help="Number of retries for offset verification queries when they fail "
            "(default: 10)",
        )

    # Handle Ctrl+C gracefully by finishing the current batch and then exiting.
    def handle_ctrl_c(self, signal_received, frame):
        if self.ctrl_c_pressed:
            log.warn("\rCtrl+C pressed again, watch your blood pressure")
        else:
            self.ctrl_c_pressed = True

    def execute(self, args) -> bool:
        # cURL command to get the date until which the updates of the
        # SPARQL endpoint are complete.
        sparql_endpoint = f"http://{args.host_name}:{args.port}"
        curl_cmd_updates_complete_until = (
            f"curl -s {sparql_endpoint}"
            f' -H "Accept: text/csv"'
            f' -H "Content-type: application/sparql-query"'
            f' --data "{self.sparql_updates_complete_until_query}"'
        )

        # Construct the command and show it.
        cmd_description = []
        if args.since:
            cmd_description.append(f"SINCE={args.since}")
        else:
            cmd_description.append(
                f"SINCE=$({curl_cmd_updates_complete_until} | sed 1d)"
            )
        if args.until:
            cmd_description.append(f"UNTIL={args.until}")
        cmd_description.append(
            f"Process SSE stream from {args.sse_stream_url} "
            f"in batches of up to {args.batch_size:,} messages "
        )
        self.show("\n".join(cmd_description), only_show=args.show)
        if args.show:
            return True

        # Compute the `since` date if not given.
        if args.since:
            since = args.since
        else:
            try:
                since = run_command(
                    f"{curl_cmd_updates_complete_until} | sed 1d",
                    return_output=True,
                ).strip()
            except Exception as e:
                log.error(
                    f"Error running `{curl_cmd_updates_complete_until}`: {e}"
                )
                return False

        # Special handling of Ctrl+C, see `handle_ctrl_c` above.
        signal.signal(signal.SIGINT, self.handle_ctrl_c)
        log.warn("Press Ctrl+C to finish and exit gracefully")
        log.info("")

        # If --offset is not provided, first try to get the offset from
        # the endpoint. Only fall back to date-based approach if no
        # offset is available.
        if not args.offset:
            try:
                sparql_query_stored_offset = (
                    "PREFIX wikibase: <http://wikiba.se/ontology#> "
                    "SELECT (MAX(?offset) AS ?maxOffset) WHERE { "
                    "<http://wikiba.se/ontology#Dump> "
                    "wikibase:updateStreamNextOffset ?offset "
                    "}"
                )
                curl_cmd_get_stored_offset = (
                    f"curl -s {sparql_endpoint}"
                    f' -H "Accept: text/csv"'
                    f' -H "Content-type: application/sparql-query"'
                    f' --data "{sparql_query_stored_offset}"'
                )
                result = run_command(
                    f"{curl_cmd_get_stored_offset} | sed 1d",
                    return_output=True,
                ).strip()
                if result and result != '""':
                    args.offset = int(result.strip('"'))
                    log.info(
                        f"Resuming from offset from endpoint: "
                        f"{args.offset}"
                    )
            except Exception as e:
                log.debug(
                    f"Could not retrieve offset from endpoint: {e}. "
                    f"Will determine offset from date instead."
                )

        # If --offset is still not set, determine it by reading a single
        # message from the SSE stream using the `since` date.
        if not args.offset:
            try:
                source = retry_with_backoff(
                    lambda: connect_to_sse_stream(
                        args.sse_stream_url, since=since
                    ),
                    "SSE stream connection",
                    args.num_retries,
                    log,
                )
                offset = None
                for event in source:
                    if event.type == "message" and event.data:
                        event_data = json.loads(event.data)
                        event_topic = event_data.get("meta").get("topic")
                        if event_topic == args.topic:
                            offset = event_data.get("meta").get("offset")
                            log.debug(
                                f"Determined offset from date: {since} -> {offset}"
                            )
                            break
                source.close()
                if offset is None:
                    raise Exception(
                        f"No event with topic {args.topic} found in stream"
                    )
                args.offset = offset
            except Exception as e:
                log.error(f"Error determining offset from stream: {e}")
                return False

        # Initialize all the statistics variables.
        batch_count = 0
        total_num_messages = 0
        total_update_time = 0
        start_time = time.perf_counter()
        wait_before_next_batch = False
        event_id_for_next_batch = (
            [
                {
                    "topic": args.topic,
                    "partition": args.partition,
                    "offset": args.offset,
                }
            ]
            if args.offset
            else None
        )

        # Track whether this is the first batch (to skip offset check)
        first_batch = True

        # Main event loop: Either resume from `event_id_for_next_batch` (if set),
        # or start a new connection to `args.sse_stream_url` (with URL
        # parameter `?since=`).
        while True:
            # Optionally wait before processing the next batch (make sure that
            # the wait is interruptible by Ctrl+C).
            if wait_before_next_batch:
                log.info(
                    f"Waiting {args.wait_between_batches} "
                    f"second{'s' if args.wait_between_batches > 1 else ''} "
                    f"before processing the next batch"
                )
                log.info("")
                wait_before_next_batch = False
                for _ in range(args.wait_between_batches):
                    if self.ctrl_c_pressed:
                        break
                    time.sleep(1)
            if self.ctrl_c_pressed:
                log.warn(
                    "\rCtrl+C pressed while waiting in between batches, "
                    "exiting"
                )
                break

            # Start stream from either `event_id_for_next_batch` or `since`.
            # We'll extract the offset for first_offset_in_batch later.
            if event_id_for_next_batch:
                event_id_json = json.dumps(event_id_for_next_batch)
                if args.verbose == "yes":
                    log.info(
                        colored(
                            f"Consuming stream from event ID: {event_id_json}",
                            attrs=["dark"],
                        )
                    )
            else:
                if args.verbose == "yes":
                    log.info(
                        colored(
                            f"Consuming stream from date: {since}",
                            attrs=["dark"],
                        )
                    )

            # Connect to the SSE stream with retry logic
            try:
                source = retry_with_backoff(
                    lambda: connect_to_sse_stream(
                        args.sse_stream_url,
                        since=since if not event_id_for_next_batch else None,
                        event_id=event_id_for_next_batch,
                    ),
                    "SSE stream connection for batch processing",
                    args.num_retries,
                    log,
                )
            except Exception as e:
                log.error(
                    f"Failed to connect to SSE stream after "
                    f"{args.num_retries} retry attempts, last error: {e}"
                )
                break

            # Next comes the inner loop, which processes exactly one "batch" of
            # messages. The batch is completed (simply using `break`) when either
            # `args.batch_size` messages have been processed, or when one of a
            # variety of conditions occur (Ctrl+C pressed, message within
            # `args.lag_seconds` of current time, delete operation followed by
            # insert of triple with that entity as subject).

            # Initialize all the batch variables.
            current_batch_size = 0
            # Extract the offset from the event ID to use as the starting offset
            # for this batch. This is set before processing any messages.
            if event_id_for_next_batch:
                first_offset_in_batch = event_id_for_next_batch[0]["offset"]
                event_id_for_next_batch = None
            else:
                # This should not happen since we now always determine the offset
                # before starting, but keep as fallback
                first_offset_in_batch = None

            # Check that the stream offset matches the offset from the endpoint
            # Skip this check on the first batch (when using --offset to resume)
            if (
                args.check_offset_before_each_batch == "yes"
                and not first_batch
                and first_offset_in_batch is not None
            ):
                sparql_query_offset = (
                    "PREFIX wikibase: <http://wikiba.se/ontology#> "
                    "SELECT (MAX(?offset) AS ?maxOffset) WHERE { "
                    "<http://wikiba.se/ontology#Dump> "
                    "wikibase:updateStreamNextOffset ?offset "
                    "}"
                )
                curl_cmd_check_offset = (
                    f"curl -s {sparql_endpoint}"
                    f' -H "Accept: text/csv"'
                    f' -H "Content-type: application/sparql-query"'
                    f' --data "{sparql_query_offset}"'
                )
                # Verify offset with retry logic
                try:
                    result = retry_with_backoff(
                        lambda: run_command(
                            f"{curl_cmd_check_offset} | sed 1d",
                            return_output=True,
                        ).strip(),
                        "Offset verification",
                        args.num_retries,
                        log,
                    )
                    if not result:
                        log.error(
                            "Failed to retrieve offset from endpoint: "
                            "query returned no results; this might be the first update, "
                            "or the offset triple is missing"
                        )
                        return False
                    endpoint_offset = int(result.strip('"'))
                    if endpoint_offset < first_offset_in_batch:
                        # Stream offset is LATER than endpoint offset
                        if args.rewind_to_earlier_offset == "yes":
                            log.info(
                                colored(
                                    f"Stream offset {first_offset_in_batch} is later "
                                    f"than offset {endpoint_offset} from endpoint; "
                                    f"this can happen after a server restart; "
                                    f"rewinding to offset {endpoint_offset} from endpoint",
                                    "cyan",
                                )
                            )
                            log.info("")
                            # Reconnect from the endpoint offset
                            event_id_for_next_batch = [
                                {
                                    "topic": args.topic,
                                    "partition": args.partition,
                                    "offset": endpoint_offset,
                                }
                            ]
                            continue  # Skip this batch and reconnect
                        else:
                            log.error(
                                f"Offset mismatch: stream offset {first_offset_in_batch} "
                                f"is later than offset {endpoint_offset} from endpoint; "
                                f"rewind disabled by --rewind-to-earlier-offset=no"
                            )
                            return False
                    elif endpoint_offset > first_offset_in_batch:
                        # Stream offset is EARLIER than endpoint offset - this is bad
                        log.error(
                            f"Offset mismatch: stream offset {first_offset_in_batch} "
                            f"is earlier than offset {endpoint_offset} from endpoint; "
                            f"this indicates that updates may have been applied "
                            f"out of order or some updates are missing"
                        )
                        return False
                except Exception as e:
                    log.error(
                        f"Failed to retrieve or verify offset from "
                        f"endpoint after {args.num_retries} retries; "
                        f"last error: {e}"
                    )
                    return False

            date_list = []
            delete_entity_ids = set()
            delta_to_now_list = []
            batch_assembly_start_time = time.perf_counter()
            insert_triples = set()
            delete_triples = set()

            # Check if we can use a cached SPARQL query file
            use_cached_file = False
            cached_file_name = None
            cached_meta_file_name = None
            cached_date_range = None
            if (
                args.use_cached_sparql_queries
                and first_offset_in_batch is not None
            ):
                cached_file_name = (
                    f"update.{first_offset_in_batch}.{args.batch_size}.sparql"
                )
                cached_meta_file_name = (
                    f"update.{first_offset_in_batch}.{args.batch_size}.meta"
                )
                if os.path.exists(cached_file_name):
                    use_cached_file = True
                    # Try to read metadata file for date range
                    if os.path.exists(cached_meta_file_name):
                        try:
                            with open(cached_meta_file_name, "r") as f:
                                cached_date_range = f.read().strip()
                        except Exception:
                            cached_date_range = None

                    if args.verbose == "yes":
                        log_msg = f"Using cached SPARQL query file: {cached_file_name}"
                        if cached_date_range:
                            log_msg += f" [date range: {cached_date_range}]"
                        log.info(colored(log_msg, "cyan"))

            # Process one event at a time (unless using cached file).
            if not use_cached_file:
                with tqdm_logging_redirect(
                    loggers=[logging.getLogger("qlever")],
                    desc="Batch",
                    total=args.batch_size,
                    leave=False,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}{postfix}",
                ) as pbar:
                    for event in source:
                        # Skip events that are not of type `message` (should not
                        # happen), have no field `data` (should not happen either), or
                        # where the topic is not in `args.topics` (one topic by itself
                        # should provide all relevant updates).
                        if event.type != "message" or not event.data:
                            continue
                        event_data = json.loads(event.data)
                        topic = event_data.get("meta").get("topic")
                        if topic != args.topic:
                            continue

                        try:
                            # Extract offset, topic, and partition from the message metadata
                            # to construct a precise event ID for resuming.
                            meta = event_data.get("meta")
                            offset = meta.get("offset")
                            topic = meta.get("topic")
                            partition = meta.get("partition")

                            # Get the date (rounded *down* to seconds).
                            date = meta.get("dt")
                            date = re.sub(r"\.\d*Z$", "Z", date)

                            # Get the other relevant fields from the message.
                            entity_id = event_data.get("entity_id")
                            operation = event_data.get("operation")
                            rdf_added_data = event_data.get("rdf_added_data")
                            rdf_deleted_data = event_data.get(
                                "rdf_deleted_data"
                            )
                            rdf_linked_shared_data = event_data.get(
                                "rdf_linked_shared_data"
                            )
                            rdf_unlinked_shared_data = event_data.get(
                                "rdf_unlinked_shared_data"
                            )

                            # Check batch completion conditions BEFORE processing the
                            # data of this message. If any of the conditions is met,
                            # we finish the batch and resume from the LAST PROCESSED
                            # message (not the current one that triggered the break).
                            #
                            # NOTE: We will update event_id_for_next_batch AFTER
                            # successfully processing each message (see below), so that
                            # when we break, it contains the last processed event ID.
                            since = None

                            # Condition 1: Delete followed by insert for same entity.
                            operation_adds_data = (
                                rdf_added_data is not None
                                or rdf_linked_shared_data is not None
                            )
                            if (
                                operation_adds_data
                                and entity_id in delete_entity_ids
                            ):
                                if args.verbose == "yes":
                                    log.warn(
                                        f"Encountered operation that adds data for "
                                        f"an entity ID ({entity_id}) that was deleted "
                                        f"earlier in this batch; finishing batch and "
                                        f"resuming from this message in the next batch"
                                    )
                                break

                            # Condition 2: Batch size or limit on number of
                            # messages reached.
                            if current_batch_size >= args.batch_size or (
                                args.num_messages is not None
                                and total_num_messages >= args.num_messages
                            ):
                                break

                            # Condition 3: Message close to current time.
                            date_obj = datetime.strptime(
                                date, "%Y-%m-%dT%H:%M:%SZ"
                            ).replace(tzinfo=timezone.utc)
                            date_as_epoch_s = date_obj.timestamp()

                            now_as_epoch_s = time.time()
                            delta_to_now_s = now_as_epoch_s - date_as_epoch_s
                            if (
                                delta_to_now_s < args.lag_seconds
                                and current_batch_size > 0
                            ):
                                if args.verbose == "yes":
                                    log.warn(
                                        f"Encountered message with date {date}, which is within "
                                        f"{args.lag_seconds} "
                                        f"second{'s' if args.lag_seconds > 1 else ''} "
                                        f"of the current time, finishing the current batch"
                                    )
                                wait_before_next_batch = (
                                    args.wait_between_batches is not None
                                    and args.wait_between_batches > 0
                                )
                                break

                            # Condition 4: Reached `--until` date and at least one
                            # message was processed.
                            if (
                                args.until
                                and date >= args.until
                                and current_batch_size > 0
                            ):
                                log.warn(
                                    f"Reached --until date {args.until} "
                                    f"(message date: {date}), that's it folks"
                                )
                                self.finished = True
                                break

                            # Delete operations are postponed until the end of the
                            # batch, so remember the entity ID here.
                            if operation == "delete":
                                delete_entity_ids.add(entity_id)

                            # Replace each occurrence of `\\` by `\u005C\u005C`
                            # (which is twice the Unicode for backslash).
                            #
                            # NOTE: Strictly speaking, it would be enough to do
                            # this for two backslashes followed by a `u`, but
                            # doing it for all double backslashes does not
                            # harm. When parsing a SPARQL query, then according
                            # to the standar, first all occurrences of `\uxxxx`
                            # (where `xxxx` are four hex digits) are replaced
                            # by the corresponding Unicode character. That is a
                            # problem when `\\uxxxx` occurs in a literal,
                            # because then it would be replaced by `\` followed
                            # by the Unicode character, which is invalied
                            # SPARQL. The subsitution avoids that problem.
                            def node_to_sparql(node: rdflib.term.Node) -> str:
                                return node.n3().replace(
                                    "\\\\", "\\u005C\\u005C"
                                )

                            # Process the to-be-deleted triples.
                            for rdf_to_be_deleted in (
                                rdf_deleted_data,
                                rdf_unlinked_shared_data,
                            ):
                                if rdf_to_be_deleted is not None:
                                    try:
                                        rdf_to_be_deleted_data = (
                                            rdf_to_be_deleted.get("data")
                                        )
                                        graph = Graph()
                                        log.debug(
                                            f"RDF to_be_deleted data: {rdf_to_be_deleted_data}"
                                        )
                                        graph.parse(
                                            data=rdf_to_be_deleted_data,
                                            format="turtle",
                                        )
                                        for s, p, o in graph:
                                            triple = f"{s.n3()} {p.n3()} {node_to_sparql(o)}"
                                            # NOTE: In case there was a previous `insert` of that
                                            # triple, it is safe to remove that `insert`, but not
                                            # the `delete` (in case the triple is contained in the
                                            # original data).
                                            if triple in insert_triples:
                                                insert_triples.remove(triple)
                                            delete_triples.add(triple)
                                    except Exception as e:
                                        log.error(
                                            f"Error reading `rdf_to_be_deleted_data`: {e}"
                                        )
                                        return False

                            # Process the to-be-added triples.
                            for rdf_to_be_added in (
                                rdf_added_data,
                                rdf_linked_shared_data,
                            ):
                                if rdf_to_be_added is not None:
                                    try:
                                        rdf_to_be_added_data = (
                                            rdf_to_be_added.get("data")
                                        )
                                        graph = Graph()
                                        log.debug(
                                            "RDF to be added data: {rdf_to_be_added_data}"
                                        )
                                        graph.parse(
                                            data=rdf_to_be_added_data,
                                            format="turtle",
                                        )
                                        for s, p, o in graph:
                                            triple = f"{s.n3()} {p.n3()} {node_to_sparql(o)}"
                                            # NOTE: In case there was a previous `delete` of that
                                            # triple, it is safe to remove that `delete`, but not
                                            # the `insert` (in case the triple is not contained in
                                            # the original data).
                                            if triple in delete_triples:
                                                delete_triples.remove(triple)
                                            insert_triples.add(triple)
                                    except Exception as e:
                                        log.error(
                                            f"Error reading `rdf_to_be_added_data`: {e}"
                                        )
                                        return False

                        except Exception as e:
                            log.error(f"Error reading data from message: {e}")
                            log.info(event)
                            continue

                        # Message was successfully processed, update batch tracking
                        current_batch_size += 1
                        total_num_messages += 1
                        pbar_update_frequency = 100
                        if (current_batch_size % pbar_update_frequency) == 0:
                            pbar.set_postfix(
                                {
                                    "Time": date_obj.strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    )
                                }
                            )
                            pbar.update(pbar_update_frequency)
                        log.debug(
                            f"DATE: {date_as_epoch_s:.0f} [{date}], "
                            f"NOW: {now_as_epoch_s:.0f}, "
                            f"DELTA: {now_as_epoch_s - date_as_epoch_s:.0f}"
                        )
                        date_list.append(date)
                        delta_to_now_list.append(delta_to_now_s)

                        # Update the event ID for the next batch. We increment the
                        # offset by 1 so that the next batch starts with the next
                        # message (not re-processing the current one).
                        event_id_for_next_batch = [
                            {
                                "topic": topic,
                                "partition": partition,
                                "offset": offset + 1,
                            }
                        ]

                        # Ctrl+C finishes the current batch (this should come at the
                        # end of the inner event loop so that always at least one
                        # message is processed).
                        if self.ctrl_c_pressed:
                            log.warn(
                                "\rCtrl+C pressed while processing a batch, "
                                "finishing it and exiting"
                            )
                            break
            else:
                # Using cached file - set batch size and calculate next offset
                current_batch_size = args.batch_size
                total_num_messages += current_batch_size
                event_id_for_next_batch = [
                    {
                        "topic": args.topic,
                        "partition": args.partition,
                        "offset": first_offset_in_batch + current_batch_size,
                    }
                ]

            # Process the current batch of messages (or skip if using cached).
            batch_count += 1
            if not use_cached_file:
                batch_assembly_end_time = time.perf_counter()
                batch_assembly_time_ms = int(
                    1000
                    * (batch_assembly_end_time - batch_assembly_start_time)
                )
                date_list.sort()
                delta_to_now_list.sort()
                min_delta_to_now_s = delta_to_now_list[0]
                if min_delta_to_now_s < 10:
                    min_delta_to_now_s = f"{min_delta_to_now_s:.1f}"
                else:
                    min_delta_to_now_s = f"{int(min_delta_to_now_s):,}"
                log.info(
                    f"Assembled batch #{batch_count}, "
                    f"#messages: {current_batch_size:2,}, "
                    f"date range: {date_list[0]} - {date_list[-1]}  "
                    f"[assembly time: {batch_assembly_time_ms:3,}ms, "
                    f"min delta to NOW: {min_delta_to_now_s}s]"
                )

                # Add the min and max date of the batch to `insert_triples`.
                #
                # NOTE: The min date means that we have *all* updates until that
                # date. The max date is the date of the latest update we have seen.
                # However, there may still be earlier updates that we have not seen
                # yet. Wikidata uses `schema:dateModified` for the latter semantics,
                # so we use it here as well. For the other semantics, we invent
                # a new property `wikibase:updatesCompleteUntil`.
                insert_triples.add(
                    f"<http://wikiba.se/ontology#Dump> "
                    f"<http://schema.org/dateModified> "
                    f'"{date_list[-1]}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
                )
                updates_complete_until = (
                    date_list[-1]
                    if args.min_or_max_date == "max"
                    else date_list[0]
                )
                insert_triples.add(
                    f"<http://wikiba.se/ontology#Dump> "
                    f"<http://wikiba.se/ontology#updatesCompleteUntil> "
                    f'"{updates_complete_until}"'
                    f"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
                )
                insert_triples.add(
                    "<http://wikiba.se/ontology#Dump> "
                    "<http://wikiba.se/ontology#updateStreamNextOffset> "
                    f'"{event_id_for_next_batch[0]["offset"]}"'
                )

                # Construct UPDATE operation.
                delete_block = " . \n  ".join(delete_triples)
                insert_block = " . \n  ".join(insert_triples)
                delete_insert_operation = (
                    f"DELETE {{\n  {delete_block} \n}} "
                    f"INSERT {{\n  {insert_block} \n}} "
                    f"WHERE {{ }}\n"
                )

                # If `delete_entity_ids` is non-empty, add a `DELETE WHERE`
                # operation that deletes all triples that are associated with only
                # those entities.
                delete_entity_ids_as_values = " ".join(
                    [f"wd:{qid}" for qid in delete_entity_ids]
                )
                if len(delete_entity_ids) > 0:
                    delete_where_operation = (
                        f"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                        f"PREFIX wikibase: <http://wikiba.se/ontology#>\n"
                        f"PREFIX wd: <http://www.wikidata.org/entity/>\n"
                        f"DELETE {{\n"
                        f"  ?s ?p ?o .\n"
                        f"}} WHERE {{\n"
                        f"  {{\n"
                        f"    VALUES ?s {{ {delete_entity_ids_as_values} }}\n"
                        f"    ?s ?p ?o .\n"
                        f"  }} UNION {{\n"
                        f"    VALUES ?_1 {{ {delete_entity_ids_as_values} }}\n"
                        f"    ?_1 ?_2 ?s .\n"
                        f"    ?s ?p ?o .\n"
                        f"    ?s rdf:type wikibase:Statement .\n"
                        f"  }}\n"
                        f"}}\n"
                    )
                    delete_insert_operation += ";\n" + delete_where_operation

            # Construct curl command. For batch size 1, send the operation via
            # `--data-urlencode`, otherwise write to file and send via `--data-binary`.
            curl_cmd = (
                f"curl -s -X POST"
                f' "{sparql_endpoint}?access-token={args.access_token}"'
                f" -H 'Content-Type: application/sparql-update'"
            )
            if use_cached_file:
                # Use the cached file instead of writing a new one
                update_arg_file_name = cached_file_name
            else:
                # Write the constructed SPARQL update to a file
                update_arg_file_name = f"update.{first_offset_in_batch}.{current_batch_size}.sparql"
                with open(update_arg_file_name, "w") as f:
                    f.write(delete_insert_operation)
                # Write metadata file with date range
                meta_file_name = (
                    f"update.{first_offset_in_batch}.{current_batch_size}.meta"
                )
                with open(meta_file_name, "w") as f:
                    f.write(f"{date_list[0]} - {date_list[-1]}")
            curl_cmd += f" --data-binary @{update_arg_file_name}"
            if args.verbose == "yes":
                log.info(colored(curl_cmd, "blue"))

            # Run it (using `curl` for batch size up to 1000, otherwise
            # `requests`) with retry logic.
            try:
                result = retry_with_backoff(
                    lambda: run_command(curl_cmd, return_output=True),
                    "UPDATE request",
                    args.num_retries,
                    log,
                )
                result_file_name = f"update.{first_offset_in_batch}.{current_batch_size}.result"
                with open(result_file_name, "w") as f:
                    f.write(result)
            except Exception as e:
                log.error(
                    f"Failed to execute UPDATE request after "
                    f"{args.num_retries} retry attempts, last error: "
                    f"{e}"
                )
                return False

            # Results should be a JSON, parse it.
            try:
                result = json.loads(result)
            except Exception as e:
                log.error(
                    f"Error parsing JSON result: {e}. "
                    f"The first 1000 characters are: {result[:1000]}"
                )
                return False

            # Check if the result contains a QLever exception.
            if "exception" in result:
                error_msg = result["exception"]
                log.error(f"QLever exception: {error_msg}")
                log.info("")
                continue

            # Helper function for getting the value of `stats["time"][...]`
            # without the "ms" suffix. If the extraction fails, return 0

            # (and optionally log the failure).
            class FailureMode(Enum):
                LOG_ERROR = auto()
                SILENTLY_RETURN_ZERO = auto()
                THROW_EXCEPTION = auto()

            def get_time_ms(
                stats, *keys: str, failure_mode=FailureMode.LOG_ERROR
            ) -> int:
                try:
                    value = stats["time"]
                    for key in keys:
                        value = value[key]
                    value = int(value)
                except Exception:
                    if failure_mode == FailureMode.THROW_EXCEPTION:
                        raise
                    elif failure_mode == FailureMode.LOG_ERROR:
                        log.error(
                            f"Error extracting time from JSON statistics, "
                            f"keys: {keys}"
                        )
                    value = 0
                return value

            # Check for old JSON format (no `operations` or `time` on top level).
            old_json_message_template = (
                "Result JSON does not contain `{}` field, you are "
                "probably using an old version of QLever"
            )
            for field in ["operations", "time"]:
                if field not in result:
                    raise RuntimeError(old_json_message_template.format(field))

            # Get the per-operation statistics.
            for i, stats in enumerate(result["operations"]):
                try:
                    ins_after = stats["delta-triples"]["after"]["inserted"]
                    del_after = stats["delta-triples"]["after"]["deleted"]
                    ops_after = stats["delta-triples"]["after"]["total"]
                    num_ins = int(
                        stats["delta-triples"]["operation"]["inserted"]
                    )
                    num_del = int(
                        stats["delta-triples"]["operation"]["deleted"]
                    )
                    num_ops = int(stats["delta-triples"]["operation"]["total"])
                    time_op_total = get_time_ms(stats, "total")
                    time_us_per_op = (
                        int(1000 * time_op_total / num_ops)
                        if num_ops > 0
                        else 0
                    )
                    if args.verbose == "yes":
                        log.info(
                            colored(
                                f"TRIPLES: {num_ops:+10,} -> {ops_after:10,}, "
                                f"INS: {num_ins:+10,} -> {ins_after:10,}, "
                                f"DEL: {num_del:+10,} -> {del_after:10,}, "
                                f"TIME: {time_op_total:7,}ms, "
                                f"TIME/TRIPLE: {time_us_per_op:6,}µs",
                                attrs=["bold"],
                            )
                        )

                    time_planning = get_time_ms(stats, "planning")
                    time_compute_ids = get_time_ms(
                        stats,
                        "execution",
                        "computeIds",
                        "total",
                    )
                    time_where = get_time_ms(
                        stats,
                        "execution",
                        "evaluateWhere",
                    )
                    time_metadata = get_time_ms(
                        stats,
                        "updateMetadata",
                    )
                    time_insert = get_time_ms(
                        stats,
                        "execution",
                        "insertTriples",
                        "total",
                        failure_mode=FailureMode.SILENTLY_RETURN_ZERO,
                    )
                    time_delete = get_time_ms(
                        stats,
                        "execution",
                        "deleteTriples",
                        "total",
                        failure_mode=FailureMode.SILENTLY_RETURN_ZERO,
                    )
                    time_unaccounted = time_op_total - (
                        time_planning
                        + time_compute_ids
                        + time_where
                        + time_metadata
                        + time_delete
                        + time_insert
                    )
                    if args.verbose == "yes":
                        log.info(
                            f"METADATA: {100 * time_metadata / time_op_total:2.0f}%, "
                            f"PLANNING: {100 * time_planning / time_op_total:2.0f}%, "
                            f"WHERE: {100 * time_where / time_op_total:2.0f}%, "
                            f"IDS: {100 * time_compute_ids / time_op_total:2.0f}%, "
                            f"DELETE: {100 * time_delete / time_op_total:2.0f}%, "
                            f"INSERT: {100 * time_insert / time_op_total:2.0f}%, "
                            f"UNACCOUNTED: {100 * time_unaccounted / time_op_total:2.0f}%",
                        )

                except Exception as e:
                    log.warn(
                        f"Error extracting statistics: {e}, "
                        f"curl command was: {curl_cmd}"
                    )
                    # Show traceback for debugging.
                    import traceback

                    traceback.print_exc()
                    log.info("")
                    continue

            # Get times for the whole request (not per operation).
            time_parsing = get_time_ms(
                result,
                "parsing",
            )
            time_metadata = get_time_ms(
                result,
                "metadataUpdateForSnapshot",
            )
            time_snapshot = get_time_ms(
                result,
                "snapshotCreation",
            )
            time_writeback = get_time_ms(
                result,
                "diskWriteback",
            )
            time_operations = get_time_ms(
                result,
                "operations",
            )
            time_total = get_time_ms(
                result,
                "total",
            )
            time_unaccounted = time_total - (
                time_parsing
                + time_metadata
                + time_snapshot
                + time_writeback
                + time_operations
            )

            # Update the totals.
            total_update_time += time_total / 1000.0
            total_elapsed_time = time.perf_counter() - start_time

            # Show statistics for the completed batch.
            if args.verbose == "yes":
                log.info(
                    colored(
                        f"TOTAL UPDATE TIME SO FAR: {total_update_time:4.0f}s, "
                        f"TOTAL ELAPSED TIME SO FAR: {total_elapsed_time:4.0f}s, "
                        f"TOTAL TIME FOR THIS UPDATE REQUEST: {time_total:7,}ms, ",
                        attrs=["bold"],
                    )
                )
                log.info(
                    f"PARSING: {100 * time_parsing / time_total:2.0f}%, "
                    f"OPERATIONS: {100 * time_operations / time_total:2.0f}%, "
                    f"METADATA: {100 * time_metadata / time_total:2.0f}%, "
                    f"SNAPSHOT: {100 * time_snapshot / time_total:2.0f}%, "
                    f"WRITEBACK: {100 * time_writeback / time_total:2.0f}%, "
                    f"UNACCOUNTED: {100 * time_unaccounted / time_total:2.0f}%",
                )
                log.info("")

            # Close the source connection (for each batch, we open a new one,
            # either from `event_id_for_next_batch` or from `since`).
            source.close()

            # After the first batch is processed, enable offset checking for
            # subsequent batches.
            first_batch = False

            # If Ctrl+C was pressed, we reached `--until`, or we processed
            # exactly `--num-messages`, finish.
            if (
                self.ctrl_c_pressed
                or self.finished
                or (
                    args.num_messages is not None
                    and total_num_messages >= args.num_messages
                )
            ):
                break

        # Final message after all batches have been processed.
        log.info(
            f"Processed {batch_count} "
            f"{'batches' if batch_count > 1 else 'batch'} "
            f"terminating update command"
        )
        return True
