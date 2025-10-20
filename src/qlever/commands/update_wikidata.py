from __future__ import annotations

import json
import re
import signal
import time
from datetime import datetime, timezone

import rdflib.term
import requests
import requests_sse
from rdflib import Graph
from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


# Monkey patch `rdflib.term._castLexicalToPython` to avoid casting of literals
# to Python types. We do not need it (all we want it convert Turtle to N-Triples),
# and we can speed up parsing by a factor of about 2.
def custom_cast_lexical_to_python(lexical, datatype):
    return None  # Your desired behavior


rdflib.term._castLexicalToPython = custom_cast_lexical_to_python


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

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
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
            "--topics",
            type=str,
            default="eqiad.rdf-streaming-updater.mutation",
            help="Comma-separated list of topics to consume from the SSE stream"
            " (default: only eqiad.rdf-streaming-updater.mutation)",
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
            default=3600,
            help="Wait this many seconds between batches that were "
            "finished due to a message that is within `lag_seconds` of "
            "the current time (default: 3600s)",
        )

    # Handle Ctrl+C gracefully by finishing the current batch and then exiting.
    def handle_ctrl_c(self, signal_received, frame):
        if self.ctrl_c_pressed:
            log.warn(
                "\rPressing Ctrl+C again does not speed things up"
                ", watch your blood pressure"
            )
        else:
            self.ctrl_c_pressed = True
            log.warn(
                "\rCtrl+C pressed, will finish the current batch and then exit"
            )

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
            f"in batches of at most {args.batch_size:,} messages "
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
        log.info(f"SINCE={since}")
        if args.until:
            log.info(f"UNTIL={args.until}")
        log.info("")
        log.info("Press Ctrl+C to finish the current batch and end gracefully")
        log.info("")

        # Initialize all the statistics variables.
        batch_count = 0
        total_num_ops = 0
        total_time_s = 0
        start_time = time.perf_counter()
        topics_to_consider = set(args.topics.split(","))
        wait_before_next_batch = False
        event_id_for_next_batch = None

        # Main event loop: Either resume from `event_id_for_next_batch` (if set),
        # or start a new connection to `args.sse_stream_url` (with URL
        # parameter `?since=`).
        while True:
            if event_id_for_next_batch:
                log.info(
                    colored(
                        f"Consuming stream from event ID: "
                        f"{event_id_for_next_batch}",
                        attrs=["dark"],
                    )
                )
                source = requests_sse.EventSource(
                    args.sse_stream_url,
                    headers={
                        "Accept": "text/event-stream",
                        "User-Agent": "qlever update-wikidata",
                        "Last-Event-ID": event_id_for_next_batch,
                    },
                )
                event_id_for_next_batch = None
            else:
                log.info(
                    colored(
                        f"Consuming stream from date: {since}", attrs=["dark"]
                    )
                )
                source = requests_sse.EventSource(
                    args.sse_stream_url,
                    params={"since": since},
                    headers={
                        "Accept": "text/event-stream",
                        "User-Agent": "qlever update-wikidata",
                    },
                )
            source.connect()

            # Next comes the inner loop, which processes exactly one "batch" of
            # messages. The batch is completed (simply using `break`) when either
            # `args.batch_size` messages have been processed, or when one of a
            # variety of conditions occur (Ctrl+C pressed, message within
            # `args.lag_seconds` of current time, delete operation followed by
            # insert of triple with that entity as subject).

            # Initialize all the batch variables.
            current_batch_size = 0
            date_list = []
            delete_entity_ids = set()
            delta_to_now_list = []
            batch_assembly_start_time = time.perf_counter()
            insert_triples = set()
            delete_triples = set()

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

            # Process one event at a time.
            for event in source:
                # Ctrl+C finishes the current batch.
                if self.ctrl_c_pressed:
                    break

                # Skip events that are not of type `message` (should not
                # happen), have no field `data` (should not happen either), or
                # where the topic is not in `args.topics` (one topic by itself
                # should provide all relevant updates).
                if event.type != "message" or not event.data:
                    continue
                event_data = json.loads(event.data)
                topic = event_data.get("meta").get("topic")
                if topic not in topics_to_consider:
                    continue

                try:
                    # The event ID of the update (precise) and its date
                    # (rounded *down* to seconds so that when we resume from
                    # this date, we do not miss any updates).
                    current_event_id = event.last_event_id
                    date = event_data.get("meta").get("dt")
                    date = re.sub(r"\.\d*Z$", "Z", date)

                    # Get the other relevant fields from the message.
                    entity_id = event_data.get("entity_id")
                    operation = event_data.get("operation")
                    rdf_added_data = event_data.get("rdf_added_data")
                    rdf_deleted_data = event_data.get("rdf_deleted_data")
                    rdf_linked_shared_data = event_data.get(
                        "rdf_linked_shared_data"
                    )
                    rdf_unlinked_shared_data = event_data.get(
                        "rdf_unlinked_shared_data"
                    )

                    # Check batch completion conditions BEFORE processing the
                    # data of this message. If any of the conditions is met,
                    # we finish the batch and resume from this message in the
                    # next batch.
                    #
                    # NOTE: In the current implementation, every batch after
                    # the first resumes from an event ID. In the future, we
                    # might have other conditions that make us want to resume
                    # from a date instead.
                    event_id_for_next_batch = current_event_id
                    since = None

                    # Condition 1: Delete followed by insert for same entity.
                    operation_adds_data = (
                        rdf_added_data is not None
                        or rdf_linked_shared_data is not None
                    )
                    if operation_adds_data and entity_id in delete_entity_ids:
                        log.warn(
                            f"Encountered operation that adds data for "
                            f"an entity ID ({entity_id}) that was deleted "
                            f"earlier in this batch; finishing batch and "
                            f"resuming from this message in the next batch"
                        )
                        break

                    # Condition 2: Batch size reached.
                    if current_batch_size >= args.batch_size:
                        break

                    # Condition 3: Message close to current time.
                    date_as_epoch_s = (
                        datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
                        .replace(tzinfo=timezone.utc)
                        .timestamp()
                    )
                    now_as_epoch_s = time.time()
                    delta_to_now_s = now_as_epoch_s - date_as_epoch_s
                    if delta_to_now_s < args.lag_seconds:
                        log.warn(
                            f"Encountered message with date {date}, which is within "
                            f"{args.lag_seconds} "
                            f"second{'s' if args.lag_seconds > 1 else ''} "
                            f"of the current time, finishing the current batch"
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

                    # Process the to-be-deleted triples.
                    for rdf_to_be_deleted in (
                        rdf_deleted_data,
                        rdf_unlinked_shared_data,
                    ):
                        if rdf_to_be_deleted is not None:
                            try:
                                rdf_to_be_deleted_data = rdf_to_be_deleted.get(
                                    "data"
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
                                    triple = f"{s.n3()} {p.n3()} {o.n3()}"
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
                                rdf_to_be_added_data = rdf_to_be_added.get(
                                    "data"
                                )
                                graph = Graph()
                                log.debug(
                                    "RDF to be added data: {rdf_to_be_added_data}"
                                )
                                graph.parse(
                                    data=rdf_to_be_added_data, format="turtle"
                                )
                                for s, p, o in graph:
                                    triple = f"{s.n3()} {p.n3()} {o.n3()}"
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
                log.debug(
                    f"DATE: {date_as_epoch_s:.0f} [{date}], "
                    f"NOW: {now_as_epoch_s:.0f}, "
                    f"DELTA: {now_as_epoch_s - date_as_epoch_s:.0f}"
                )
                date_list.append(date)
                delta_to_now_list.append(delta_to_now_s)

            # Process the current batch of messages.
            batch_assembly_end_time = time.perf_counter()
            batch_assembly_time_ms = int(
                1000 * (batch_assembly_end_time - batch_assembly_start_time)
            )
            batch_count += 1
            date_list.sort()
            delta_to_now_list.sort()
            min_delta_to_now_s = delta_to_now_list[0]
            if min_delta_to_now_s < 10:
                min_delta_to_now_s = f"{min_delta_to_now_s:.1f}"
            else:
                min_delta_to_now_s = f"{int(min_delta_to_now_s):,}"
            log.info(
                f"Assembled batch #{batch_count} "
                f"with {current_batch_size:,} "
                f"message{'s' if current_batch_size > 1 else ''}, "
                f"date range: {date_list[0]} - {date_list[-1]}  "
                f"[assembly time: {batch_assembly_time_ms:,}ms, "
                f"min delta to NOW: {min_delta_to_now_s}s]"
            )
            wait_before_next_batch = (
                args.wait_between_batches is not None
                and args.wait_between_batches > 0
                and current_batch_size < args.batch_size
                and not event_id_for_next_batch
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
                f"curl -s -X POST {sparql_endpoint}"
                f" -H 'Authorization: Bearer {args.access_token}'"
                f" -H 'Content-Type: application/sparql-update'"
            )
            update_arg_file_name = f"update.sparql.{batch_count}"
            with open(update_arg_file_name, "w") as f:
                f.write(delete_insert_operation)
            curl_cmd += f" --data-binary @{update_arg_file_name}"
            log.info(colored(curl_cmd, "blue"))

            # Run it (using `curl` for batch size up to 1000, otherwise
            # `requests`).
            try:
                headers = {
                    "Authorization": f"Bearer {args.access_token}",
                    "Content-Type": "application/sparql-update",
                }
                response = requests.post(
                    url=sparql_endpoint,
                    headers=headers,
                    data=delete_insert_operation,
                )
                result = response.text
                with open(f"update.result.{batch_count}", "w") as f:
                    f.write(result)
            except Exception as e:
                log.warn(f"Error running `requests.post`: {e}")
                log.info("")
                continue

            # Results should be a JSON, parse it.
            try:
                result = json.loads(result)
            except Exception as e:
                log.error(
                    f"Error parsing JSON result: {e}"
                    f", the first 1000 characters are:"
                )
                log.info(result[:1000])
                log.info("")
                continue

            # Check if the result contains a QLever exception.
            if "exception" in result:
                error_msg = result["exception"]
                log.error(f"QLever exception: {error_msg}")
                log.info("")
                continue

            # Helper function for getting the value of `stats["time"][...]`
            # without the "ms" suffix. If the extraction fails, return 0
            # (and optionally log the failure).
            def get_time_ms(stats, *keys: str, log_fail: bool = False) -> int:
                try:
                    value = stats["time"]
                    for key in keys:
                        value = value[key]
                    value = int(value)
                except Exception:
                    if log_fail:
                        log.error(
                            f"Error extracting time from JSON statistics, "
                            f"keys: {keys}"
                        )
                    value = 0
                return value

            # If the batch ended due to a delete operation, we have two
            # operations (and two statistics), otherwise only one.
            for i, stats in enumerate(result):
                # Show statistics of the update operation.
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
                    time_ms = get_time_ms(stats, "total")
                    time_us_per_op = int(1000 * time_ms / num_ops)
                    log.info(
                        colored(
                            f"TRIPLES: {num_ops:+10,} -> {ops_after:10,}, "
                            f"INS: {num_ins:+10,} -> {ins_after:10,}, "
                            f"DEL: {num_del:+10,} -> {del_after:10,}, "
                            f"TIME: {time_ms:7,}ms, "
                            f"TIME/TRIPLE: {time_us_per_op:6,}µs",
                            attrs=["bold"],
                        )
                    )

                    # Also show a detailed breakdown of the total time.
                    time_preparation = get_time_ms(
                        stats,
                        "execution",
                        "processUpdateImpl",
                        "preparation",
                    )
                    time_insert = get_time_ms(
                        stats,
                        "execution",
                        "processUpdateImpl",
                        "insertTriples",
                        "total",
                        log_fail=False,
                    )
                    time_delete = get_time_ms(
                        stats,
                        "execution",
                        "processUpdateImpl",
                        "deleteTriples",
                        "total",
                        log_fail=False,
                    )
                    time_snapshot = get_time_ms(
                        stats, "execution", "snapshotCreation"
                    )
                    time_writeback = get_time_ms(
                        stats, "execution", "diskWriteback"
                    )
                    time_unaccounted = time_ms - (
                        time_delete
                        + time_insert
                        + time_preparation
                        + time_snapshot
                        + time_writeback
                    )
                    log.info(
                        f"PREPARATION: {100 * time_preparation / time_ms:2.0f}%, "
                        f"INSERT: {100 * time_insert / time_ms:2.0f}%, "
                        f"DELETE: {100 * time_delete / time_ms:2.0f}%, "
                        f"SNAPSHOT: {100 * time_snapshot / time_ms:2.0f}%, "
                        f"WRITEBACK: {100 * time_writeback / time_ms:2.0f}%, "
                        f"UNACCOUNTED: {100 * time_unaccounted / time_ms:2.0f}%",
                    )

                    # Update the totals.
                    total_num_ops += num_ops
                    total_time_s += time_ms / 1000.0
                    elapsed_time_s = time.perf_counter() - start_time
                    time_us_per_op = int(1e6 * total_time_s / total_num_ops)

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

            # Show statistics for the completed batch.
            log.info(
                colored(
                    f"TOTAL TRIPLES SO FAR: {total_num_ops:10,}, "
                    f"TOTAL UPDATE TIME SO FAR: {total_time_s:4.0f}s, "
                    f"ELAPSED TIME SO FAR: {elapsed_time_s:4.0f}s, "
                    f"AVG TIME/TRIPLE SO FAR: {time_us_per_op:,}µs",
                    attrs=["bold"],
                )
            )
            log.info("")

            # Close the source connection (for each batch, we open a new one,
            # either from `event_id_for_next_batch` or from `since`).
            source.close()

            # If Ctrl+C was pressed or we reached `--until`, finish.
            if self.ctrl_c_pressed or self.finished:
                break

        # Final statistics after all batches have been processed.
        elapsed_time_s = time.perf_counter() - start_time
        time_us_per_op = (
            int(1e6 * total_time_s / total_num_ops) if total_num_ops > 0 else 0
        )
        log.info(
            f"Processed {batch_count} "
            f"{'batches' if batch_count > 1 else 'batch'} "
            f"terminating update command"
        )
        log.info(
            colored(
                f"TOTAL TRIPLES: {total_num_ops:10,}, "
                f"TOTAL TIME: {total_time_s:4.0f}s, "
                f"ELAPSED TIME: {elapsed_time_s:4.0f}s, "
                f"AVG TIME/TRIPLE: {time_us_per_op:,}µs",
                attrs=["bold"],
            )
        )
        return True
