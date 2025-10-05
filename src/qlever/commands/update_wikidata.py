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
            log.warn("\rCtrl+C pressed again, undoing the previous Ctrl+C")
            self.ctrl_c_pressed = False
        else:
            self.ctrl_c_pressed = True
            log.warn(
                "\rCtrl+C pressed, will finish the current batch and then exit"
                " [press Ctrl+C again to continue]"
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
        lag_seconds_str = (
            f"{args.lag_seconds} second{'s' if args.lag_seconds > 1 else ''}"
        )
        cmd_description = []
        if args.since:
            cmd_description.append(f"SINCE={args.since}")
        else:
            cmd_description.append(
                f"SINCE=$({curl_cmd_updates_complete_until} | sed 1d)"
            )
        cmd_description.append(
            f"Process SSE stream from {args.sse_stream_url}?since=$SINCE "
            f"in batches of {args.batch_size:,} messages "
            f"(less if a message is encountered that is within "
            f"{lag_seconds_str} of the current time)"
        )
        self.show("\n".join(cmd_description), only_show=args.show)
        if args.show:
            return True

        # Compute the `since` date if not given.
        if not args.since:
            try:
                args.since = run_command(
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
        log.warn(
            "Press Ctrl+C to finish the current batch and end gracefully, "
            "press Ctrl+C again to continue with the next batch"
        )
        log.info("")
        log.info(f"SINCE={args.since}")
        log.info("")
        args.sse_stream_url = f"{args.sse_stream_url}?since={args.since}"

        # Initialize the SSE stream and all the statistics variables.
        source = requests_sse.EventSource(
            args.sse_stream_url,
            headers={
                "Accept": "text/event-stream",
                "User-Agent": "qlever update-wikidata",
            },
        )
        source.connect()
        current_batch_size = 0
        batch_count = 0
        total_num_ops = 0
        total_time_s = 0
        start_time = time.perf_counter()
        topics_to_consider = set(args.topics.split(","))
        wait_before_next_batch = False

        # Iterating over all messages in the stream.
        for event in source:
            # Beginning of a new batch of messages.
            if current_batch_size == 0:
                date_list = []
                delta_to_now_list = []
                batch_assembly_start_time = time.perf_counter()
                insert_triples = set()
                delete_triples = set()
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

            # Check if the `args.batch_size` is reached (note that we come here
            # after a `continue` due to an error).
            if self.ctrl_c_pressed:
                break

            # Process the message. Skip messages that are not of type `message`
            # (should not happen), have no field `data` (should not happen
            # either), or where the topic is not in `args.topics`.
            if event.type != "message" or not event.data:
                continue
            event_data = json.loads(event.data)
            topic = event_data.get("meta").get("topic")
            if topic not in topics_to_consider:
                continue

            try:
                # event_id = json.loads(event.last_event_id)
                # date_ms_since_epoch = event_id[0].get("timestamp")
                # date = time.strftime(
                #     "%Y-%m-%dT%H:%M:%SZ",
                #     time.gmtime(date_ms_since_epoch / 1000.0),
                # )
                date = event_data.get("meta").get("dt")
                # date = event_data.get("dt")
                date = re.sub(r"\.\d*Z$", "Z", date)
                # entity_id = event_data.get("entity_id")
                # operation = event_data.get("operation")
                rdf_added_data = event_data.get("rdf_added_data")
                rdf_deleted_data = event_data.get("rdf_deleted_data")
                rdf_linked_shared_data = event_data.get(
                    "rdf_linked_shared_data"
                )
                rdf_unlinked_shared_data = event_data.get(
                    "rdf_unlinked_shared_data"
                )

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
                                data=rdf_to_be_deleted_data, format="turtle"
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
                            rdf_to_be_added_data = rdf_to_be_added.get("data")
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

            # Continue assembling until either the batch size is reached, or
            # we encounter a message that is within `args.lag_seconds` of the
            # current time.
            current_batch_size += 1
            date_as_epoch_s = (
                datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
            now_as_epoch_s = time.time()
            delta_to_now_s = now_as_epoch_s - date_as_epoch_s
            log.debug(
                f"DATE: {date_as_epoch_s:.0f} [{date}], "
                f"NOW: {now_as_epoch_s:.0f}, "
                f"DELTA: {now_as_epoch_s - date_as_epoch_s:.0f}"
            )
            date_list.append(date)
            delta_to_now_list.append(delta_to_now_s)
            if (
                current_batch_size < args.batch_size
                and not self.ctrl_c_pressed
            ):
                if delta_to_now_s < args.lag_seconds:
                    log.warn(
                        f"Encountered message with date {date}, which is within "
                        f"{args.lag_seconds} "
                        f"second{'s' if args.lag_seconds > 1 else ''} "
                        f"of the current time, finishing the current batch"
                    )
                else:
                    continue

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
                f"Processing batch #{batch_count} "
                f"with {current_batch_size:,} "
                f"message{'s' if current_batch_size > 1 else ''}, "
                f"date range: {date_list[0]} - {date_list[-1]}  "
                f"[assembly time: {batch_assembly_time_ms:,} ms, "
                f"min delta to NOW: {min_delta_to_now_s} s]"
            )
            wait_before_next_batch = (
                args.wait_between_batches is not None
                and current_batch_size < args.batch_size
            )
            current_batch_size = 0

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

            # Construct update operation.
            delete_block = " . \n  ".join(delete_triples)
            insert_block = " . \n  ".join(insert_triples)
            delete_insert_operation = (
                f"DELETE {{\n  {delete_block} .\n}} "
                f"INSERT {{\n  {insert_block} .\n}} "
                f"WHERE {{ }}\n"
            )

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
                if isinstance(result, list):
                    result = result[0]
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

            # Helper function for getting the value of `result["time"][...]`
            # without the "ms" suffix.
            def get_time_ms(*keys: str) -> int:
                value = result["time"]
                for key in keys:
                    value = value[key]
                return int(value)
                # return int(re.sub(r"ms$", "", value))

            # Show statistics of the update operation.
            try:
                ins_after = result["delta-triples"]["after"]["inserted"]
                del_after = result["delta-triples"]["after"]["deleted"]
                ops_after = result["delta-triples"]["after"]["total"]
                num_ins = int(result["delta-triples"]["operation"]["inserted"])
                num_del = int(result["delta-triples"]["operation"]["deleted"])
                num_ops = int(result["delta-triples"]["operation"]["total"])
                time_ms = get_time_ms("total")
                time_us_per_op = int(1000 * time_ms / num_ops)
                log.info(
                    colored(
                        f"NUM_OPS: {num_ops:+6,} -> {ops_after:6,}, "
                        f"INS: {num_ins:+6,} -> {ins_after:6,}, "
                        f"DEL: {num_del:+6,} -> {del_after:6,}, "
                        f"TIME: {time_ms:7,} ms, "
                        f"TIME/OP: {time_us_per_op:,} µs",
                        attrs=["bold"],
                    )
                )

                # Also show a detailed breakdown of the total time.
                time_preparation = get_time_ms(
                    "execution", "processUpdateImpl", "preparation"
                )
                time_insert = get_time_ms(
                    "execution", "processUpdateImpl", "insertTriples", "total"
                )
                time_delete = get_time_ms(
                    "execution", "processUpdateImpl", "deleteTriples", "total"
                )
                time_snapshot = get_time_ms("execution", "snapshotCreation")
                time_writeback = get_time_ms("execution", "diskWriteback")
                time_unaccounted = time_ms - (
                    time_delete
                    + time_insert
                    + time_preparation
                    + time_snapshot
                    + time_writeback
                )
                log.info(
                    f"PREPARATION: {100 * time_preparation / time_ms:2.0f}%, "
                    # f"PLANNING: {100 * time_planning / time_ms:2.0f}%, "
                    f"INSERT: {100 * time_insert / time_ms:2.0f}%, "
                    f"DELETE: {100 * time_delete / time_ms:2.0f}%, "
                    f"SNAPSHOT: {100 * time_snapshot / time_ms:2.0f}%, "
                    f"WRITEBACK: {100 * time_writeback / time_ms:2.0f}%, "
                    f"UNACCOUNTED: {100 * time_unaccounted / time_ms:2.0f}%",
                )

                # Show the totals so far.
                total_num_ops += num_ops
                total_time_s += time_ms / 1000.0
                elapsed_time_s = time.perf_counter() - start_time
                time_us_per_op = int(1e6 * total_time_s / total_num_ops)
                log.info(
                    colored(
                        f"TOTAL NUM_OPS SO FAR: {total_num_ops:8,}, "
                        f"TOTAL UPDATE TIME SO FAR: {total_time_s:4.0f} s, "
                        f"ELAPSED TIME SO FAR: {elapsed_time_s:4.0f} s, "
                        f"AVG TIME/OP SO FAR: {time_us_per_op:,} µs",
                        attrs=["bold"],
                    )
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

            # Stop after processing the specified number of batches.
            log.info("")

        # Final statistics after all batches have been processed.
        elapsed_time_s = time.perf_counter() - start_time
        time_us_per_op = int(1e6 * total_time_s / total_num_ops)
        log.info(
            f"Processed {batch_count} "
            f"{'batches' if batch_count > 1 else 'batch'} "
            f"terminating update command"
        )
        log.info(
            colored(
                f"TOTAL NUM_OPS: {total_num_ops:8,}, "
                f"TOTAL TIME: {total_time_s:4.0f} s, "
                f"ELAPSED TIME: {elapsed_time_s:4.0f} s, "
                f"AVG TIME/OP: {time_us_per_op:,} µs",
                attrs=["bold"],
            )
        )
        return True
