from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import get_total_file_size


class IndexStatsCommand(QleverCommand):
    """
    Class for executing the `index-stats` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Breakdown of the time and space used for the index build"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"data": ["name"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--only-time",
            action="store_true",
            default=False,
            help="Show only the time used",
        )
        subparser.add_argument(
            "--only-space",
            action="store_true",
            default=False,
            help="Show only the space used",
        )
        subparser.add_argument(
            "--ignore-text-index",
            action="store_true",
            default=False,
            help="Ignore the text index",
        )
        subparser.add_argument(
            "--time-unit",
            choices=["s", "min", "h", "auto"],
            default="auto",
            help="The time unit",
        )
        subparser.add_argument(
            "--size-unit",
            choices=["B", "MB", "GB", "TB", "auto"],
            default="auto",
            help="The size unit",
        )

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """
        Parse the index build log file and compute the duration of each
        indexing phase. Returns a dict mapping phase names (e.g.
        "Parse input", "TOTAL time") to (duration, unit) tuples. The
        duration is None if the phase timestamps are missing. Returns
        an empty dict on error.
        """

        # Read the content of `log_file_name` into a list of lines.
        try:
            with open(log_file_name, "r") as log_file:
                lines = log_file.readlines()
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}
        # If there is a separate `add-text-index-log.txt` file, append those
        # lines.
        text_log_file_name = f"{args.name}.text-index-log.txt"
        try:
            if Path(text_log_file_name).exists():
                with open(text_log_file_name, "r") as text_log_file:
                    lines.extend(text_log_file.readlines())
        except Exception as e:
            log.error(
                f"Problem reading text index log file "
                f"{text_log_file_name}: {e}"
            )
            return {}

        # Helper function that finds the next line matching the given `regex`,
        # starting from `current_line`, and extracts the time. Returns a tuple
        # of the time and the regex match object.
        #
        # If `update_current_line` is `False`, then `current_line` will not be
        # updated by this call.
        #
        # Otherwise, and this is the default behavior, `current_line` will be
        # updated to the line after the first match, or one beyond the last
        # line if no match is found.
        current_line = 0

        def find_next_line(regex: str, update_current_line: bool = True):
            nonlocal lines
            nonlocal current_line
            current_line_backup = current_line
            # Find starting from `current_line`.
            while current_line < len(lines):
                line = lines[current_line]
                current_line += 1
                timestamp_regex = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
                timestamp_format = "%Y-%m-%d %H:%M:%S"
                regex_match = re.search(regex, line)
                if regex_match:
                    try:
                        return datetime.strptime(
                            re.match(timestamp_regex, line).group(),
                            timestamp_format,
                        ), regex_match
                    except Exception as e:
                        log.error(
                            f"Could not parse timestamp of form "
                            f'"{timestamp_regex}" from line '
                            f' "{line.rstrip()}" ({e})'
                        )
            # If we get here, we did not find a matching line.
            if not update_current_line:
                current_line = current_line_backup
            return None, None

        # Find the lines matching the key_lines_regex and extract the time
        # information from them.
        overall_begin, _ = find_next_line(r"INFO:\s*Processing")
        merge_begin, _ = find_next_line(r"INFO:\s*Merging partial vocab")
        convert_begin, _ = find_next_line(r"INFO:\s*Converting triples")
        perm_begin_and_info = []
        while True:
            # Find the next line that starts a permutation.
            #
            # NOTE: Should work for the old and new format of the index log
            # file (old format: "Creating a pair" + names of permutations in
            # line "Writing meta data for ..."; new format: name of
            # permutations already in line "Creating permutations ...").
            perm_begin, _ = find_next_line(
                r"INFO:\s*Creating a pair", update_current_line=False
            )
            if perm_begin is None:
                perm_begin, perm_info = find_next_line(
                    r"INFO:\s*Creating permutations ([A-Z]+ and [A-Z]+)",
                    update_current_line=False,
                )
            else:
                _, perm_info = find_next_line(
                    r"INFO:\s*Writing meta data for ([A-Z]+ and [A-Z]+)",
                    update_current_line=False,
                )
            if perm_info is None:
                break
            perm_begin_and_info.append((perm_begin, perm_info))
        convert_end = (
            perm_begin_and_info[0][0] if len(perm_begin_and_info) > 0 else None
        )
        normal_end, _ = find_next_line(r"INFO:\s*Index build completed")
        text_begin, _ = find_next_line(
            r"INFO:\s*Adding text index", update_current_line=False
        )
        text_end, _ = find_next_line(
            r"INFO:\s*Text index build comp", update_current_line=False
        )
        if args.ignore_text_index:
            text_begin = text_end = None

        # Check whether at least the first phase is done.
        if overall_begin is None:
            log.error("Missing line that index build has started")
            return {}
        if overall_begin and not merge_begin:
            log.error(
                "According to the log file, the index build "
                "has started, but is still in its first "
                "phase (parsing the input)"
            )
            return {}

        def duration(
            start_end_pairs: list[tuple[datetime | None, datetime | None]],
        ) -> float | None:
            """
            Compute the total duration across all valid (start, end) pairs,
            converted to `time_unit`. Returns None if no pair has both
            timestamps available.
            """
            nonlocal time_unit
            num_start_end_pairs = 0
            diff_seconds = 0
            for start, end in start_end_pairs:
                if start and end:
                    diff_seconds += (end - start).total_seconds()
                    num_start_end_pairs += 1
            if num_start_end_pairs > 0:
                return diff_seconds / self.get_time_unit_factor(time_unit)
            return None

        # Determine the time unit based on the duration of the first phase
        # (parsing), unless explicitly specified.
        parse_duration = None
        if merge_begin and overall_begin:
            parse_duration = (merge_begin - overall_begin).total_seconds()
        time_unit = self.get_time_unit(args.time_unit, parse_duration)

        # Compute durations for each indexing phase. Each entry maps a
        # phase name to (duration_in_time_unit, time_unit).
        durations = {}
        durations["Parse input"] = (
            duration([(overall_begin, merge_begin)]),
            time_unit,
        )
        durations["Build vocabularies"] = (
            duration([(merge_begin, convert_begin)]),
            time_unit,
        )
        durations["Convert to global IDs"] = (
            duration([(convert_begin, convert_end)]),
            time_unit,
        )
        for i in range(len(perm_begin_and_info)):
            perm_begin, perm_info = perm_begin_and_info[i]
            perm_end = (
                perm_begin_and_info[i + 1][0]
                if i + 1 < len(perm_begin_and_info)
                else normal_end
            )
            perm_info_text = (
                perm_info.group(1).replace(" and ", " & ")
                if perm_info
                else f"#{i + 1}"
            )
            durations[f"Permutation {perm_info_text}"] = (
                duration([(perm_begin, perm_end)]),
                time_unit,
            )
        durations["Text index"] = (
            duration([(text_begin, text_end)]),
            time_unit,
        )
        # TOTAL includes the text index time if it was built separately.
        if text_begin and text_end:
            durations["TOTAL time"] = (
                duration(
                    [(overall_begin, normal_end), (text_begin, text_end)]
                ),
                time_unit,
            )
        elif normal_end:
            durations["TOTAL time"] = (
                duration([(overall_begin, normal_end)]),
                time_unit,
            )
        return durations

    @staticmethod
    def get_time_unit(time_unit: str, parse_duration: float | None) -> str:
        """
        Resolve the time unit. If `time_unit` is not "auto", return it
        as-is. Otherwise, pick a unit based on how long the parse phase
        took (seconds if < 200s, minutes if < 1h, hours otherwise).
        """
        if time_unit != "auto":
            return time_unit
        time_unit = "h"
        if parse_duration is not None:
            if parse_duration < 200:
                time_unit = "s"
            elif parse_duration < 3600:
                time_unit = "min"
        return time_unit

    @staticmethod
    def get_time_unit_factor(time_unit: str) -> int:
        """Return the number of seconds per `time_unit`."""
        return {"s": 1, "min": 60, "h": 3600}[time_unit]

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Compute the disk space used by each group of index files. Returns
        a dict mapping display labels (e.g. "Files index.*", "TOTAL size")
        to (size, unit) tuples, where size is already converted to `unit`.
        """
        # Collect raw sizes in bytes.
        sizes = {}
        for size_type in ["index", "vocabulary", "text"]:
            sizes[size_type] = get_total_file_size(
                [f"{args.name}.{size_type}.*"]
            )
        if args.ignore_text_index:
            sizes["text"] = 0
        sizes["total"] = sum(sizes.values())

        # Convert all sizes to the chosen unit.
        size_unit = self.get_size_unit(args.size_unit, sizes["total"])
        unit_factor = self.get_size_unit_factor(size_unit)
        for size_type in sizes:
            sizes[size_type] /= unit_factor

        sizes_to_show = {}
        sizes_to_show["Files index.*"] = (sizes["index"], size_unit)
        sizes_to_show["Files vocabulary.*"] = (sizes["vocabulary"], size_unit)
        if sizes["text"] > 0:
            sizes_to_show["Files text.*"] = (sizes["text"], size_unit)
        sizes_to_show["TOTAL size"] = (sizes["total"], size_unit)
        return sizes_to_show

    @staticmethod
    def get_size_unit(size_unit: str, total_size: int) -> str:
        """
        Resolve the size unit. If `size_unit` is not "auto", return it
        as-is. Otherwise, pick the largest unit that keeps the total
        size >= 1 in that unit.
        """
        if size_unit != "auto":
            return size_unit
        size_unit = "TB"
        if total_size < 1e6:
            size_unit = "B"
        elif total_size < 1e9:
            size_unit = "MB"
        elif total_size < 1e12:
            size_unit = "GB"
        return size_unit

    @staticmethod
    def get_size_unit_factor(size_unit: str) -> int | float:
        """Return the number of bytes per `size_unit`."""
        return {"B": 1, "MB": 1e6, "GB": 1e9, "TB": 1e12}[size_unit]

    def execute(self, args) -> bool:
        return_value = True

        # The "time" part of the command.
        if not args.only_space:
            log_file_name = f"{args.name}.index-log.txt"
            self.show(
                f"Breakdown of the time used for "
                f"building the index, based on the timestamps for key "
                f'lines in "{log_file_name}"',
                only_show=args.show,
            )
            if not args.show:
                durations = self.execute_time(args, log_file_name)
                # Display each phase duration, skipping phases with
                # missing timestamps (duration is None).
                for heading, (duration, time_unit) in durations.items():
                    if duration is not None:
                        if heading == "TOTAL time":
                            log.info("")
                        log.info(
                            f"{heading:<21} : {duration:>6.1f} {time_unit}"
                        )
                return_value &= len(durations) != 0
            if not args.only_time:
                log.info("")

        # The "space" part of the command.
        if not args.only_time:
            self.show(
                "Breakdown of the space used for building the index",
                only_show=args.show,
            )
            if not args.show:
                sizes = self.execute_space(args)
                # Display the disk space used by each group of index files.
                for heading, (size, size_unit) in sizes.items():
                    if heading == "TOTAL size":
                        log.info("")
                    if size_unit == "B":
                        log.info(f"{heading:<21} :  {size:,} {size_unit}")
                    else:
                        log.info(f"{heading:<21} : {size:>6.1f} {size_unit}")
                return_value &= len(sizes) != 0

        return return_value
