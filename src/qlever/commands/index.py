from __future__ import annotations

import glob
import json
import re
import shlex

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    binary_exists,
    get_existing_index_files,
    get_total_file_size,
    run_command,
)


class IndexCommand(QleverCommand):
    """
    Class for executing the `index` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name", "format"],
            "index": [
                "input_files",
                "cat_input_files",
                "encode_as_id",
                "multi_input_json",
                "parallel_parsing",
                "settings_json",
                "vocabulary_type",
                "index_binary",
                "only_pso_and_pos_permutations",
                "ulimit",
                "use_patterns",
                "text_index",
                "stxxl_memory",
                "parser_buffer_size",
            ],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--overwrite-existing",
            action="store_true",
            default=False,
            help="Overwrite an existing index, think twice before using this",
        )

    # Exception for invalid JSON.
    class InvalidInputJson(Exception):
        def __init__(self, error_message, additional_info):
            self.error_message = error_message
            self.additional_info = additional_info
            super().__init__()

    # Helper function to get command line options from JSON.
    def get_input_options_for_json(self, args) -> str:
        # Parse the JSON. If `args.multi_input_json` look like JSONL, turn
        # it into a JSON array.
        try:
            jsonl_line_regex = re.compile(r"^\s*\{.*\}\s*$")
            jsonl_lines = args.multi_input_json.split("\n")
            if all(re.match(jsonl_line_regex, line) for line in jsonl_lines):
                args.multi_input_json = "[" + ", ".join(jsonl_lines) + "]"
            input_specs = json.loads(args.multi_input_json)
        except Exception as e:
            raise self.InvalidInputJson(
                f"Failed to parse `MULTI_INPUT_JSON` as either JSON or JSONL ({e})",
                args.multi_input_json,
            )
        # Check that it is an array of length at least one.
        if not isinstance(input_specs, list):
            raise self.InvalidInputJson(
                "`MULTI_INPUT_JSON` must be a JSON array",
                args.multi_input_json,
            )
        if len(input_specs) == 0:
            raise self.InvalidInputJson(
                "`MULTI_INPUT_JSON` must contain at least one element",
                args.multi_input_json,
            )
        # For each of the maps, construct the corresponding command-line
        # options to the index binary.
        input_options = []
        for i, input_spec in enumerate(input_specs):
            # Check that `input_spec` is a dictionary.
            if not isinstance(input_spec, dict):
                raise self.InvalidInputJson(
                    f"Element {i} in `MULTI_INPUT_JSON` must be a JSON "
                    "object",
                    input_spec,
                )
            # For each `input_spec`, we must have a command.
            if "cmd" not in input_spec:
                raise self.InvalidInputJson(
                    f"Element {i} in `MULTI_INPUT_JSON` must contain a "
                    "key `cmd`",
                    input_spec,
                )
            # If the command contains a `{}` placeholder, we need a `for-each`
            # key` specifying the pattern for the placeholder values, and vice
            # versa.
            if "{}" in input_spec["cmd"] and "for-each" not in input_spec:
                raise self.InvalidInputJson(
                    f"Element {i} in `MULTI_INPUT_JSON` must contain a "
                    "key `for-each` if the command contains a placeholder "
                    "`{}`",
                    input_spec,
                )
            if "for-each" in input_spec and "{}" not in input_spec["cmd"]:
                raise self.InvalidInputJson(
                    f"Element {i} in `MULTI_INPUT_JSON` contains a "
                    "key `for-each`, but the command does not contain a "
                    "placeholder `{{}}`",
                    input_spec,
                )
            # Get all commands. This is just the value of the `cmd` key if no
            # `for-each` key is specified. Otherwise, we have a command for
            # each file matching the pattern.
            if "for-each" not in input_spec:
                input_cmds = [input_spec["cmd"]]
            else:
                try:
                    files = sorted(glob.glob(input_spec["for-each"]))
                except Exception as e:
                    raise self.InvalidInputJson(
                        f"Element {i} in `MULTI_INPUT_JSON` contains an "
                        f"invalid `for-each` pattern: {e}",
                        input_spec,
                    )
                input_cmds = [input_spec["cmd"].format(file) for file in files]
            # The `format`, `graph`, and `parallel` keys are optional.
            input_format = input_spec.get("format", args.format)
            input_graph = input_spec.get("graph", "-")
            input_parallel = input_spec.get("parallel", "false")
            # There must not be any other keys.
            extra_keys = input_spec.keys() - {
                "cmd",
                "format",
                "graph",
                "parallel",
                "for-each",
            }
            if extra_keys:
                raise self.InvalidInputJson(
                    f"Element {i} in `MULTI_INPUT_JSON` must only contain "
                    "the keys `format`, `graph`, and `parallel`. Contains "
                    f"extra keys {extra_keys}.",
                    input_spec,
                )
            # Add the command-line options for this input stream. We use
            # process substitution `<(...)` as a convenient way to handle an
            # input stream just like a file. This is not POSIX compliant, but
            # supported by various shells, including bash and zsh. If
            # `for-each` is specified, add one command for each matching file.
            for input_cmd in input_cmds:
                input_option = f"-f <({input_cmd}) -g {input_graph}"
                input_option += f" -F {input_format}"
                if input_parallel == "true":
                    input_option += " -p true"
                else:
                    input_option += " -p false"
                input_options.append(input_option)
        # Return the concatenated command-line options.
        return " ".join(input_options)

    def execute(self, args) -> bool:
        # The mandatory part of the command line (specifying the input, the
        # basename of the index, and the settings file). There are two ways
        # to specify the input: via a single stream or via multiple streams.
        if args.cat_input_files and not args.multi_input_json:
            index_cmd = (
                f"{args.cat_input_files} | {args.index_binary}"
                f" -i {args.name} -s {args.name}.settings.json"
                f" --vocabulary-type {args.vocabulary_type}"
                f" -F {args.format} -f -"
            )
            if args.parallel_parsing:
                index_cmd += f" -p {args.parallel_parsing}"
        elif args.multi_input_json and not args.cat_input_files:
            try:
                input_options = self.get_input_options_for_json(args)
            except self.InvalidInputJson as e:
                log.error(e.error_message)
                log.info("")
                log.info(e.additional_info)
                return False
            index_cmd = (
                f"{args.index_binary}"
                f" -i {args.name} -s {args.name}.settings.json"
                f" --vocabulary-type {args.vocabulary_type}"
                f" {input_options}"
            )
        else:
            log.error(
                "Specify exactly one of `CAT_INPUT_FILES` (for a "
                "single input stream) or `MULTI_INPUT_JSON` (for "
                "multiple input streams)"
            )
            log.info("")
            log.info("See `qlever index --help` for more information")
            return False

        # Add remaining options.
        if args.encode_as_id:
            index_cmd += f" --encode-as-id {args.encode_as_id}"
        if args.only_pso_and_pos_permutations:
            index_cmd += " --only-pso-and-pos-permutations --no-patterns"
        if not args.use_patterns:
            index_cmd += " --no-patterns"
        if args.text_index in [
            "from_text_records",
            "from_text_records_and_literals",
        ]:
            index_cmd += (
                f" -w {args.name}.wordsfile.tsv"
                f" -d {args.name}.docsfile.tsv"
            )
        if args.text_index in [
            "from_literals",
            "from_text_records_and_literals",
        ]:
            index_cmd += " --text-words-from-literals"
        if args.stxxl_memory:
            index_cmd += f" --stxxl-memory {args.stxxl_memory}"
        if args.parser_buffer_size:
            index_cmd += f" --parser-buffer-size {args.parser_buffer_size}"
        index_cmd += f" | tee {args.name}.index-log.txt"

        # If the total file size is larger than 10 GB, set ulimit (such that a
        # large number of open files is allowed).
        total_file_size = get_total_file_size(shlex.split(args.input_files))
        if args.ulimit is not None:
            index_cmd = f"ulimit -Sn {args.ulimit} && {index_cmd}"
        elif total_file_size > 1e10:
            index_cmd = f"ulimit -Sn 500000 && {index_cmd}"

        # Run the command in a container (if so desired).
        if args.system in Containerize.supported_systems():
            index_cmd = Containerize().containerize_command(
                index_cmd,
                args.system,
                "run --rm",
                args.image,
                args.index_container,
                volumes=[("$(pwd)", "/index")],
                working_directory="/index",
            )

        # Command for writing the settings JSON to a file.
        settings_json_cmd = (
            f"echo {shlex.quote(args.settings_json)} "
            f"> {args.name}.settings.json"
        )

        # Show the command line.
        self.show(f"{settings_json_cmd}\n{index_cmd}", only_show=args.show)
        if args.show:
            return True

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not binary_exists(args.index_binary, "index-binary"):
                return False

        # Check if all of the input files exist.
        for pattern in shlex.split(args.input_files):
            if len(glob.glob(pattern)) == 0:
                log.error(f'No file matching "{pattern}" found')
                log.info("")
                log.info(
                    "Did you call `qlever get-data`? If you did, check "
                    "GET_DATA_CMD and INPUT_FILES in the QLeverfile"
                )
                return False

        # Check if index files (name.index.*) already exist.
        existing_index_files = get_existing_index_files(args.name)
        if len(existing_index_files) > 0 and not args.overwrite_existing:
            log.error(
                f'Index files for basename "{args.name}" found, if you '
                f"want to overwrite them, use --overwrite-existing"
            )
            log.info("")
            log.info(f"Index files found: {existing_index_files}")
            return False

        # Remove already existing container.
        if (
            args.system in Containerize.supported_systems()
            and args.overwrite_existing
        ):
            if Containerize.is_running(args.system, args.index_container):
                log.info(
                    "Another index process is running, trying to stop "
                    "it ..."
                )
                log.info("")
                try:
                    run_command(f"{args.system} rm -f {args.index_container}")
                except Exception as e:
                    log.error(f"Removing existing container failed: {e}")
                    return False

        # Write settings.json file.
        try:
            run_command(settings_json_cmd)
        except Exception as e:
            log.error(f"Writing the settings.json file failed: {e}")
            return False

        # Run the index command.
        try:
            run_command(index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
