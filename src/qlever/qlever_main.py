#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

# Copyright 2024, University of Freiburg,
# Chair of Algorithms and Data Structures
# Author: Hannah Bast <bast@cs.uni-freiburg.de>

from __future__ import annotations

import re
import traceback

from termcolor import colored

from qlever import command_objects, script_name
from qlever.config import ConfigException, QleverConfig
from qlever.log import log, log_levels


def main():
    # Parse the command line arguments and read the Qleverfile.
    try:
        qlever_config = QleverConfig()
        args = qlever_config.parse_args()
    except ConfigException as e:
        log.error(e)
        log.info("")
        log.info(traceback.format_exc())
        exit(1)

    # Execute the command.
    command_object = command_objects[args.command]
    log.setLevel(log_levels[args.log_level])
    try:
        log.info("")
        log.info(colored(f"Command: {args.command}", attrs=["bold"]))
        log.info("")
        command_successful = command_object.execute(args)
        log.info("")
        if not command_successful:
            exit(1)
    except KeyboardInterrupt:
        log.warn("\rCtrl-C pressed, exiting ...")
        log.info("")
        exit(1)
    except Exception as e:
        # Check if it's a certain kind of `AttributeError` and give a hint in
        # that case.
        log.debug(
            "Command failed with exception, full traceback: "
            f"{traceback.format_exc()}"
        )
        match_error = re.search(r"object has no attribute '(.+)'", str(e))
        match_trace = re.search(
            rf"({script_name}/commands/.+\.py)\", line (\d+)",
            traceback.format_exc(),
        )
        if isinstance(e, AttributeError) and match_error and match_trace:
            attribute = match_error.group(1)
            trace_command = match_trace.group(1)
            trace_line = match_trace.group(2)
            log.error(f"{e} in `{trace_command}` at line {trace_line}")
            log.info("")
            log.info(
                f"Likely cause: you used `args.{attribute}`, but it was "
                f"neither defined in `relevant_qleverfile_arguments` "
                f"nor in `additional_arguments`"
            )
            log.info("")
            log.info(
                f"If you did not implement `{trace_command}` yourself, "
                f"please report this issue"
            )
            log.info("")
        else:
            log.error(f"An unexpected error occurred: {e}")
            log.info("")
            log.info(traceback.format_exc())
        exit(1)
