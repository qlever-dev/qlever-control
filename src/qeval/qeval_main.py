#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures
# Author: Tanmay Garg <gargt@cs.uni-freiburg.de>

from __future__ import annotations

import os
import traceback

from qeval.config import parse_command_line
from qlever.command import execute_command
from qlever.config import ConfigException
from qlever.log import log, log_levels


def main() -> None:
    # Color the output even when stdout is not a terminal (e.g. when piping
    # through `tee`). Setting `NO_COLOR` still disables all colors, because
    # `termcolor` gives it precedence over `FORCE_COLOR`. Note that
    # `termcolor` evaluates these variables on each call to `colored`, so it
    # is enough to set this here (and not before the imports above).
    os.environ.setdefault("FORCE_COLOR", "1")

    # Parse the command line arguments and read the Qleverfile.
    try:
        args = parse_command_line()
    except ConfigException as e:
        log.error(e)
        log.info("")
        log.info(traceback.format_exc())
        exit(1)

    # Execute the command.
    log.setLevel(log_levels[args.log_level])
    execute_command(args)
