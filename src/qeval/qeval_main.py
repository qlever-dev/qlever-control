#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures
# Author: Tanmay Garg <gargt@cs.uni-freiburg.de>

from __future__ import annotations

from qeval.config import parse_command_line
from qlever.qlever_main import execute_command, parse_args


def main() -> None:
    execute_command(parse_args(parse_command_line))
