from __future__ import annotations

from importlib import import_module
from typing import NamedTuple


class Engine(NamedTuple):
    """How to find an engine: importable package string and its display name"""

    package: str
    display_name: str


ENGINES = {
    "qlever": Engine(package="qlever", display_name="QLever"),
}


def add_engine_qleverfile_args(
    engine: str, all_args: dict[str, dict[str, tuple]]
) -> None:
    """
    Add the engine's own Qleverfile arguments to qlever's `all_args`,
    overriding the shared ones where they differ.
    """
    package = ENGINES[engine].package
    if package == "qlever":
        return
    import_module(f"{package}.qleverfile").qleverfile_args(all_args)
