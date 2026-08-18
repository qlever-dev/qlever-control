from __future__ import annotations

from importlib import import_module
from typing import NamedTuple


class Engine(NamedTuple):
    """
    The package with the commands of an engine, and the name of the engine as
    shown to the user.
    """

    package: str
    display_name: str


ENGINES = {
    "qlever": Engine(package="qlever", display_name="QLever"),
}


def add_engine_qleverfile_args(engine: str, all_args: dict) -> None:
    """
    Add the engine's own Qleverfile arguments to qlever's `all_args`,
    overriding the shared ones where they differ.
    """
    # `all_args` comes from `qlever` and already holds its arguments.
    if engine == "qlever":
        return
    package = ENGINES[engine].package
    import_module(f"{package}.qleverfile").qleverfile_args(all_args)
