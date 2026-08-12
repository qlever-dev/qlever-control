from __future__ import annotations

from importlib import import_module
from pathlib import Path

from qlever.command import QleverCommand


# Helper function to turn "snake_case" into "CamelCase".
def snake_to_camel(str):
    # Split by _ and - and capitalize each word.
    return "".join([w.capitalize() for w in str.replace("-", "_").split("_")])


def load_commands(package: str) -> dict[str, QleverCommand]:
    """
    Create one command object per module in `<package>/commands`, keyed by the
    module name with `_` replaced by `-`.
    """
    package_path = Path(import_module(package).__file__).parent
    command_names = [
        p.stem
        for p in package_path.glob("commands/*.py")
        if p.name != "__init__.py"
    ]

    # Dynamically load all the command classes and create an object for each.
    command_objects = {}
    for command_name in command_names:
        module_path = f"{package}.commands.{command_name}"
        try:
            module = import_module(module_path)
        except ImportError as e:
            raise Exception(
                f"Could not import module {module_path}: {e}"
            ) from e
        # Create an object of the class and store it in the dictionary. For the
        # commands, take - instead of _.
        class_name = snake_to_camel(command_name) + "Command"
        command_class = getattr(module, class_name)
        command_objects[command_name.replace("_", "-")] = command_class()
    return command_objects
