from __future__ import annotations

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import binary_exists, run_command


class UpgradeIndexCommand(QleverCommand):
    """
    Class for executing the `upgrade-index` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Upgrade an index in the previous index format to the "
            "format introduced on 2026-09-01 (only this conversion, "
            "older indexes have to be rebuilt)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "index": ["index_binary"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--upgrade-index-binary",
            type=str,
            default=None,
            help="The binary for upgrading the index (default: "
            "`qlever-upgrade-index` from the directory of the "
            "index binary)",
        )

    def execute(self, args) -> bool:
        # By default, take the `qlever-upgrade-index` that sits next to the
        # index binary (which is just `qlever-upgrade-index` from the `PATH`,
        # or from the container image, when the index binary is a plain
        # `qlever-index`).
        upgrade_index_binary = args.upgrade_index_binary
        if upgrade_index_binary is None:
            directory, slash, _ = args.index_binary.rpartition("/")
            upgrade_index_binary = (
                f"{directory}/qlever-upgrade-index"
                if slash
                else "qlever-upgrade-index"
            )

        # Construct the command line.
        upgrade_index_cmd = (
            f"{upgrade_index_binary} {args.name}"
            f" 2>&1 | tee {args.name}.upgrade-index-log.txt"
        )

        # Run the command in a container (if so desired).
        if args.system in Containerize.supported_systems():
            upgrade_index_cmd = Containerize().containerize_command(
                upgrade_index_cmd,
                args.system,
                "run --rm",
                args.image,
                args.index_container,
                volumes=[("$(pwd)", "/index")],
                working_directory="/index",
            )

        # Show the command line.
        self.show(upgrade_index_cmd, only_show=args.show)
        if args.show:
            return True

        if not binary_exists(
            upgrade_index_binary, "upgrade-index-binary", args
        ):
            return False

        # Run the upgrade command.
        try:
            run_command(upgrade_index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Upgrading the index failed: {e}")
            return False

        return True
