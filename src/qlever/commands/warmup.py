from __future__ import annotations

import subprocess

from qlever.command import QleverCommand
from qlever.log import log


class WarmupCommand(QleverCommand):
    """
    Class for executing the `warmup` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Execute WARMUP_CMD"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["port", "warmup_cmd"]}

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        # Show what the command is doing.
        self.show(args.warmup_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the command.
        try:
            subprocess.run(args.warmup_cmd, shell=True, check=True)
        except Exception as e:
            log.error(f"{e.output if hasattr(e, 'output') else e}")
            return False
        return True
