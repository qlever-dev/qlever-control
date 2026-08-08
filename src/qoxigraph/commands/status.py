from __future__ import annotations

from qlever.commands.status import StatusCommand as QleverStatusCommand


class StatusCommand(QleverStatusCommand):
    DEFAULT_REGEX = "oxigraph\\s+serve-read-only"

    def description(self) -> str:
        return "Show Oxigraph processes running on this machine"

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--cmdline-regex",
            default=self.DEFAULT_REGEX,
            help=(
                "Show only processes where the command line matches this regex"
            ),
        )
