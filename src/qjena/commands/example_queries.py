from __future__ import annotations

from qlever.commands.example_queries import (
    ExampleQueriesCommand as QleverExampleQueriesCommand,
)


class ExampleQueriesCommand(QleverExampleQueriesCommand):
    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"data": ["name"], "server": ["host_name", "port"], "ui": ["ui_config"]}

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = (
                f"{args.host_name}:{args.port}/{args.name}/query"
            )
        return super().execute(args)
