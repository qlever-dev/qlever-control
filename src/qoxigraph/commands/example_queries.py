from __future__ import annotations

from qlever.commands.example_queries import (
    ExampleQueriesCommand as QleverExampleQueriesCommand,
)


class ExampleQueriesCommand(QleverExampleQueriesCommand):
    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/query"
        return super().execute(args)
