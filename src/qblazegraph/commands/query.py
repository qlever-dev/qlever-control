from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"data": ["name"], "server": ["port", "access_token"]}

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = (
                f"localhost:{args.port}/blazegraph/namespace/"
                f"{args.name}/sparql"
            )
        super().execute(args)
