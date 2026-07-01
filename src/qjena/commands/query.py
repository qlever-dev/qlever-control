from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["port", "host_name", "access_token"],
        }

    def execute(self, args, called_from_conformance_test: bool = False) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = (
                f"{args.host_name}:{args.port}/{args.name}/query"
            )
        return super().execute(
            args, called_from_conformance_test=called_from_conformance_test
        )
