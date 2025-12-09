from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        qleverfile_args = super().relevant_qleverfile_arguments()
        if qleverfile_args.get("data"):
            qleverfile_args["data"].append("name")
        else:
            qleverfile_args["data"] = ["name"]
        return qleverfile_args

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = (
                f"{args.host_name}:{args.port}/repositories/{args.name}"
            )
        super().execute(args)
