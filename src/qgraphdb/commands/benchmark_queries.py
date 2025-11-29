from __future__ import annotations

from qlever.commands.benchmark_queries import (
    BenchmarkQueriesCommand as QleverBenchmarkQueriesCommand,
)


class BenchmarkQueriesCommand(QleverBenchmarkQueriesCommand):
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
        return super().execute(args)
