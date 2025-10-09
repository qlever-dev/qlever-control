from __future__ import annotations

from pathlib import Path

import rdflib

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class IndexCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qgraphdb"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name", "format"],
            "index": [
                "input_files",
                "index_binary",
                "threads",
                "jvm_args",
                "entity_index_size",
                "ruleset",
                "extra_args",
            ],
            "server": ["timeout", "read_only"],
            "runtime": [
                "system",
                "image",
                "index_container",
            ],
        }

    def additional_arguments(self, subparser):
        pass

    def update_config_ttl(self, config_dict: dict[str, str]) -> None:
        graph = rdflib.Graph()
        graph.parse(Path.cwd() / "config.ttl", format="ttl")
        for sub, pred, obj in graph:
            pred_str = str(pred).split("#")[1]
            if pred_str in config_dict:
                new_val = rdflib.Literal(config_dict[pred_str])
                graph.remove((sub, pred, obj))
                graph.add((sub, pred, new_val))
        graph.serialize(destination=Path.cwd() / "config.ttl", format="ttl")
        log.info(
            "config.ttl successfully overwritten with relevant Qleverfile entries!"
        )

    @staticmethod
    def construct_config_ttl_dict(args) -> dict[str, str]:
        try:
            timeout = int(args.timeout[:-1])
        except ValueError:
            timeout = 0

        config_dict = {
            "repositoryID": args.name,
            "label": f"{args.name} repository TTL config file",
            "throw-QueryEvaluationException-on-timeout": "true",
            "read-only": "true" if args.read_only == "yes" else "false",
            "query-timeout": str(timeout),
            "ruleset": args.ruleset,
            "entity-index-size": str(args.entity_index_size),
        }

        return config_dict

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand="run --rm",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/opt/graphdb/home")],
            working_directory="/opt/graphdb/home",
        )

    def execute(self, args) -> bool:
        index_cmd = (
            f"{args.index_binary} preload {args.jvm_args} -c config.ttl "
            f"{'-t ' + args.threads if args.threads is not None else ''}"
            f"-Dgraphdb.home={args.name}_index {args.extra_args} {args.input_files}"
        )
        index_cmd += f" | tee {args.name}.index-log.txt"

        if args.system != "native":
            index_cmd = self.wrap_cmd_in_container(args, index_cmd)

        config_dict = self.construct_config_ttl_dict(args)
        log.info(
            "Following options of GraphDB config.ttl will be updated "
            "with the values from Qleverfile as shown below:\n"
        )
        for option, value in config_dict.items():
            log.info(f"{option} = {value}")
        log.info("")

        # Show the command line.
        self.show(index_cmd, only_show=args.show)
        if args.show:
            if not Path("config.ttl").exists():
                log.warning(
                    "config.ttl file not found in current working directory! "
                    "The index command will fail in its absence!"
                )
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(args.input_files, self.script_name):
            return False

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not util.binary_exists(args.index_binary, "index-binary"):
                return False
        else:
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False

        if not Path("config.ttl").exists():
            log.error(
                "config.ttl file not found in the current working directory! "
                f"Did you call {self.script_name} setup-config {args.name}?"
            )
            return False

        # index_dir = Path(f"{args.name}_index/data/repositories/{args.name}")
        # if index_dir.exists():
        #     log.error(
        #         f"Index directory found in {args.name}_index/data/repositories "
        #         "which shows presence of a previous index\n"
        #     )
        #     log.info("Aborting the index operation...")
        #     return False

        # Run the index command.
        try:
            self.update_config_ttl(config_dict)
            util.run_command(index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
