from __future__ import annotations

from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class IndexCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qblazegraph"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name", "format"],
            "index": ["input_files", "jvm_args", "extra_args"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--blazegraph-jar",
            type=str,
            default="blazegraph.jar",
            help=(
                "Path to blazegraph.jar file (default: blazegraph.jar) "
                "(this requires that you have Java installed and blazegraph.jar "
                "downloaded on your machine)"
            ),
        )

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand="run --rm",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/opt/index")],
            working_directory="/opt/index",
        )

    def execute(self, args) -> bool:
        system = args.system
        input_files = args.input_files

        jar_path = (
            args.blazegraph_jar
            if args.system == "native"
            else "/opt/blazegraph.jar"
        )

        index_cmd = (
            f"java {args.jvm_args} -cp {jar_path} com.bigdata.rdf.store.DataLoader "
            f"{args.extra_args} RWStore.properties {input_files}"
        )
        index_cmd += f" | tee {args.name}.index-log.txt"

        if args.system == "native":
            cmd_to_show = index_cmd
        else:
            index_cmd = self.wrap_cmd_in_container(args, index_cmd)
            dockerfile_dir = Path(__file__).parent.parent
            dockerfile_path = dockerfile_dir / "Dockerfile"
            build_cmd = (
                f"{system} build -f {dockerfile_path} -t {args.image} --build-arg "
                f"UID=$(id -u) --build-arg GID=$(id -g) {dockerfile_dir}"
            )
            image_id = util.get_container_image_id(system, args.image) 
            cmd_to_show = (
                f"{build_cmd}\n\n{index_cmd}" if not image_id else index_cmd
            )

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(input_files, self.script_name):
            return False

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            try:
                util.run_command("java --help")
            except Exception as e:
                log.error(f"Java not found on the machine! - {e}")
                log.info(
                    "Blazegraph needs Java to execute the blazegraph.jar file"
                )
                return False
            if not Path(args.blazegraph_jar).exists():
                jar_link = (
                    "https://github.com/blazegraph/database/releases/download/"
                    "BLAZEGRAPH_2_1_6_RC/blazegraph.jar"
                )
                log.error(
                    "Couldn't find the blazegraph.jar in specified path: "
                    f"{Path(args.blazegraph_jar).absolute()}\n"
                )
                log.info(
                    "Are you sure you downloaded the blazegraph.jar file? "
                    f"blazegraph.jar can be downloaded from {jar_link}"
                )
                return False
        else:
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False

            if not image_id:
                build_successful = util.build_image(
                    build_cmd, system, args.image
                )
                if not build_successful:
                    return False
            else:
                log.info(f"{args.image} image present on the system\n")

        index_jnl = Path("blazegraph.jnl")
        if index_jnl.exists():
            log.error(
                "Blazegraph journal blazegraph.jnl found in current working "
                "directory which shows presence of a previous index\n"
            )
            log.info("Aborting the index operation...")
            return False

        # Run the index command.
        try:
            util.run_command(index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
