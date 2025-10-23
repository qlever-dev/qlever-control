import os
from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


class VisualizeCommand(QleverCommand):
    def __init__(self):
        pass

    def description(self) -> str:
        return "Visualize SPARQL conformance test results."

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {"runtime": ["system"],
                "conformance_ui": ["result_directory", "port"]
                }

    def additional_arguments(self, subparser):
        pass

    def execute(self, args) -> bool:
        dockerfile_dir = Path(__file__).parent.parent
        dockerfile_path = dockerfile_dir / "Dockerfile"
        system = args.system
        uid = f"UID={os.getuid()}" if hasattr(os, "getuid") else "UID=1000"
        gid = f"GID={os.getgid()}" if hasattr(os, "getuid") else "GID=1000"
        build_cmd = f"docker build -f {dockerfile_path} -t visualize-results \
                            --build-arg {uid} --build-arg {gid} {dockerfile_dir}"
        start_server_cmd = f"docker run -it --rm \
                            -p {args.port}:3000 \
                            -v {args.result_directory}:/app/public/results \
                            visualize-results"
        image_id = run_command(
            f"{system} images -q visualize-results", return_output=True
        )
        if not image_id:
            try:
                run_command(build_cmd, show_output=True)
            except Exception as e:
                log.error(f"Building the {system} image visualize-results failed: {e}")
                return False
        try:
            run_command(start_server_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False
        return True