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

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"runtime": ["system"],
                "conformance_ui": ["result_directory", "port", "ui_branch"]
                }

    def additional_arguments(self, subparser):
        pass

    def execute(self, args) -> bool:
        compose_file = Path(__file__).parent.parent / "docker-compose.yml"
        system = args.system
        result_dir = (
            Path.cwd() if args.result_directory == "$(pwd)"
            else Path(args.result_directory).resolve()
        )
        port = args.port
        branch = args.ui_branch

        compose_cmd = (
            f'LOCAL_RESULTS_DIR="{result_dir}" '
            f'PRIVATE_WEB_PORT="{port}" '
            f'SPARQL_CONFORMANCE_UI_BRANCH="{branch}" '
            f'{system} compose -f {compose_file}'
        )

        # Remove any previous run's DB volume for a clean start.
        run_command(f"{compose_cmd} down -v", show_stderr=True)

        # Images are tagged by branch; build only when the tag is missing.
        web_id = run_command(
            f"{system} images -q sparql-conformance-web:{branch}",
            return_output=True
        )
        api_id = run_command(
            f"{system} images -q sparql-conformance-api:{branch}",
            return_output=True
        )
        if not web_id or not api_id:
            log.info(
                f"Building sparql-conformance-ui images from branch '{branch}' "
                "(first run for this branch, may take a few minutes)..."
            )
            try:
                run_command(f"{compose_cmd} build", show_output=True)
            except Exception as e:
                log.error(f"Building the images failed: {e}")
                return False

        log.info(f"Starting visualization at http://localhost:{port}")
        try:
            run_command(f"{compose_cmd} up", show_output=True)
        except Exception as e:
            log.error(f"Starting visualization failed: {e}")
            return False
        finally:
            run_command(f"{compose_cmd} down -v", show_stderr=True)

        return True
