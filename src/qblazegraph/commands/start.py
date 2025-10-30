from __future__ import annotations

import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import is_server_alive, run_command


class StartCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qblazegraph"

    def description(self) -> str:
        return (
            "Start the server for Blazegraph (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": [
                "host_name",
                "port",
                "jvm_args",
                "read_only",
                "timeout",
                "extra_args",
            ],
            "runtime": ["system", "image", "server_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--run-in-foreground",
            action="store_true",
            default=False,
            help=(
                "Run the start command in the foreground "
                "(default: run in the background)"
            ),
        )
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
    def overwrite_web_xml(
        xml_file_path: Path, timeout_ms: int, read_only: bool
    ) -> None:
        """
        Overwrite readOnly and queryTimeout parameters in web.xml
        This method could be made more general by making new_value dict
        itself an input parameter to the function. But for now, I could only
        identify readOnly and queryTimeout as the 2 parameters that would
        need updating and it is better to be explicit about it.
        """
        new_values = {
            "queryTimeout": str(timeout_ms),
            "readOnly": str(read_only).lower(),
        }
        ns_uri = "http://java.sun.com/xml/ns/javaee"
        namespace = {"ns": "http://java.sun.com/xml/ns/javaee"}

        # Register the default namespace to avoid ns0 prefixes
        ET.register_namespace("", ns_uri)

        # Parse the XML and preserve comments
        parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
        tree = ET.parse(xml_file_path, parser=parser)
        root = tree.getroot()

        # Find and update values
        for context_param in root.findall("ns:context-param", namespace):
            param_name = context_param.find("ns:param-name", namespace)
            if param_name is not None and param_name.text in new_values:
                param_value = context_param.find("ns:param-value", namespace)
                if param_value is not None:
                    param_value.text = new_values[param_name.text]
        tree.write(xml_file_path, encoding="UTF-8", xml_declaration=True)
        log.info("Successfully updated web.xml.")

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        run_subcommand = "run --restart=unless-stopped"
        if not args.run_in_foreground:
            run_subcommand += " -d"
        if not args.run_in_foreground:
            cmd = f"{cmd} > {args.name}.server-log.txt 2>&1"
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand=run_subcommand,
            image_name=args.image,
            container_name=args.server_container,
            volumes=[("$(pwd)", "/opt/index")],
            working_directory="/opt/index",
            ports=[(args.port, args.port)],
        )

    def execute(self, args) -> bool:
        jar_path = (
            args.blazegraph_jar
            if args.system == "native"
            else "/opt/blazegraph.jar"
        )

        xml_candidates = [Path("web.xml"), Path(f"{args.name}.web.xml")]
        existing_files = [p for p in xml_candidates if p.is_file()]
        if len(existing_files) > 1:
            log.error(
                "Expected exactly one of 'web.xml' or "
                f"'{args.name}.web.xml' in the current directory."
            )
            return False
        web_xml_exists = len(existing_files) == 1
        web_xml_path = existing_files[0] if web_xml_exists else None

        if web_xml_exists:
            log.info(
                f"queryTimeout and readOnly parameters would be overwritten in {web_xml_path.name}"
            )
            if web_xml_path.name == "web.xml":
                log.info(f"web.xml would be renamed to {args.name}.web.xml")
            log.info("")
        start_cmd = (
            f"java -server {args.jvm_args} -Dbigdata.propertyFile=RWStore.properties"
            f"{'' if not web_xml_exists else f' -Djetty.overrideWebXml={args.name}.web.xml'} "
            f"-Djetty.port={args.port} {args.extra_args} -jar {jar_path}"
        )

        if args.system == "native":
            if not args.run_in_foreground:
                start_cmd = (
                    f"nohup {start_cmd} > {args.name}.server-log.txt 2>&1 &"
                )
        else:
            start_cmd = self.wrap_cmd_in_container(args, start_cmd)

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            try:
                run_command("java --help")
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

        jnl_file = Path("blazegraph.jnl")
        if not jnl_file.exists():
            log.info(f"No Blazegraph journal for {args.name} found! ")
            log.info(
                f"Did you call `{self.script_name} index`? If you did, check "
                "if blazegraph.jnl is present in the current working directory"
            )
            return False

        endpoint_url = f"http://{args.host_name}:{args.port}/blazegraph"
        if is_server_alive(url=endpoint_url):
            log.error(f"Blazegraph server already running on {endpoint_url}\n")
            log.info(
                f"To kill the existing server, use `{self.script_name} stop`"
            )
            return False

        try:
            timeout_ms = int(args.timeout[:-1]) * 1000
        except ValueError as e:
            log.error(f"Invalid timeout value {args.timeout}. Error: {e}")
            return False

        try:
            if web_xml_exists:
                read_only = True if args.read_only == "yes" else False
                self.overwrite_web_xml(web_xml_path, timeout_ms, read_only)
                if web_xml_path.name == "web.xml":
                    Path("web.xml").rename(f"{args.name}.web.xml")
                    log.info(
                        f"Successfully renamed web.xml to {args.name}.web.xml\n"
                    )
        except Exception as e:
            log.error(
                f"Overwriting web.xml with Qleverfile parameters failed: {e}"
            )
            return False

        # Run the start command.
        try:
            process = run_command(
                start_cmd,
                use_popen=args.run_in_foreground,
            )
        except Exception as e:
            log.error(f"Starting the Jena server failed ({e})")
            return False

        # Tail the server log until the server is ready (note that the `exec`
        # is important to make sure that the tail process is killed and not
        # just the bash process).
        if args.run_in_foreground:
            log.info(
                "Follow the server logs as long as the server is"
                " running (Ctrl-C stops the server)"
            )
        else:
            log.info(
                "Follow the server logs until the server is ready"
                " (Ctrl-C stops following the log, but NOT the server)"
            )
        log.info("")
        log_cmd = f"exec tail -f {args.name}.server-log.txt"
        log_proc = subprocess.Popen(log_cmd, shell=True)
        while not is_server_alive(endpoint_url):
            time.sleep(1)

        log.info(
            f"Blazegraph server webapp for {args.name} will be available at "
            f"http://{args.host_name}:{args.port} and the sparql endpoint for "
            f"queries is {endpoint_url}/namespace/kb/sparql"
        )

        # Kill the log process
        if not args.run_in_foreground:
            log_proc.terminate()

        # With `--run-in-foreground`, wait until the server is stopped.
        if args.run_in_foreground:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
            log_proc.terminate()

        return True
