from __future__ import annotations

import os
import signal
import subprocess
import shlex
import time
from typing import Optional

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command, is_qlever_server_alive, binary_exists, \
    get_total_file_size

from qlever.containerize import Containerize


# Exception to be raised when the user interrupts the command with Ctrl+C or
# Ctrl+Z.
class UserInterruptException(Exception):
    pass


class UpdateOsmCommand(QleverCommand):
    """
    Class for executing the `update-osm` command.
    """

    def __init__(self):
        self.planet_replication_server_url = \
            "https://planet.osm.org/replication/"
        # Remember if Ctrl+C was pressed and if an update is currently running,
        # so we can handle it gracefully.
        self.is_running_update = False
        self.ctrl_c_pressed = False
        # The process which starts the osm-live-updates tool.
        self.olu_process: Optional[subprocess.Popen] = None

    def description(self) -> str:
        return "Update OSM data for a given dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {"data": ["name"],
                "server": ["host_name", "port", "access_token"],
                "runtime": ["system"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--granularity",
            nargs=1,
            choices=["minute", "hour", "day"],
            type=str,
            default=["day"],
            help="The granularity with which the OSM data should be updated. "
                 "Choose from 'minute', 'hour', or 'day'.",
        )
        subparser.add_argument(
            "--once",
            action='store_true',
            default=False,
            help="If set, the OSM data will be updated only once. "
                 "Otherwise, it will be updated continuously at the specified "
                 "granularity.",
        )
        subparser.add_argument(
            "--bbox",
            nargs='?',
            type=str,
            help="The bounding box (LEFT,BOTTOM,RIGHT,TOP) that defines the "
                 "boundaries of your OSM dataset. Not necessary if you want to"
                 " use the complete OSM planet data or if you have already run"
                 " the 'qlever get-polygon' command.",
        )
        subparser.add_argument(
            "--replication-server",
            nargs='?',
            type=str,
            help="The URL of the OSM replication server to use. By default, "
                 "the OSM planet replication server "
                 "('https://planet.osm.org/replication/) is used."
        )
        subparser.add_argument(
            "--olu-image",
            type=str,
            default="docker.io/adfreiburg/olu",
            help="The name of the image used for osm-live-updates.",
        )
        subparser.add_argument(
            "--olu-binary",
            type=str,
            default="osm-live-updates",
            help="The name or path of the compiled `osm-live-updates` binary"
                 " to use when running natively.",
        )
        subparser.add_argument(
            "--polygon",
            type=str,
            default=None,
            help="The name of the file containing the polygon for an OSM "
                 "extract",
        )
        subparser.add_argument(
            "--tmp",
            type=str,
            default="olu_tmp",
            help="The directory to use for temporary files created by olu",
        )
        subparser.add_argument(
            "--olu-statistics",
            action='store_true',
            default=False,
            help="If set, olu will print extensive statistics about the update"
                 " process",
        )

    # Handle Ctrl+C gracefully by finishing the current update and then
    # exiting.
    def handle_ctrl_c(self, signal_received, frame):
        if self.ctrl_c_pressed:
            log.warn("\rCtrl+C pressed again, undoing the previous Ctrl+C")
            self.ctrl_c_pressed = False
        else:
            self.ctrl_c_pressed = True
            if self.is_running_update:
                log.warn("\rCtrl+C pressed, will finish the current update "
                         "and then exit [press Ctrl+C again to continue]")
            else:
                raise UserInterruptException()

    # Handle forceful termination (Ctrl+Z)
    def handle_ctrl_z(self, args, signal_received, frame):
        if self.is_running_update:
            log.error("Ctrl+Z pressed, will kill the current update and exit."
                      "\nThe data may be corrupted if triples where currently "
                      "inserted or deleted.")
        else:
            raise UserInterruptException()

        if self.olu_process and self.olu_process.poll() is None:
            self.olu_process.kill()

        if self.is_running_update:
            Containerize().stop_and_remove_container(args.system,
                                                     f"olu-{args.name}")

        raise UserInterruptException()

    def execute(self, args) -> bool:
        # If the user has specified a replication server, use that one,
        # otherwise we use the planet replication server with the specified
        # granularity.
        granularity = args.granularity[0]
        replications_server: str
        if args.replication_server:
            replication_server = args.replication_server
        else:
            replication_server = (f"{self.planet_replication_server_url}"
                                  f"{granularity}/")

        granularity_in_seconds: int
        if granularity == "minute":
            granularity_in_seconds = 60
        elif granularity == "hour":
            granularity_in_seconds = 3600
        elif granularity == "day":
            granularity_in_seconds = 86400

        cmd_description = [
            f"Update OSM data for dataset '{args.name}' with "
            f"granularity '{granularity}' from the OSM replication"
            f" server '{replication_server}'."]
        self.show("\n".join(cmd_description), only_show=args.show)

        # Handle user interruptions (Ctrl+C) gracefully by waiting for the
        # current update to finish and then exiting.
        signal.signal(signal.SIGINT, self.handle_ctrl_c)
        signal.signal(signal.SIGTSTP,
                      lambda s, f: self.handle_ctrl_z(args, s, f))
        if not args.once and not args.show:
            log.warn(
                "Press Ctrl+C to finish any currently running updates and end "
                "gracefully, press Ctrl+C again to continue\n"
                "Press Ctrl+Z to terminate updates forcefully. Doing so while "
                "triples are being deleted or inserted may corrupt the data.\n"
            )

        # Create command to pull the latest image for osm-live-updates if
        # remote image is used.
        pull_cmd = ""
        if ("/" in args.olu_image and
                args.system in Containerize.supported_systems()):
            pull_cmd = f"{args.system} pull -q {args.olu_image}"
            log.debug(f"Pulling image `{args.olu_image}` for"
                      f" osm-live-updates.")
            self.show(f"{pull_cmd}")

        # Construct the command to run the osm-live-updates tool.
        try:
            olu_cmd = self.construct_olu_cmd(replication_server, args)
            self.show(f"{olu_cmd}")
        except (ValueError, FileNotFoundError) as e:
            log.error(f"{e}")
            return False

        # If the user has specified `--show`, we only show the command and
        # return without executing it.
        if args.show:
            return True

        endpoint_url = f"http://{args.host_name}:{args.port}"
        if not is_qlever_server_alive(endpoint_url):
            log.error(
                f"QLever endpoint at {endpoint_url} is not running."
            )
            return False

        # Create the temporary directory for olu if it does not exist yet.
        if not os.path.exists(args.tmp):
            os.makedirs(args.tmp)

        # Pull the latest image for osm-live-updates if remote image is used.
        if pull_cmd:
            run_command(pull_cmd)

        try:
            while True:
                log.info(f"Starting OSM data update...\n")

                start_time = time.time()

                # Run the osm-live-updates tool in a subprocess,
                # use new_session to avoid that the subprocess receives the
                # Ctrl+C signal.
                self.is_running_update = True
                self.olu_process = run_command(olu_cmd, show_stderr=True,
                                               show_output=True,
                                               use_popen=True,
                                               new_session=True)

                # Wait for the subprocess to finish.
                olu_return_code = self.olu_process.wait()
                self.is_running_update = False
                if olu_return_code != 0:
                    log.error(f"\nOSM data update failed with return code "
                              f"{olu_return_code}.")
                    return False
                else:
                    log.info("\nOSM data update completed successfully.")

                # Check if the user has pressed Ctrl+C during the update.
                if self.ctrl_c_pressed:
                    raise UserInterruptException()

                # If the user has specified `--once`, we exit after the
                # first update.
                if args.once:
                    return True

                # Wait for the next update interval based on the granularity
                # and the time it took to run the previous update.
                elapsed = time.time() - start_time
                sleep_time = max(0, granularity_in_seconds - elapsed)
                if sleep_time > 0:
                    formatted_time = time.strftime('%Hh:%Mm:%Ss',
                                                   time.gmtime(sleep_time))
                    log.info(f"\nWaiting for {formatted_time} until the next "
                             f"update...")
                time.sleep(sleep_time)

        except UserInterruptException:
            log.info("\nOSM data update interrupted by user.")
            return True

        except BaseException as e:
            log.error(f"An error occurred during the OSM data update: {e}")
            return False

    def construct_olu_cmd(self, replication_server_url: str, args) -> str:
        sparql_endpoint = f"http://{args.host_name}:{args.port}"
        container_name = f"olu-{args.name}"

        olu_cmd = f"{sparql_endpoint}"
        olu_cmd += f" --access-token {args.access_token}"
        olu_cmd += f" --replication-server {replication_server_url}"
        olu_cmd += f" --qlever"
        olu_cmd += f" --tmp {args.tmp}"

        if args.olu_statistics:
            olu_cmd += f" --statistics"

        # If the user has specified a boundary, we add it to the command.
        if args.bbox and args.polygon:
            raise ValueError("You cannot specify both --bbox and --polygon. "
                             "Please choose one of them.")
        if args.bbox:
            olu_cmd += f" --bbox {args.bbox}"
        elif args.polygon:
            # Check if the polygon file exists
            if not os.path.exists(args.polygon):
                raise FileNotFoundError(f'No file matching "{args.polygon}"'
                                        f' found.')

            olu_cmd += f" --polygon {args.polygon}"
        # If the user has not specified a bounding box or polygon, we assume
        # the user wants to use the complete OSM planet data.

        if args.system == "native":
            if not binary_exists(args.olu_binary, "olu-binary"):
                # 'binary_exists' will log an error message, so we raise the
                # FileNotFoundError without an additional message.
                raise FileNotFoundError()
            else:
                return f'{args.olu_binary} {olu_cmd}'
        else:
            return Containerize().containerize_command(
                olu_cmd,
                args.system,
                "run --rm",
                args.olu_image,
                container_name,
                volumes=[("$(pwd)", "/update")],
                working_directory="/update",
                use_bash=False
            )