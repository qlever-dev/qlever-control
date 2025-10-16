from __future__ import annotations

import platform
from importlib.metadata import version
from pathlib import Path

import psutil

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import format_size, run_command


def show_heading(text: str) -> str:
    log.info(text)
    log.info("-" * len(text))
    log.info("")


def get_partition(dir: Path):
    """
    Returns the partition on which `dir` resides. May return None.
    """
    # The first partition that whose mountpoint is a parent of `dir` is
    # returned. Sort the partitions by the length of the mountpoint to ensure
    # that the result is correct. Assume there are partitions with mountpoint
    # `/` and `/home`. This ensures that `/home/foo` is detected as being in
    # the partition with mountpoint `/home`.
    partitions = sorted(
        psutil.disk_partitions(),
        key=lambda part: len(part.mountpoint),
        reverse=True,
    )
    for partition in partitions:
        if dir.is_relative_to(partition.mountpoint):
            return partition
    return None


class SystemInfoCommand(QleverCommand):
    def __init__(self):
        pass

    def description(self) -> str:
        return "Gather some system info to help with troubleshooting"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"runtime": ["system", "image", "server_container"]}

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        # Say what the command is doing.
        self.show(
            "Show system information and Qleverfile", only_show=args.show
        )
        if args.show:
            return True

        # Show system information.
        show_heading("System Information")
        system = platform.system()
        is_linux = system == "Linux"
        is_mac = system == "Darwin"
        is_windows = system == "Windows"
        if is_windows:
            log.warn("Only limited information is gathered on Windows.")
        log.info(f"Version: {version('qlever')} (qlever --version)")
        if is_linux:
            info = platform.freedesktop_os_release()
            log.info(f"OS: {platform.system()} ({info['PRETTY_NAME']})")
        else:
            log.info(f"OS: {platform.system()}")
        log.info(f"Arch: {platform.machine()}")
        log.info(f"Host: {platform.node()}")
        psutil.virtual_memory().total / (1000**3)
        memory_total = psutil.virtual_memory().total / (1024.0**3)
        memory_available = psutil.virtual_memory().available / (1024.0**3)
        log.info(
            f"RAM: {memory_total:.1f} GB total, "
            f"{memory_available:.1f} GB available"
        )
        num_cores = psutil.cpu_count(logical=False)
        num_threads = psutil.cpu_count(logical=True)
        cpu_freq = psutil.cpu_freq().max / 1000
        log.info(
            f"CPU: {num_cores} Cores, "
            f"{num_threads} Threads @ {cpu_freq:.2f} GHz"
        )

        cwd = Path.cwd()
        log.info(f"CWD: {cwd}")
        # Free and total size of the partition on which the current working
        # directory resides.
        disk_usage = psutil.disk_usage(str(cwd))
        partition = get_partition(cwd)
        partition_description = f"{partition.device} @ {partition.mountpoint}"
        fs_type = partition.fstype
        fs_free = format_size(disk_usage.free)
        fs_total = format_size(disk_usage.total)
        log.info(
            f"Disk space in {partition_description} is "
            f"({fs_type}): {fs_free} free / {fs_total} total"
        )
        # User/Group on host and in container
        if is_linux or is_mac:
            user_info = run_command("id", return_output=True).strip()
            log.info(f"User and group on host: {user_info}")
        elif is_windows:
            user_info = run_command("whoami /all", return_output=True).strip()
            log.info(f"User and group on host: {user_info}")
        if args.system in Containerize.supported_systems():
            user_info = Containerize.run_in_container("id", args).strip()
            log.info(f"User and group in container: {user_info}")

        # Show Qleverfile.
        log.info("")
        show_heading("Contents of Qleverfile")
        qleverfile = cwd / "Qleverfile"
        if qleverfile.exists():
            # TODO: output the effective qlever file using primites from #57
            log.info(qleverfile.read_text())
        else:
            log.info("No Qleverfile found")
        return True
