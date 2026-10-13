from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path

import psutil

from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import resource_usage_prefix, run_command


@dataclass
class Sample:
    """
    One sample of elapsed time, memory (RSS) and CPU usage. A field that
    is None is written as an empty TSV column.
    """

    elapsed_s: float | None = None
    rss: int | None = None
    cpu_percent: float | None = None


def sample_to_tsv_row(sample: Sample) -> str:
    """Format a `Sample` as a TSV row."""
    values = [getattr(sample, field.name) for field in fields(sample)]
    return (
        "\t".join("" if value is None else str(value) for value in values)
        + "\n"
    )


def sample_process(proc: psutil.Process) -> Sample:
    """
    One reading of the memory and CPU usage of `proc`. Empty if the
    process is gone or cannot be read.
    """
    try:
        mem = proc.memory_info()
        cpu_pct = proc.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return Sample()
    return Sample(rss=mem.rss, cpu_percent=cpu_pct)


def container_memory_to_bytes(memory_string: str) -> int:
    """
    Parse a memory usage string from `docker stats` or `podman stats`
    into bytes.  Docker reports binary units (GiB, MiB) while Podman
    reports decimal units (GB, MB).
    """
    memory_string = memory_string.strip()
    # Order matters: the first endswith match wins, and "B" is a suffix
    # of every other unit, so it must stay last.
    units = {
        "TIB": 1024**4,
        "TB": 1000**4,
        "GIB": 1024**3,
        "GB": 1000**3,
        "MIB": 1024**2,
        "MB": 1000**2,
        "KIB": 1024,
        "KB": 1000,
        "B": 1,
    }
    for suffix, multiplier in units.items():
        if memory_string.upper().endswith(suffix):
            number = float(memory_string[: len(memory_string) - len(suffix)])
            return int(number * multiplier)
    return 0


def sample_container(system: str, container: str) -> Sample:
    """
    One reading of the memory and CPU usage of `container`, via
    `<system> stats --no-stream`. Empty if it cannot be read.
    """
    try:
        output = run_command(
            f"{system} stats --no-stream"
            f" --format '{{{{.MemUsage}}}}\t{{{{.CPUPerc}}}}'"
            f" {container}",
            return_output=True,
        )
        memory_field, cpu_field = output.strip().split("\t")
        used_memory = memory_field.split("/")[0].strip()
        cpu_percent = float(cpu_field.strip().rstrip("%"))
        return Sample(
            rss=container_memory_to_bytes(used_memory),
            cpu_percent=cpu_percent,
        )
    except Exception:
        return Sample()


def find_process_by_binary(
    parent_pid: int | None, binary: str
) -> psutil.Process | None:
    """Find a descendant of parent_pid whose argv[0] basename matches binary."""
    try:
        root = psutil.Process(parent_pid)
        candidates = [root] + root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None
    # Both sides use Path(...).name so any path form (relative, absolute,
    # bare, symlink) matches as long as argv[0] preserves the binary's name.
    target = Path(binary).name
    for proc in candidates:
        try:
            cmdline = proc.cmdline()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if cmdline and Path(cmdline[0]).name == target:
            return proc
    return None


class ResourceMonitor:
    """
    Monitor resource usage (memory, CPU) of an index-building
    process. Works in both native mode (via psutil) and container mode
    (via docker/podman stats). Use it as a context manager around the
    index command.
    """

    def __init__(
        self,
        dataset: str,
        engine: str,
        binary: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
    ):
        """
        Args:
            dataset:    Name of the dataset being indexed.
            engine:     Engine key, which the log and plot names start with.
            binary:     Name of the index executable, matched against the
                        descendant processes (native mode only).
            container:  Container name to sample. When set with `system`,
                        sampling uses `docker/podman stats`, not psutil.
            system:     Container runtime ("docker" or "podman").
            interval:   Seconds between samples.
        """
        self.dataset = dataset
        self.engine = engine
        self.binary = binary
        self.container = container
        self.system = system
        self.interval = interval
        self.peak_rss = 0
        self.worker_proc = None
        self.log_path = None
        self.log_file = None
        self.stop_event = threading.Event()
        self.start_time = 0.0
        self.thread = None

    @classmethod
    def from_args(cls, args) -> ResourceMonitor:
        """Monitor the index build configured by `args`."""
        return cls(
            dataset=args.name,
            engine=args.engine_short_name,
            binary=args.index_binary,
            container=args.index_container,
            system=args.system,
            interval=args.resource_usage_interval,
        )

    def take_sample(self) -> Sample:
        """
        Dispatch to container or native sampling, caching the resolved
        process.
        """
        if self.system in Containerize.supported_systems():
            return sample_container(self.system, self.container)
        if self.worker_proc is None or not self.worker_proc.is_running():
            self.worker_proc = find_process_by_binary(os.getpid(), self.binary)
            if self.worker_proc is None:
                return Sample()
            # cpu_percent reports usage since the previous call, so this
            # first call seeds the baseline and its 0.0 result is discarded.
            try:
                self.worker_proc.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self.worker_proc = None
                return Sample()
        return sample_process(self.worker_proc)

    def run_loop(self):
        """
        Polling loop on a background thread. Samples resource usage
        and appends one TSV row per iteration until stop_event is set.
        """
        while not self.stop_event.is_set():
            sample = self.take_sample()
            sample.elapsed_s = round(time.monotonic() - self.start_time, 1)
            if sample.rss is not None and self.log_file is not None:
                self.peak_rss = max(self.peak_rss, sample.rss)
                self.log_file.write(sample_to_tsv_row(sample))
                self.log_file.flush()
            self.stop_event.wait(self.interval)

    def __enter__(self):
        """Open the TSV log, write its header, and start the sampling thread."""
        prefix = resource_usage_prefix(self.engine, self.dataset)
        self.log_path = Path(f"{prefix}.index.resource-usage-log.tsv")
        self.log_file = open(self.log_path, "w")
        header = "\t".join(field.name for field in fields(Sample)) + "\n"
        self.log_file.write(header)
        self.log_file.flush()
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop sampling and close the log, reporting where it was saved."""
        self.stop_event.set()
        self.thread.join()
        self.log_file.close()
        if self.peak_rss > 0:
            log.info(
                "Resource-usage log (RSS memory and CPU usage) saved to "
                f"`{self.log_path.name}`"
            )
        else:
            log.warning(
                "Resource usage was not recorded (no samples collected)."
            )
        return False
