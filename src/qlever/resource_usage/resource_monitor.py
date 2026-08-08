from __future__ import annotations

import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path

import psutil

from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    container_memory_to_bytes,
    find_process_by_binary,
    run_command,
)


@dataclass
class Sample:
    """One sample of elapsed time, memory (RSS), and CPU usage; None
    fields are written as empty TSV columns."""

    elapsed_s: float | None = None
    rss: int | None = None
    cpu_percent: float | None = None


def sample_to_tsv_row(sample: Sample) -> str:
    """Format a Sample as a TSV row; None fields become empty columns."""
    values = [getattr(sample, field.name) for field in fields(sample)]
    return "\t".join("" if v is None else str(v) for v in values) + "\n"


def sample_process(proc: psutil.Process) -> Sample:
    """One RSS+CPU read from a psutil.Process; empty Sample on access errors."""
    try:
        mem = proc.memory_info()
        cpu_pct = proc.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return Sample()
    return Sample(rss=mem.rss, cpu_percent=cpu_pct)


def sample_container(system: str, container: str) -> Sample:
    """One RSS+CPU read via `<system> stats --no-stream` on a named container."""
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


class ResourceMonitor:
    """
    Monitor resource usage (memory, CPU) of an index-building
    process. Works in both native mode (via psutil) and container mode
    (via docker/podman stats).

    Usage as a context manager:

        with ResourceMonitor(dataset="wikidata", binary="qlever-index"):
            run_command(cmd, show_output=True)

        # For container mode:
        with ResourceMonitor(dataset="wikidata",
                             binary="qlever-index",
                             container="qlever.index.wikidata",
                             system="docker"):
            run_command(cmd, show_output=True)
    """

    def __init__(
        self,
        dataset: str,
        binary: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
        output_dir: Path | None = None,
        parent_pid: int | None = None,
    ):
        """
        Args:
            dataset:    Name of the dataset being indexed.
            binary:     Name of the index executable, matched against the
                        descendant processes (native mode only).
            container:  Container name to sample; when set with `system`,
                        sampling uses `docker/podman stats` not psutil.
            system:     Container runtime ("docker" or "podman").
            interval:   Seconds between samples.
            output_dir: Directory for the TSV usage log file.
            parent_pid: PID whose descendants are searched for the index
                        process. Defaults to the current process; pass a
                        different PID when the target re-parents away from
                        us.
        """
        self.dataset = dataset
        self.binary = binary
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = output_dir or Path.cwd()
        self.parent_pid = parent_pid
        self.peak_rss = 0
        self.worker_proc = None
        self.log_file = None
        self.stop_event = threading.Event()
        self.start_time = 0

    def take_sample(self) -> Sample:
        """Dispatch to container or native sampling, caching the resolved process."""
        if self.system in Containerize.supported_systems():
            return sample_container(self.system, self.container)
        if self.worker_proc is None or not self.worker_proc.is_running():
            self.worker_proc = find_process_by_binary(
                self.parent_pid, self.binary
            )
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
        """Open the TSV log, write the header, start sampling thread."""
        self.log_path = (
            self.output_dir / f"{self.dataset}.index.resource-usage-log.tsv"
        )
        self.log_file = open(self.log_path, "w")
        header = "\t".join(f.name for f in fields(Sample)) + "\n"
        self.log_file.write(header)
        self.log_file.flush()
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop sampling, close the log, report where it was saved."""
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
