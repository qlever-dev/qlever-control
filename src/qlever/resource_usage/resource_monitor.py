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
    resource_usage_prefix,
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
    """
    One RSS+CPU read from a psutil.Process; empty Sample on access errors.
    """
    try:
        mem = proc.memory_info()
        cpu_pct = proc.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return Sample()
    return Sample(rss=mem.rss, cpu_percent=cpu_pct)


def sample_container(system: str, container: str) -> Sample:
    """
    One RSS+CPU read via `<system> stats --no-stream` on a named container.
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


def read_last_elapsed_s(log_path: Path) -> float | None:
    """
    Read the `elapsed_s` of the last sample of an existing usage log, so
    that a further run can continue from it; 0.0 if the log has a header
    but no samples yet. None if the log does not start with a header row,
    in which case it holds nothing worth keeping and can be overwritten.
    """
    lines = log_path.read_text().splitlines()
    if not lines or lines[0].split("\t")[0] != fields(Sample)[0].name:
        return None
    for line in reversed(lines[1:]):
        elapsed_s = line.split("\t")[0]
        if elapsed_s:
            try:
                return float(elapsed_s)
            except ValueError:
                continue
    return 0.0


class ResourceMonitor:
    """
    Monitor resource usage (memory, CPU) of an index-building
    process. Works in both native mode (via psutil) and container mode
    (via docker/podman stats).

    Usage as a context manager:

        with ResourceMonitor(dataset="wikidata", engine="oxigraph",
                            binary="oxigraph"):
            run_command(cmd, show_output=True)

        # For container mode:
        with ResourceMonitor(dataset="wikidata",
                             engine="oxigraph",
                             binary="oxigraph",
                             container="oxigraph.index.wikidata",
                             system="docker"):
            run_command(cmd, show_output=True)
    """

    def __init__(
        self,
        dataset: str,
        engine: str,
        binary: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
        output_dir: Path | None = None,
        parent_pid: int | None = None,
        append: bool = False,
    ):
        """
        Args:
            dataset:    Name of the dataset being indexed.
            engine:     Engine key, which the log and plot names start with.
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
            append:     Add this run's samples to an existing usage log
                        instead of overwriting it, continuing `elapsed_s`
                        from its last row. For engines that build an index
                        in several runs. A run that raises is rolled back.
        """
        self.dataset = dataset
        self.engine = engine
        self.binary = binary
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = output_dir or Path.cwd()
        self.parent_pid = parent_pid
        self.append = append
        self.peak_rss = 0
        self.worker_proc = None
        self.log_file = None
        self.stop_event = threading.Event()
        self.start_time = 0
        # Set in `__enter__` when appending to an existing log: the
        # `elapsed_s` to continue from, and the size the file had before
        # this run, which `__exit__` truncates back to on failure.
        self.elapsed_offset = 0.0
        self.append_offset = None

    @classmethod
    def from_args(cls, args) -> ResourceMonitor:
        """Monitor the index build configured by `args`."""
        return cls(
            dataset=args.name,
            engine=args.engine,
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
            sample.elapsed_s = round(
                self.elapsed_offset + time.monotonic() - self.start_time, 1
            )
            if sample.rss is not None and self.log_file is not None:
                self.peak_rss = max(self.peak_rss, sample.rss)
                self.log_file.write(sample_to_tsv_row(sample))
                self.log_file.flush()
            self.stop_event.wait(self.interval)

    def __enter__(self):
        """
        Open the TSV log and start the sampling thread. Writes a header to
        a fresh log; continues an existing one when `append` was set.
        """
        prefix = resource_usage_prefix(self.engine, self.dataset)
        self.log_path = (
            self.output_dir / f"{prefix}.index.resource-usage-log.tsv"
        )
        previous_elapsed_s = (
            read_last_elapsed_s(self.log_path)
            if self.append and self.log_path.exists()
            else None
        )
        if previous_elapsed_s is None:
            self.log_file = open(self.log_path, "w")
            header = "\t".join(f.name for f in fields(Sample)) + "\n"
            self.log_file.write(header)
        else:
            self.elapsed_offset = previous_elapsed_s
            self.append_offset = self.log_path.stat().st_size
            self.log_file = open(self.log_path, "a")
        self.log_file.flush()
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop sampling and close the log, reporting where it was saved.
        When appending, a run that raised is rolled back: it completed no
        index build, so its samples belong to no run and would stretch
        `elapsed_s` beyond what the index log accounts for.
        """
        self.stop_event.set()
        self.thread.join()
        self.log_file.close()
        if exc_type is not None and self.append_offset is not None:
            with open(self.log_path, "r+") as log_file:
                log_file.truncate(self.append_offset)
            log.warning(
                "Discarded the resource-usage samples of the failed run "
                f"from `{self.log_path.name}`"
            )
            return False
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
