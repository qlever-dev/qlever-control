from __future__ import annotations

import ctypes
import ctypes.util
import json
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import psutil

from qlever import engine_name
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import format_size, run_command

IS_LINUX = sys.platform.startswith("linux")
IS_MACOS = sys.platform == "darwin"

libc = ctypes.CDLL(ctypes.util.find_library("c")) if IS_MACOS else None


def phys_footprint(pid: int) -> int:
    """
    Return `ri_phys_footprint` from `proc_pid_rusage` on macOS, the
    closest available analog to Linux PSS. Returns 0 on any other
    platform or on error.
    """
    if IS_MACOS:
        buf = (ctypes.c_uint64 * 24)()
        if libc.proc_pid_rusage(pid, 2, buf) == 0:
            return buf[9]
    return 0


@dataclass
class MemorySample:
    """
    One memory measurement. `rss` is always populated (native or
    container). `uss` is populated in native mode on both Linux and
    macOS. `pss` and `swap` are Linux-only; `phys_footprint` is
    macOS-only. Unavailable fields stay `None` and are dropped from
    the JSON output.
    """

    elapsed_s: float = 0.0
    rss: int = 0
    pss: int | None = None
    uss: int | None = None
    swap: int | None = None
    phys_footprint: int | None = None


def parse_container_mem_usage(usage: str) -> int:
    """
    Parse a memory usage string from `docker stats` or `podman stats`
    into bytes.  Docker reports binary units (GiB, MiB) while Podman
    reports decimal units (GB, MB).
    """
    usage = usage.strip()
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
        if usage.upper().endswith(suffix):
            number = float(usage[: len(usage) - len(suffix)])
            return int(number * multiplier)
    return 0


class MemoryMonitor:
    """
    Monitor memory usage of an index-building process. Works in both
    native mode (via psutil) and container mode (via docker/podman stats).

    Usage as a context manager:

        with MemoryMonitor(dataset="wikidata", binary="qlever-index"):
            run_command(cmd, show_output=True)

        # For container mode:
        with MemoryMonitor(dataset="wikidata",
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
        output_dir: Path = Path.cwd(),
        parent_pid: int | None = None,
    ):
        """
        Args:
            dataset:        Name of the dataset being indexed.
            binary:         Name of the index executable. Its basename
                            is matched against `Path(argv[0]).name` of
                            each process in the descendant tree to find
                            the worker (native mode only).
            container:      Container name to query for memory stats.
                            When set together with `system`, sampling
                            uses `docker/podman stats` instead of
                            psutil.
            system:         Container runtime ("docker" or "podman").
            interval:       Seconds between samples (default 1.0).
            output_dir:     Directory for the JSON memory log file.
            parent_pid:     PID whose descendants are searched for the
                            index process. Defaults to the current
                            Python process. Useful when the target is a
                            daemonized process that re-parents away
                            from us (e.g. virtuoso-t).
        """
        self.engine = engine_name
        self.dataset = dataset
        self.binary = binary
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = Path(output_dir)
        self.parent_pid = parent_pid
        self.peak_rss = 0
        self.samples = []
        self.stop_event = threading.Event()
        self.thread = None
        self.start_time = 0

    def sample_native(self) -> MemorySample:
        """
        Read memory usage of the index worker found among
        `self.parent_pid` and its descendants. The worker is the
        process whose `Path(argv[0]).name` equals `self.binary`.
        """
        try:
            root = psutil.Process(self.parent_pid)
            candidates = [root] + root.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return MemorySample()
        target = Path(self.binary).name
        for proc in candidates:
            try:
                cmdline = proc.cmdline()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if cmdline and Path(cmdline[0]).name == target:
                try:
                    info = proc.memory_full_info()
                    sample = MemorySample(rss=info.rss, uss=info.uss)
                    if IS_LINUX:
                        sample.pss = info.pss
                        sample.swap = info.swap
                    elif IS_MACOS:
                        sample.phys_footprint = phys_footprint(proc.pid)
                    return sample
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    return MemorySample()
        return MemorySample()

    def sample_container(self) -> MemorySample:
        """
        Query the container runtime for the memory usage of the
        index container. Only `rss` is populated; cgroup stats do
        not expose a PSS/USS equivalent per process.
        """
        try:
            output = run_command(
                f"{self.system} stats --no-stream"
                f" --format '{{{{.MemUsage}}}}' {self.container}",
                return_output=True,
            )
            usage = output.strip().split("/")[0].strip()
            return MemorySample(rss=parse_container_mem_usage(usage))
        except Exception:
            return MemorySample()

    def run_loop(self):
        """
        Polling loop that runs on a background thread. Selects the
        appropriate sampling method (native or container) and collects
        `MemorySample` entries until the stop event is set.
        """
        sample = (
            self.sample_container
            if self.system in Containerize.supported_systems()
            else self.sample_native
        )
        while not self.stop_event.is_set():
            ms = sample()
            ms.elapsed_s = round(time.monotonic() - self.start_time, 1)
            self.peak_rss = max(self.peak_rss, ms.rss)
            self.samples.append(ms)
            self.stop_event.wait(self.interval)

    def save(self):
        """
        Write all collected samples and metadata to a JSON file at
        `<output_dir>/<dataset>.memory-log.json`. Fields that are
        unavailable on the current platform (e.g. pss on macOS,
        phys_footprint on Linux) are omitted from each sample.
        """
        path = self.output_dir / f"{self.dataset}.memory-log.json"
        data = {
            "engine": self.engine,
            "dataset": self.dataset,
            "start_time": datetime.fromtimestamp(
                time.time() - (time.monotonic() - self.start_time)
            ).isoformat(timespec="seconds"),
            "peak_rss_bytes": self.peak_rss,
            "peak_rss_human": format_size(self.peak_rss),
            "elapsed_s": (self.samples[-1].elapsed_s if self.samples else 0),
            "samples": [
                {k: v for k, v in asdict(s).items() if v is not None}
                for s in self.samples
            ],
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def __enter__(self):
        """Start the background sampling thread."""
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop sampling, persist results, and log peak memory usage."""
        self.stop_event.set()
        self.thread.join()
        self.save()
        log.info(f"Peak memory usage: {format_size(self.peak_rss)}")
        return False
