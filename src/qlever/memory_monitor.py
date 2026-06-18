from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil

from qlever import engine_name
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import format_size, run_command


def parse_container_mem_usage(usage: str) -> int:
    """
    Parse a memory usage string from ``docker stats`` or ``podman stats``
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

        with MemoryMonitor(dataset="wikidata", cmdline_regex="qlever-index"):
            run_command(cmd, show_output=True)

        # For container mode:
        with MemoryMonitor(dataset="wikidata",
                           cmdline_regex="qlever-index",
                           container="qlever.index.wikidata",
                           system="docker"):
            run_command(cmd, show_output=True)
    """

    def __init__(
        self,
        dataset: str,
        cmdline_regex: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
        output_dir: Path = Path.cwd(),
    ):
        """
        Args:
            dataset:        Name of the dataset being indexed.
            cmdline_regex:  Regex matched against child process command
                            lines to identify the index process (native
                            mode only).
            container:      Container name to query for memory stats.
                            When set together with ``system``, sampling
                            uses ``docker/podman stats`` instead of
                            psutil.
            system:         Container runtime ("docker" or "podman").
            interval:       Seconds between samples (default 1.0).
            output_dir:     Directory for the JSON memory log file.
        """
        self.engine = engine_name
        self.dataset = dataset
        self.cmdline_regex = cmdline_regex
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = Path(output_dir)
        self.peak_rss = 0
        self.samples = []
        self.stop_event = threading.Event()
        self.thread = None
        self.start_time = 0

    def sample_native(self) -> int:
        """
        Find the index process among our children by matching its
        command line, then sum RSS of that process and all its
        descendants.
        """
        me = psutil.Process()
        for child in me.children(recursive=True):
            try:
                cmdline = " ".join(child.cmdline())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if re.search(self.cmdline_regex, cmdline):
                rss = child.memory_info().rss
                for grandchild in child.children(recursive=True):
                    try:
                        rss += grandchild.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                return rss
        return 0

    def sample_container(self) -> int:
        """
        Query the container runtime for the memory usage of the
        index container.
        """
        try:
            output = run_command(
                f"{self.system} stats --no-stream"
                f" --format '{{{{.MemUsage}}}}' {self.container}",
                return_output=True,
            )
            usage = output.strip().split("/")[0].strip()
            return parse_container_mem_usage(usage)
        except Exception:
            return 0

    def run_loop(self):
        """
        Polling loop that runs on a background thread. Selects the
        appropriate sampling method (native or container) and collects
        (elapsed_seconds, rss_bytes) tuples until the stop event is set.
        """
        sample = (
            self.sample_container
            if self.system in Containerize.supported_systems()
            else self.sample_native
        )
        while not self.stop_event.is_set():
            rss = sample()
            self.peak_rss = max(self.peak_rss, rss)
            elapsed = time.monotonic() - self.start_time
            self.samples.append((elapsed, rss))
            self.stop_event.wait(self.interval)

    def save(self):
        """
        Write all collected samples and metadata to a JSON file at
        ``<output_dir>/<engine>.<dataset>.memory-log.json``.
        """
        path = (
            self.output_dir
            / f"{self.engine.lower()}.{self.dataset.lower()}.memory-log.json"
        )
        data = {
            "engine": self.engine,
            "dataset": self.dataset,
            "start_time": datetime.fromtimestamp(
                time.time() - (time.monotonic() - self.start_time)
            ).isoformat(timespec="seconds"),
            "peak_rss_bytes": self.peak_rss,
            "peak_rss_human": format_size(self.peak_rss),
            "elapsed_s": (
                round(self.samples[-1][0], 1) if self.samples else 0
            ),
            "samples": [
                {"elapsed_s": round(t, 1), "rss_bytes": r}
                for t, r in self.samples
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
