from __future__ import annotations

import csv
import json
import warnings
from datetime import datetime
from pathlib import Path

import matplotlib
import numpy as np
import psutil

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402

from qlever import engine_name
from qlever.log import log
from qlever.util import (
    iter_permutation_phases,
    parse_git_hash,
    parse_phase_markers,
)

GB = 1024**3


def read_usage_tsv(path: Path) -> dict[str, np.ndarray]:
    """
    Read a resource-usage log into a dict of numpy arrays keyed by
    column name. Empty cells become NaN so matplotlib can skip them.
    """
    columns = {}
    with open(path) as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter="\t")
        if reader.fieldnames is None:
            return {}
        for column_name in reader.fieldnames:
            columns[column_name] = []
        for row in reader:
            for column_name in reader.fieldnames:
                cell = row[column_name]
                columns[column_name].append(float(cell) if cell else np.nan)
    return {name: np.array(values) for name, values in columns.items()}


def downsample_for_plot(
    data: dict[str, np.ndarray], max_points: int
) -> dict[str, np.ndarray]:
    """
    Bucket consecutive samples and reduce each bucket to one point so
    the plot stays readable on long builds. Returns `data` unchanged
    if it already has at most `max_points` rows.
    """
    n_samples = len(data["elapsed_s"])
    if n_samples <= max_points:
        return data

    bucket_size = -(-n_samples // max_points)
    n_buckets = -(-n_samples // bucket_size)
    pad_length = n_buckets * bucket_size - n_samples

    downsampled = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        for column_name, series in data.items():
            # reshape needs exact divisibility; pad the tail with NaN
            # so the nan-aware reducers ignore the padding cells.
            if pad_length:
                series = np.concatenate([series, np.full(pad_length, np.nan)])
            buckets = series.reshape(n_buckets, bucket_size)
            # elapsed_s uses nanmin so each plotted x marks the start of its
            # bucket's time window and the curve begins at the build start
            # rss/cpu use nanmax so peaks survive the bucketing.
            reducer = np.nanmin if column_name == "elapsed_s" else np.nanmax
            downsampled[column_name] = reducer(buckets, axis=1)
    return downsampled


def pick_time_unit(max_elapsed_s: float) -> tuple[str, float]:
    """Pick an axis label and divisor for the X axis based on build duration."""
    if max_elapsed_s < 200:
        return "Elapsed (s)", 1.0
    if max_elapsed_s < 3600:
        return "Elapsed (min)", 60.0
    return "Elapsed (h)", 3600.0


def annotate_peak(
    axes: plt.Axes,
    x_values: np.ndarray,
    y_values_gb: np.ndarray,
    label: str,
    color: str,
    offset: tuple[int, int],
) -> None:
    """Draw an arrow pointing at the peak of a GB-valued series."""
    if np.all(np.isnan(y_values_gb)):
        return
    peak_idx = int(np.nanargmax(y_values_gb))
    axes.annotate(
        f"peak {label}: {y_values_gb[peak_idx]:.2f} GB",
        xy=(x_values[peak_idx], y_values_gb[peak_idx]),
        xytext=offset,
        textcoords="offset points",
        arrowprops=dict(arrowstyle="->", color=color),
        fontsize=9,
        color=color,
    )


def compute_phase_boundaries(
    log_path: Path,
) -> dict[str, tuple[float, float]]:
    """
    Parse the index build log for phase timestamps. Maps each phase
    name to (start_s, end_s) elapsed seconds relative to the first
    "INFO: Processing" line. Returns {} if the log is missing or the
    first phase cannot be located. Phases with incomplete timestamps
    are skipped.
    """
    try:
        lines = log_path.read_text().splitlines()
    except OSError:
        return {}

    markers = parse_phase_markers(lines)
    overall_begin = markers.overall_begin
    if overall_begin is None:
        return {}

    merge_begin = markers.merge_begin
    convert_begin = markers.convert_begin
    perms = markers.permutations
    normal_end = markers.normal_end
    text_begin = markers.text_begin
    text_end = markers.text_end

    def rel(ts: datetime | None) -> float | None:
        return (ts - overall_begin).total_seconds() if ts else None

    phases = {}

    def add(name: str, start: datetime | None, end: datetime | None) -> None:
        start_s, end_s = rel(start), rel(end)
        if start_s is not None and end_s is not None:
            phases[name] = (start_s, end_s)

    add("Parse input", overall_begin, merge_begin)
    add("Build vocabularies", merge_begin, convert_begin)
    if perms:
        add("Convert to global IDs", convert_begin, perms[0][0])
        for name, perm_begin, perm_end in iter_permutation_phases(
            perms, normal_end
        ):
            add(f"Permutation {name}", perm_begin, perm_end)
    else:
        add("Convert to global IDs", convert_begin, normal_end)
    add("Text index", text_begin, text_end)

    return phases


def build_plot_subtitle(
    log_path: Path, stxxl_memory: str, settings_json: str
) -> str | None:
    """Assemble a 'batch | git | STXXL' line from the index log and resolved args."""
    git_hash = parse_git_hash(log_path)
    parts = []
    try:
        triples_per_batch = json.loads(settings_json).get(
            "num-triples-per-batch"
        )
    except (json.JSONDecodeError, AttributeError):
        triples_per_batch = None
    if triples_per_batch is not None:
        if triples_per_batch >= 1_000_000:
            batch_str = f"{triples_per_batch / 1_000_000:g}M"
        elif triples_per_batch >= 1_000:
            batch_str = f"{triples_per_batch / 1_000:g}K"
        else:
            batch_str = str(triples_per_batch)
        parts.append(f"batch = {batch_str} triples")
    if git_hash:
        parts.append(f"git = {git_hash}")
    if stxxl_memory:
        parts.append(f"STXXL = {stxxl_memory}")
    return "   |   ".join(parts) if parts else None


class UsagePlot:
    """
    Render a resource-usage plot from an index build's TSV log.
    Subclass and override `overlay` and/or `subtitle` for a specific
    engine; the base versions describe a QLever index build.
    """

    def __init__(
        self,
        dataset: str,
        args,
        *,
        output_dir: Path | None = None,
        plot_max_points: int = 500,
    ):
        self.dataset = dataset
        self.args = args
        self.output_dir = output_dir or Path.cwd()
        self.plot_max_points = plot_max_points
        self.log_path = self.output_dir / f"{dataset}.index-log.txt"

    def overlay(self) -> list[tuple[str, float, float]]:
        """Background regions as (label, start_s, end_s); empty if none."""
        phases = compute_phase_boundaries(self.log_path)
        return [(name, start, end) for name, (start, end) in phases.items()]

    def subtitle(self) -> str | None:
        """Subtitle line drawn under the title, or None."""
        return build_plot_subtitle(
            self.log_path,
            self.args.stxxl_memory or "",
            self.args.settings_json,
        )

    def build_figure(self, tsv_path: Path, plot_path: Path) -> bool:
        """
        Read the usage TSV and save a dual-axis memory/CPU figure to
        `plot_path`, shading `self.overlay()` regions and adding
        `self.subtitle()` under the title. Returns True if a plot was
        saved, False if the TSV has no usable samples.
        """
        data = read_usage_tsv(tsv_path)
        if not data or len(data.get("elapsed_s", [])) == 0:
            return False

        # drop leading rows where rss was never sampled so the plot starts
        # at the first real measurement rather than a flat NaN run.
        valid = np.where(~np.isnan(data["rss"]))[0]
        if len(valid) == 0:
            return False
        data = {name: values[valid[0] :] for name, values in data.items()}
        data["elapsed_s"] = data["elapsed_s"] - data["elapsed_s"][0]

        overlay = self.overlay()

        data = downsample_for_plot(data, self.plot_max_points)
        elapsed_s = data["elapsed_s"]

        x_label, x_factor = pick_time_unit(float(elapsed_s[-1]))
        x_values = elapsed_s / x_factor
        rss_gb = data["rss"] / GB
        cores = psutil.cpu_count() or 1
        # cpu_percent is per-core (100% == one fully used core), so dividing
        # by 100 converts it to a count of cores for the CPU axis.
        cpu_cores = data["cpu_percent"] / 100.0

        fig, ax_mem = plt.subplots(figsize=(12, 6), constrained_layout=True)
        ax_cpu = ax_mem.twinx()

        band_colors = plt.colormaps["Pastel1"].colors
        total_s = float(elapsed_s[-1]) if len(elapsed_s) else 0.0
        # skip drawing the region name when the band is too narrow to fit
        # it legibly; arbitrary 2% of total duration.
        min_label_s = total_s * 0.02
        for band_idx, (name, start_s, end_s) in enumerate(overlay):
            band_s = end_s - start_s
            if band_s <= 0:
                continue
            ax_mem.axvspan(
                start_s / x_factor,
                end_s / x_factor,
                color=band_colors[band_idx % len(band_colors)],
                alpha=0.4,
                zorder=0,
            )
            if band_s < min_label_s:
                continue
            mid = (start_s + end_s) / 2 / x_factor
            ax_mem.text(
                mid,
                0.98,
                name,
                transform=ax_mem.get_xaxis_transform(),
                ha="center",
                va="top",
                rotation=90,
                fontsize=8,
                alpha=0.7,
            )

        ax_mem.plot(
            x_values, rss_gb, color="#cc0000", label="RSS", linewidth=1.5
        )
        ax_mem.set_xlabel(x_label)
        ax_mem.set_ylabel("RSS Memory (GB)")
        ax_mem.grid(True, linestyle="--", alpha=0.3)

        if not np.all(np.isnan(cpu_cores)):
            ax_cpu.plot(
                x_values,
                cpu_cores,
                color="#1f77b4",
                label="CPU",
                linewidth=1.2,
                alpha=0.7,
            )
        ax_cpu.set_ylabel(f"CPU (cores, {cores} available)")
        ax_cpu.set_ylim(0, cores)

        max_rss = float(np.nanmax(rss_gb))
        x_max = float(x_values[-1])
        # With a single sample both spans are zero, which makes the axis
        # limits identical; fall back to a unit span so the limits differ.
        y_span = max_rss if max_rss > 0 else 1.0
        x_span = x_max if x_max > 0 else 1.0
        ax_mem.set_ylim(-y_span * 0.04, y_span * 1.4)
        ax_mem.set_xlim(-x_span * 0.02, x_span * 1.06)

        annotate_peak(ax_mem, x_values, rss_gb, "RSS", "#cc0000", (-25, 20))

        lines_mem, labels_mem = ax_mem.get_legend_handles_labels()
        lines_cpu, labels_cpu = ax_cpu.get_legend_handles_labels()
        ax_mem.legend(
            lines_mem + lines_cpu,
            labels_mem + labels_cpu,
            loc="center left",
            bbox_to_anchor=(1.08, 0.5),
        )

        title = f"{engine_name} index build: {self.dataset}"
        subtitle = self.subtitle()
        ax_mem.set_title(f"{title}\n{subtitle}" if subtitle else title)
        fig.savefig(plot_path, dpi=120)
        plt.close(fig)
        return True

    def render(self) -> Path | None:
        """
        Resolve the TSV and output paths, build the figure, and return
        the saved plot path. Returns None if the log is missing or the
        plot could not be rendered.
        """
        tsv_path = (
            self.output_dir / f"{self.dataset}.index.resource-usage-log.tsv"
        )
        # Backwards compatibility with older resource-usage log filename
        if not tsv_path.exists():
            tsv_path = (
                self.output_dir / f"{self.dataset}.resource-usage-log.tsv"
            )
        plot_path = self.output_dir / f"{self.dataset}.resource-usage-plot.png"
        if not tsv_path.exists():
            log.warning(f"Resource-usage log not found: `{tsv_path.name}`")
            return None
        try:
            rendered = self.build_figure(tsv_path, plot_path)
        except Exception as error:
            log.warning(f"Could not render resource-usage plot: {error}")
            return None
        if not rendered:
            log.warning(
                "Resource-usage plot not rendered: no usable samples in "
                f"the resource-usage log `{tsv_path.name}`"
            )
            return None
        return plot_path
