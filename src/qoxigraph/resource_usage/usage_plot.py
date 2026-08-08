from __future__ import annotations

import re

from qlever.containerize import Containerize
from qlever.resource_usage.usage_plot import UsagePlot as BaseUsagePlot
from qlever.util import run_command


def parse_logged_seconds(text: str, label: str) -> float | None:
    """Return the integer seconds logged after `label`, or None."""
    match = re.search(rf"{re.escape(label)}\s*(\d+)s", text)
    return float(match.group(1)) if match else None


class UsagePlot(BaseUsagePlot):
    """Resource-usage plot for an Oxigraph index build."""

    def overlay(self) -> list[tuple[str, float, float]]:
        """Shade the load and optimize phases; empty if no optimize step."""
        try:
            text = self.log_path.read_text()
        except OSError:
            return []
        load_s = parse_logged_seconds(text, "Load time:")
        optimize_s = parse_logged_seconds(text, "Optimize time:")
        if load_s is None or optimize_s is None:
            return []
        return [
            ("Load", 0.0, load_s),
            ("Optimize", load_s, load_s + optimize_s),
        ]

    def subtitle(self) -> str | None:
        """Assemble a 'version | read-only' line from the index args."""
        if self.args.system in Containerize.supported_systems():
            version_cmd = (
                f"{self.args.system} run --rm {self.args.image} --version"
            )
        else:
            version_cmd = f"{self.args.index_binary} --version"
        try:
            version = run_command(version_cmd, return_output=True).strip()
        except Exception:
            version = ""
        parts = []
        if version:
            parts.append(version)
        parts.append(f"read-only = {self.args.read_only}")
        return "   |   ".join(parts)
