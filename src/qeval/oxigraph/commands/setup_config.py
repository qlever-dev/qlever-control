from __future__ import annotations

from qeval.base_commands.setup_config import BaseSetupConfigCommand


class SetupConfigCommand(BaseSetupConfigCommand):
    """Create a Qleverfile for Oxigraph."""

    def image(self) -> str:
        return "ghcr.io/oxigraph/oxigraph"

    def engine_settings(self, args) -> dict[str, dict[str, str]]:
        return {"server": {"READ_ONLY": "yes", "TIMEOUT": "60s"}}
