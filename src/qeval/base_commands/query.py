from __future__ import annotations

from qlever.commands.query import QueryCommand as QleverQueryCommand


class BaseQueryCommand(QleverQueryCommand):
    """
    Send a query to the SPARQL endpoint of an engine. QLever's option to pin
    the result to its cache, and the access token that this needs, are left
    out.
    """

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port"]}

    def accept_header_choices(self) -> list[str]:
        return [
            "text/tab-separated-values",
            "text/csv",
            "application/sparql-results+json",
            "application/sparql-results+xml",
        ]

    def can_pin_to_cache(self) -> bool:
        return False
