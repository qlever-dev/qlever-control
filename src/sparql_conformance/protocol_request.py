from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ProtocolHeader:
    name: str
    value: str

    def render(self) -> str:
        return f"{self.name}: {self.value}"


@dataclass
class ProtocolResponse:
    status_codes: List[str]
    expected_boolean: Optional[bool] = None
    expected_format: Optional[str] = None
    expectation: Optional[str] = None
    headers: List[ProtocolHeader] = field(default_factory=list)
    body: Optional[str] = None
    character_encoding: str = "UTF-8"
    expected_location: Optional[str] = None

    def render(self) -> str:
        """Render the expected response as a readable ``#### Response`` block."""
        lines = ["#### Response"]
        if self.status_codes:
            lines.append("Status: " + " or ".join(self.status_codes))
        lines.extend(header.render() for header in self.headers)
        if self.expected_boolean is not None:
            lines.append(
                "Expected boolean: "
                + ("true" if self.expected_boolean else "false"))
        if self.expected_format:
            lines.append(f"Expected format: {self.expected_format}")
        if self.expectation:
            lines.append(f"Expectation: {self.expectation}")
        if self.expected_location:
            lines.append(f"Location: {self.expected_location}")
        if self.body:
            lines.append("")
            lines.append(self.body)
        return "\n".join(lines)


@dataclass
class ProtocolRequest:
    method: str
    absolute_path: str
    connection_authority: str
    expected_response: ProtocolResponse
    http_version: str = "1.1"
    headers: List[ProtocolHeader] = field(default_factory=list)
    body: Optional[str] = None
    character_encoding: str = "UTF-8"

    def render(self) -> str:
        """Render this request and its expected response as an HTTP-like block."""
        lines = ["#### Request",
                 f"{self.method} {self.absolute_path} HTTP/{self.http_version}"]
        if self.connection_authority:
            lines.append(f"Host: {self.connection_authority}")
        lines.extend(header.render() for header in self.headers)
        if self.body:
            lines.append("")
            lines.append(self.body)
        return "\n".join(lines) + "\n\n" + self.expected_response.render()


def render_protocol_requests(requests: Optional[List[ProtocolRequest]]) -> str:
    """Render a list of structured protocol requests as a readable string.

    Multiple requests within one test are separated by a ``#### Request N``
    header (N >= 2), echoing the old "followed by" multi-request convention.
    Returns an empty string for an empty or missing list.
    """
    if not requests:
        return ""
    blocks = []
    for index, request in enumerate(requests):
        block = request.render()
        if index >= 1:
            block = f"#### Request {index + 1}\n" + block[len("#### Request\n"):]
        blocks.append(block)
    return "\n\n".join(blocks)
