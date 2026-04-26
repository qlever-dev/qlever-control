from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ProtocolHeader:
    name: str
    value: str


@dataclass
class ProtocolResponse:
    status_codes: List[str]
    expected_boolean: Optional[bool] = None
    expected_format: Optional[str] = None
    expectation: Optional[str] = None


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
