"""Small shared helpers for the monitor-queries TUI widgets."""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
from datetime import datetime
from functools import lru_cache


def format_timestamp(ms: int) -> str:
    """Render an epoch (ms) as a full local date and time."""
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def format_clock(ms: int) -> str:
    """Render an epoch (ms) as a local wall-clock time."""
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S")


def format_seconds(seconds: float) -> str:
    """Format seconds to 3 shown digits, no unit suffix.

    The leading zero counts as a digit, so it is dropped below 0.1
    (.032) to keep exactly three digits on screen.
    """
    if seconds >= 10.0:
        text = f"{seconds:.1f}"
    elif seconds >= 0.1:
        text = f"{seconds:.2f}"
    elif seconds == 0:
        text = "0"
    else:
        text = f"{seconds:.3f}"[1:]
    return text


def format_duration(ms: int) -> str:
    """Render a duration in seconds to 3 significant figures, or '?'."""
    if ms < 0:
        return "?"
    return f"{format_seconds(ms / 1000)}s"


def truncate(text: str, max_len: int) -> str:
    """Trim text to max_len with an ellipsis."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def oneline(text: str) -> str:
    """Collapse all runs of whitespace (incl. newlines) to single spaces."""
    return " ".join(text.split())


@lru_cache(maxsize=1024)
def resolve_client_name(client_ip: str) -> str:
    """Reverse-DNS a client IP to its hostname, or return the IP.

    Blocking network call; run it off the UI thread. Falls back to the
    IP when there is no PTR record or the lookup fails. Cached per IP so
    each address is resolved at most once per process.
    """
    if not client_ip:
        return ""
    try:
        return socket.gethostbyaddr(client_ip)[0]
    except OSError:
        return client_ip


def clipboard_commands() -> list[list[str]]:
    """Pick clipboard CLIs available on this platform, in priority order."""
    system = platform.system()
    if system == "Darwin":
        return [["pbcopy"]]
    if system != "Linux":
        return []

    # On Wayland, never fall through to xclip/xsel: they write to the
    # XWayland selection, which Wayland-native apps don't read.
    on_wayland = bool(os.environ.get("WAYLAND_DISPLAY"))
    if on_wayland and shutil.which("wl-copy"):
        # Force text/plain so wl-copy doesn't tag SPARQL starting with
        # `PREFIX foo: <http://...>` as a URI-ish MIME type.
        return [["wl-copy", "--type", "text/plain"]]

    cmds = []
    if shutil.which("xclip"):
        cmds.append(["xclip", "-selection", "clipboard", "-t", "UTF8_STRING"])
    if shutil.which("xsel"):
        cmds.append(["xsel", "--clipboard", "--input"])
    return cmds


def clipboard_install_hint() -> str:
    """Platform-specific suggestion for installing a clipboard tool."""
    system = platform.system()
    if system == "Darwin":
        return "pbcopy should already be available on macOS"
    if system != "Linux":
        return f"no clipboard tool support for {system}"
    if os.environ.get("WAYLAND_DISPLAY"):
        return "install wl-clipboard (provides wl-copy)"
    return "install xclip or xsel"


def copy_text(text: str) -> bool | None:
    """Copy text to the system clipboard.

    Returns True on success, False if a tool ran but failed, None if no
    clipboard tool is available on this system.
    """
    cmds = clipboard_commands()
    if not cmds:
        return None
    payload = text.encode("utf-8")
    for cmd in cmds:
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc.communicate(input=payload, timeout=2)
        except (subprocess.TimeoutExpired, OSError):
            continue
        if proc.returncode == 0:
            return True
    return False
