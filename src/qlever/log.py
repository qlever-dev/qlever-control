from __future__ import annotations

import logging
from contextlib import contextmanager

from termcolor import colored


class QleverLogFormatter(logging.Formatter):
    """
    Custom formatter for logging.
    """

    def format(self, record):
        message = record.getMessage()
        if record.levelno == logging.DEBUG:
            return colored(f"{message}", "magenta")
        elif record.levelno == logging.WARNING:
            return colored(f"{message}", "yellow")
        elif record.levelno in [logging.CRITICAL, logging.ERROR]:
            return colored(f"{message}", "red")
        else:
            return message


# Custom logger.
log = logging.getLogger("qlever")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(QleverLogFormatter())
log.addHandler(handler)
log_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "NO_LOG": logging.CRITICAL + 1,
}


@contextmanager
def mute_log(level=logging.ERROR):
    """
    Temporarily mute the log, simply works as follows:

    with mute_log():
       ...
    """
    original_level = log.getEffectiveLevel()
    log.setLevel(level)
    try:
        yield
    finally:
        log.setLevel(original_level)
