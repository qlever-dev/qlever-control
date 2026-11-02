import pytest

from qeval.base_commands.index_stats import BaseIndexStatsCommand


class FakeIndexStatsCommand(BaseIndexStatsCommand):
    def index_size_patterns(self, args):
        return [f"{args.name}.fake-index.*"]

    def parse_index_durations(self, log_text):
        phases = (line.rsplit(maxsplit=1) for line in log_text.splitlines())
        return {heading: float(seconds) for heading, seconds in phases}


@pytest.fixture
def fake_index_stats():
    """
    The `index-stats` command of a fake engine. Its index log has one line
    per phase, like `TOTAL time 90`, and its index files are the files
    `<name>.fake-index.*`.
    """
    return FakeIndexStatsCommand()
