import argparse
import unittest

from qlever.commands.cache_stats import CacheStatsCommand


class TestStartCommand(unittest.TestCase):
    def test_description(self):
        self.assertEqual(
            "Show how much of the cache is currently being " "used",
            CacheStatsCommand().description(),
        )

    def test_should_have_qleverfile(self):
        assert not CacheStatsCommand().should_have_qleverfile()

    def test_relevant_qleverfile_arguments(self):
        testdict = {"server": ["host_name", "port"]}
        self.assertEqual(
            testdict, CacheStatsCommand().relevant_qleverfile_arguments()
        )

    def test_additional_arguments(self):
        # Create an instance of CacheStatsCommand
        csc = CacheStatsCommand()

        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")
        # Call the method
        csc.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value for server-url is set correctly
        """Why is there no default="localhost:{port}"? """
        self.assertEqual(args.server_url, None)

        # Test that the help text for server-url is correctly set
        argument_help = subparser._group_actions[-2].help
        self.assertEqual(
            "URL of the QLever server, default is {host_name}:{port}",
            argument_help,
        )

        # Test that the default value for --detailed is set correctly
        self.assertEqual(False, args.detailed)

        # Test that the help text for --detailed is correctly set
        argument_help = subparser._group_actions[-1].help
        self.assertEqual(
            "Show detailed statistics and settings", argument_help
        )
