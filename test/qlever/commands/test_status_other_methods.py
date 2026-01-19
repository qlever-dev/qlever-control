import argparse
import unittest

from qlever.commands.status import StatusCommand


class TestStatusCommand(unittest.TestCase):
    def test_description(self):
        result = StatusCommand().description()
        self.assertEqual(
            result, "Show QLever processes running on this machine"
        )

    def test_should_have_qleverfile(self):
        self.assertFalse(StatusCommand().should_have_qleverfile())

    def test_relevant_qleverfile_arguments(self):
        result = StatusCommand().relevant_qleverfile_arguments()
        self.assertEqual(result, {})

    def test_additional_arguments(self):
        # Create an instance of StatusCommand
        sc = StatusCommand()

        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")
        # Call the method
        sc.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value is set correctly
        self.assertEqual(args.cmdline_regex, "^(qlever-server|qlever-index)")

        # Test that the help text is correctly set
        argument_help = subparser._group_actions[-1].help
        self.assertEqual(
            argument_help,
            "Show only processes where the command line matches this regex",
        )
