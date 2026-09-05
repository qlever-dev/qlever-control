import argparse
import unittest

from qlever.commands.log import LogCommand


class TestStartCommand(unittest.TestCase):
    def test_description(self):
        self.assertEqual(
            "Show the last lines of the server log file and follow it",
            LogCommand().description(),
        )

    def test_should_have_qleverfile(self):
        assert not LogCommand().should_have_qleverfile()

    def test_relevant_qleverfile_arguments(self):
        testdict = {"data": ["name"]}
        self.assertEqual(
            testdict, LogCommand().relevant_qleverfile_arguments()
        )

    def test_additional_arguments(self):
        # Create an instance of StopCommand
        lc = LogCommand()

        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")
        # Call the method
        lc.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value for tail-num-lines is set correctly
        self.assertEqual(args.tail_num_lines, 20)

        # Test that the help text for
        # --tail-num-lines is correctly set
        argument_help = subparser._group_actions[-3].help
        self.assertEqual(
            "Show this many of the last lines of the log file",
            argument_help,
        )

        # Test that the default value for --from-beginning is set correctly
        self.assertEqual(False, args.from_beginning)

        # Test that the help text for --from-beginning is correctly set
        argument_help = subparser._group_actions[-2].help
        self.assertEqual("Show all lines of the log file", argument_help)

        # Test that the default value for -no-follow is set correctly
        self.assertEqual(False, args.no_follow)

        # Test that the help text for --no-follow is correctly set
        argument_help = subparser._group_actions[-1].help
        self.assertEqual(argument_help, "Don't follow the log file")
